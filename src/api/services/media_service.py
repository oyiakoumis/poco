"""Media handling service for WhatsApp messages."""

import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import httpx
from azure.storage.blob import BlobSasPermissions, ContentSettings, generate_blob_sas
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from fastapi import Request

from api.models import MediaItem
from settings import settings
from utils.logging import logger


class BlobStorageService:
    """Service for interacting with Azure Blob Storage."""

    @asynccontextmanager
    async def blob_service_client(self) -> AsyncGenerator[BlobServiceClient, None]:
        """Get a blob service client using a context manager for proper cleanup.

        Yields:
            A blob service client
        """
        client = BlobServiceClient.from_connection_string(settings.azure_storage_connection_string)
        try:
            yield client
        finally:
            await client.close()

    @asynccontextmanager
    async def container_client(self) -> AsyncGenerator[ContainerClient, None]:
        """Get a container client using a context manager for proper cleanup.

        Yields:
            A container client for the configured blob container
        """
        async with self.blob_service_client() as blob_service_client:
            container_client = blob_service_client.get_container_client(settings.azure_blob_container_name)

            # Create container if it doesn't exist
            if not await container_client.exists():
                await container_client.create_container()

            try:
                yield container_client
            finally:
                await container_client.close()

    async def upload_blob(self, content: bytes, blob_name: str, content_type: str) -> str:
        """Upload content to Azure Blob Storage.

        Args:
            content: The content to upload
            blob_name: The name to give the blob
            content_type: The content type of the blob

        Returns:
            The name of the blob in Azure Blob Storage
        """
        async with self.container_client() as container_client:
            blob_client = container_client.get_blob_client(blob_name)
            content_settings = ContentSettings(content_type=content_type)
            await blob_client.upload_blob(content, content_settings=content_settings)
            return blob_name

    async def generate_presigned_url(self, blob_name: str, expiry_hours: int = 24) -> str:
        """Generate a presigned URL for a blob in Azure Blob Storage.

        Args:
            blob_name: The name of the blob
            expiry_hours: Number of hours until the URL expires

        Returns:
            A presigned URL for the blob
        """
        async with self.blob_service_client() as blob_service_client:
            # Extract account name from the blob service client
            account_name = blob_service_client.account_name
            container_name = settings.azure_blob_container_name
            account_key = settings.azure_storage_account_key

            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
            )

            # Create the presigned URL
            presigned_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            return presigned_url

    async def generate_multiple_blob_presigned_urls(self, blob_names: List[str], expiry_hours: int = 24) -> Dict[str, Optional[str]]:
        """Generate presigned URLs for multiple blobs concurrently.

        Args:
            blob_names: List of blob names to generate URLs for
            expiry_hours: Number of hours until the URLs expire

        Returns:
            Dictionary mapping blob names to their presigned URLs (or None if generation failed)
        """
        if not blob_names:
            return {}

        async with self.blob_service_client() as blob_service_client:
            # Extract common information
            account_name = blob_service_client.account_name
            container_name = settings.azure_blob_container_name
            account_key = settings.azure_storage_account_key

            # Create a helper function that doesn't create a new client each time
            async def get_url_with_error_handling(blob_name: str) -> Tuple[str, Optional[str]]:
                try:
                    # Generate SAS token
                    sas_token = generate_blob_sas(
                        account_name=account_name,
                        container_name=container_name,
                        blob_name=blob_name,
                        account_key=account_key,
                        permission=BlobSasPermissions(read=True),
                        expiry=datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
                    )

                    # Create the presigned URL
                    presigned_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
                    return blob_name, presigned_url
                except Exception as e:
                    logger.error(f"Error generating presigned URL for blob {blob_name}: {str(e)}")
                    return blob_name, None

            # Use asyncio.gather to run all URL generation tasks concurrently
            results = await asyncio.gather(*[get_url_with_error_handling(blob_name) for blob_name in blob_names])

            # Convert results to a dictionary
            return {blob_name: url for blob_name, url in results}


class MediaService:
    """Service for handling media in WhatsApp messages."""

    def __init__(self):
        """Initialize the MediaService with a BlobStorageService instance."""
        self.blob_storage = BlobStorageService()

    async def process_media(self, request: Request, num_media: int) -> List[MediaItem]:
        """Process media items from a WhatsApp message.

        Args:
            request: The FastAPI request object
            num_media: The number of media items in the message

        Returns:
            A list of MediaItem objects
        """
        if not num_media:
            return []

        media_items = []
        form_data = await request.form()

        for i in range(num_media):
            media_url = form_data.get(f"MediaUrl{i}")
            media_type = form_data.get(f"MediaContentType{i}")

            if not media_url or not media_type:
                continue

            # Only process images
            if not media_type.startswith("image/"):
                logger.info(f"Skipping non-image media: {media_type}")
                continue

            try:
                # Upload image to Azure Blob Storage
                blob_name = await self._upload_from_url(media_url, media_type)

                # Store blob name instead of URL
                media_items.append(MediaItem(blob_name=blob_name, content_type=media_type))
                logger.info(f"Image uploaded to Azure Blob Storage: {blob_name}")
            except Exception as e:
                logger.error(f"Error uploading image to Azure Blob Storage: {str(e)}")

        return media_items

    async def validate_media(self, request: Request, num_media: int) -> Tuple[bool, Optional[str]]:
        """Validate media items in a WhatsApp message.

        Checks:
        - Only supported image types (PNG, JPEG/JPG, non-animated GIF)
        - Maximum 10 images
        - Maximum 5MB per image

        Args:
            request: The FastAPI request object
            num_media: The number of media items in the message

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not num_media:
            return True, None

        # Check if there are too many images
        if num_media > 10:
            return False, f"Too many images. Maximum allowed is 10, but received {num_media}."

        try:
            form_data = await request.form()

            for i in range(num_media):
                media_type = form_data.get(f"MediaContentType{i}")

                # Check if it's an image
                if not media_type or not media_type.startswith("image/"):
                    return False, f"Unsupported media type: {media_type}. Only images are supported."

                # Check if it's a supported image type
                supported_types = ["image/png", "image/jpeg", "image/jpg", "image/gif"]
                if media_type not in supported_types:
                    return False, f"Unsupported image format: {media_type}. Supported formats are PNG, JPEG, and GIF."

                # Download the media to check its size
                media_url = form_data.get(f"MediaUrl{i}")
                if media_url:
                    async with httpx.AsyncClient() as client:
                        auth = (settings.twilio_account_sid, settings.twilio_auth_token)
                        # Just get the headers to check content length
                        media_response = await client.head(media_url, auth=auth, follow_redirects=True)

                        # Check file size (5MB = 5 * 1024 * 1024 bytes)
                        content_length = int(media_response.headers.get("content-length", 0))
                        max_size = 5 * 1024 * 1024  # 5MB in bytes

                        if content_length > max_size:
                            return False, f"Image size exceeds maximum allowed (5MB). Image {i+1} is {content_length / (1024 * 1024):.2f}MB."

            return True, None
        except Exception as e:
            logger.error(f"Error validating media: {str(e)}")
            return False, f"Error validating media: {str(e)}"

    async def _upload_from_url(self, media_url: str, content_type: str) -> str:
        """Upload media from a URL to Azure Blob Storage.

        Args:
            media_url: The URL of the media to upload
            content_type: The content type of the media

        Returns:
            The name of the blob in Azure Blob Storage
        """
        # Generate a unique blob name
        file_extension = content_type.split("/")[-1]
        if file_extension == "jpeg":
            file_extension = "jpg"  # Standardize jpeg extension
        blob_name = f"{uuid4()}.{file_extension}"

        # Download the media from Twilio using httpx
        async with httpx.AsyncClient() as client:
            auth = (settings.twilio_account_sid, settings.twilio_auth_token)
            media_response = await client.get(media_url, auth=auth, follow_redirects=True)
            if not media_response or not media_response.content:
                raise Exception(f"Failed to download media from Twilio")

        # Upload to Azure Blob Storage
        await self.blob_storage.upload_blob(content=media_response.content, blob_name=blob_name, content_type=content_type)

        return blob_name

    async def prepare_outgoing_attachment(self, content: str, filename: str, content_type: str) -> str:
        """Prepare an outgoing file attachment by uploading it to Azure Blob Storage.

        Args:
            content: Base64-encoded file content
            filename: The name of the file.
            content_type: The content type of the file

        Returns:
            The media URL for Twilio
        """
        try:
            # Decode base64 content
            content = base64.b64decode(content)

            # Upload to Azure Blob Storage
            await self.blob_storage.upload_blob(content=content, blob_name=filename, content_type=content_type)

            # Generate a presigned URL for the blob
            presigned_url = await self.blob_storage.generate_presigned_url(filename)

            logger.info(f"File '{filename}' prepared for outgoing attachment: {filename}")
            return presigned_url

        except Exception as e:
            logger.error(f"Error preparing outgoing attachment '{filename}': {str(e)}")
            raise
