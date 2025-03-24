"""Media handling service for WhatsApp messages."""

import asyncio
import base64
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import httpx
from azure.storage.blob import BlobSasPermissions, ContentSettings, generate_blob_sas
from azure.storage.blob.aio import BlobServiceClient
from fastapi import Request

from api.models import MediaItem
from settings import settings
from utils.azure_auth import get_azure_credential
from utils.logging import logger
from utils.singleton import Singleton


class BlobStorageService(metaclass=Singleton):
    """Service for interacting with Azure Blob Storage."""

    def __init__(self):
        """Initialize the BlobStorageService with Azure storage settings."""
        self.account_name = settings.azure_storage_account
        self.container_name = settings.azure_storage_container
        self.account_url = f"https://{self.account_name}.blob.core.windows.net"

        # Initialize these as None - they'll be created on first use
        self._credential = None
        self._blob_service_client = None
        self._container_client = None

    async def _get_credential(self):
        """Get or create the appropriate Azure credential based on the environment.

        Returns:
            The Azure credential instance
        """
        if self._credential is None:
            # Check if running locally
            self._credential = get_azure_credential(do_async=True)

        return self._credential

    async def _get_blob_service_client(self):
        """Get or create the BlobServiceClient.

        Returns:
            The BlobServiceClient instance
        """
        if self._blob_service_client is None:
            credential = await self._get_credential()
            self._blob_service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        return self._blob_service_client

    async def _get_container_client(self):
        """Get or create the ContainerClient.

        Returns:
            The ContainerClient instance
        """
        if self._container_client is None:
            blob_service_client = await self._get_blob_service_client()
            self._container_client = blob_service_client.get_container_client(self.container_name)

            # Create container if it doesn't exist
            if not await self._container_client.exists():
                await self._container_client.create_container()

        return self._container_client

    async def upload_blob(self, content: bytes, blob_name: str, content_type: str) -> str:
        """Upload content to Azure Blob Storage.

        Args:
            content: The content to upload
            blob_name: The name to give the blob
            content_type: The content type of the blob

        Returns:
            The name of the blob in Azure Blob Storage
        """
        container_client = await self._get_container_client()
        blob_client = container_client.get_blob_client(blob_name)
        content_settings = ContentSettings(content_type=content_type)
        await blob_client.upload_blob(content, content_settings=content_settings)
        return blob_name

    async def generate_presigned_url(self, blob_name: str, expiry_hours: int = 24) -> str:
        """Generate a presigned URL for a blob in Azure Blob Storage using user delegation key.

        Args:
            blob_name: The name of the blob
            expiry_hours: Number of hours until the URL expires

        Returns:
            A presigned URL for the blob
        """
        blob_service_client = await self._get_blob_service_client()

        # Get a user delegation key that will be valid for the specified duration
        start_time = datetime.now(timezone.utc)
        expiry_time = start_time + timedelta(hours=expiry_hours)

        # Get user delegation key - this is the secure way to generate SAS tokens with DefaultAzureCredential
        user_delegation_key = await blob_service_client.get_user_delegation_key(key_start_time=start_time, key_expiry_time=expiry_time)

        # Get blob client to generate the SAS
        blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)

        # Generate the SAS URL
        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            user_delegation_key=user_delegation_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry_time,
            start=start_time,
        )

        # Create the presigned URL
        presigned_url = f"{blob_client.url}?{sas_token}"
        return presigned_url

    async def generate_multiple_blob_presigned_urls(self, blob_names: List[str], expiry_hours: int = 24) -> Dict[str, Optional[str]]:
        """Generate presigned URLs for multiple blobs concurrently using user delegation key.

        Args:
            blob_names: List of blob names to generate URLs for
            expiry_hours: Number of hours until the URLs expire

        Returns:
            Dictionary mapping blob names to their presigned URLs (or None if generation failed)
        """
        if not blob_names:
            return {}

        blob_service_client = await self._get_blob_service_client()

        # Get a user delegation key once for all blobs
        start_time = datetime.now(timezone.utc)
        expiry_time = start_time + timedelta(hours=expiry_hours)

        try:
            # Get user delegation key - this is the secure way to generate SAS tokens with DefaultAzureCredential
            user_delegation_key = await blob_service_client.get_user_delegation_key(key_start_time=start_time, key_expiry_time=expiry_time)

            # Create a helper function that doesn't create a new client each time
            async def get_url_with_error_handling(blob_name: str) -> Tuple[str, Optional[str]]:
                try:
                    # Get blob client
                    blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)

                    # Generate SAS token
                    sas_token = generate_blob_sas(
                        account_name=self.account_name,
                        container_name=self.container_name,
                        blob_name=blob_name,
                        user_delegation_key=user_delegation_key,
                        permission=BlobSasPermissions(read=True),
                        expiry=expiry_time,
                        start=start_time,
                    )

                    # Create the presigned URL
                    presigned_url = f"{blob_client.url}?{sas_token}"
                    return blob_name, presigned_url
                except Exception as e:
                    logger.error(f"Error generating presigned URL for blob {blob_name}: {str(e)}")
                    return blob_name, None

            # Use asyncio.gather to run all URL generation tasks concurrently
            results = await asyncio.gather(*[get_url_with_error_handling(blob_name) for blob_name in blob_names])

            # Convert results to a dictionary
            return {blob_name: url for blob_name, url in results}
        except Exception as e:
            logger.error(f"Error getting user delegation key: {str(e)}")
            return {blob_name: None for blob_name in blob_names}

    async def close(self):
        """Close all clients and release resources."""
        if self._container_client:
            await self._container_client.close()
            self._container_client = None

        if self._blob_service_client:
            await self._blob_service_client.close()
            self._blob_service_client = None


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
        if num_media > 1:
            return False, f"Too many images. Maximum allowed is 1, but received {num_media}."

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
