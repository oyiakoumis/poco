import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import httpx
from azure.storage.blob import BlobSasPermissions, ContentSettings, generate_blob_sas
from azure.storage.blob.aio import BlobServiceClient

from config import settings
from utils.logging import logger


async def upload_to_blob_storage(media_url: str, content_type: str, message_id: uuid4) -> str:
    """Upload media from Twilio URL to Azure Blob Storage."""
    # Generate a unique blob name using message_id and timestamp
    file_extension = content_type.split("/")[-1]
    if file_extension == "jpeg":
        file_extension = "jpg"  # Standardize jpeg extension
    blob_name = f"{message_id}_{int(time.time())}.{file_extension}"

    # Initialize Azure Blob Storage client
    async with BlobServiceClient.from_connection_string(settings.azure_storage_connection_string) as blob_service_client:
        container_client = blob_service_client.get_container_client(settings.azure_blob_container_name)

        # Create container if it doesn't exist
        if not await container_client.exists():
            await container_client.create_container()

        # Download the media from Twilio using httpx
        async with httpx.AsyncClient() as client:
            auth = (settings.twilio_account_sid, settings.twilio_auth_token)
            media_response = await client.get(media_url, auth=auth, follow_redirects=True)
            if not media_response or not media_response.content:
                raise Exception(f"Failed to download media from Twilio")

        # Upload to Azure Blob Storage
        blob_client = container_client.get_blob_client(blob_name)
        content_settings = ContentSettings(content_type=content_type)
        await blob_client.upload_blob(media_response.content, content_settings=content_settings)

        logger.info(f"Media uploaded to Azure Blob Storage: {blob_name}")
        return blob_name


async def generate_blob_presigned_url(blob_name: str) -> str:
    """Generate a presigned URL for a blob in Azure Blob Storage."""
    async with BlobServiceClient.from_connection_string(settings.azure_storage_connection_string) as blob_service_client:
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
            expiry=datetime.now(timezone.utc) + timedelta(hours=24),
        )

        # Create the presigned URL
        presigned_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        return presigned_url


async def generate_multiple_blob_presigned_urls(blob_names: List[str]) -> Dict[str, Optional[str]]:
    """Generate presigned URLs for multiple blobs concurrently."""
    if not blob_names:
        return {}

    # Create a helper function to handle errors for individual URL generation
    async def get_url_with_error_handling(blob_name: str) -> Tuple[str, Optional[str]]:
        try:
            url = await generate_blob_presigned_url(blob_name)
            return blob_name, url
        except Exception as e:
            logger.error(f"Error generating presigned URL for blob {blob_name}: {str(e)}")
            return blob_name, None

    # Use asyncio.gather to run all URL generation tasks concurrently
    results = await asyncio.gather(*[get_url_with_error_handling(blob_name) for blob_name in blob_names])

    # Convert results to a dictionary
    return {blob_name: url for blob_name, url in results}
