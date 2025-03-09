import time
from uuid import uuid4

import httpx
from azure.storage.blob import ContentSettings
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
