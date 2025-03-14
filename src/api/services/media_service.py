"""Media handling service for WhatsApp messages."""

from typing import List, Optional

from fastapi import Request

from api.models import MediaItem
from utils.logging import logger
from utils.media_storage import upload_to_blob_storage


class MediaService:
    """Service for handling media in WhatsApp messages."""

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
                blob_name = await upload_to_blob_storage(media_url, media_type)

                # Store blob name instead of URL
                media_items.append(MediaItem(blob_name=blob_name, content_type=media_type))
                logger.info(f"Image uploaded to Azure Blob Storage: {blob_name}")
            except Exception as e:
                logger.error(f"Error uploading image to Azure Blob Storage: {str(e)}")

        return media_items

    async def has_unsupported_media(self, request: Request, num_media: int) -> bool:
        """Check if the message contains unsupported media types.

        Args:
            request: The FastAPI request object
            num_media: The number of media items in the message

        Returns:
            True if the message contains unsupported media, False otherwise
        """
        if not num_media:
            return False

        try:
            form_data = await request.form()
            return any(not form_data.get(f"MediaContentType{i}").startswith("image/") for i in range(num_media))
        except Exception as e:
            logger.error(f"Error checking for unsupported media: {str(e)}")
            return False
