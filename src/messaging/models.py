"""Message models for Azure Service Bus."""

from typing import Dict, List, Optional

from pydantic import BaseModel

from models.base import PydanticUUID


class MediaItem(BaseModel):
    """Media item model for WhatsApp messages."""

    blob_name: str
    content_type: str


class WhatsAppQueueMessage(BaseModel):
    """WhatsApp message model for Azure Service Bus queue."""

    from_number: str
    body: str
    profile_name: Optional[str]
    wa_id: str
    sms_message_sid: str
    user_id: str
    conversation_id: PydanticUUID
    message_id: PydanticUUID
    request_url: Optional[str] = None
    media_count: int = 0
    media_items: List[MediaItem] = []
    unsupported_media: bool = False
