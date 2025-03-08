"""Message models for Azure Service Bus."""

from typing import Optional

from pydantic import BaseModel

from models.base import PydanticUUID


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
