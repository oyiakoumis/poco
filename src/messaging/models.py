"""Message models for Azure Service Bus."""

from typing import Dict, Optional
from uuid import UUID

from pydantic import BaseModel


class WhatsAppQueueMessage(BaseModel):
    """WhatsApp message model for Azure Service Bus queue."""

    from_number: str
    body: str
    profile_name: Optional[str]
    wa_id: str
    sms_message_sid: str
    user_id: str
    conversation_id: UUID
    message_id: UUID
    request_url: Optional[str] = None
