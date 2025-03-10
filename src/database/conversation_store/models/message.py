"""Message model for chat history."""

from datetime import datetime
from enum import Enum
from typing import Dict, Optional

from pydantic import Field

from models.base import BaseDocument, PydanticUUID


class MessageRole(str, Enum):
    """Enum for message roles."""

    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class Message(BaseDocument):
    """Model representing a chat message."""

    conversation_id: PydanticUUID = Field(..., description="ID of the conversation this message belongs to")
    content: str = Field(..., description="Content of the message")
    role: MessageRole = Field(..., description="Role of the message sender")
    metadata: Dict = Field(default_factory=dict, description="Additional metadata for the message")
