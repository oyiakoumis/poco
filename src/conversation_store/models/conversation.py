"""Conversation model for chat history."""

from datetime import datetime
from typing import Optional

from pydantic import Field

from models.base import BaseDocument, PydanticUUID


class Conversation(BaseDocument):
    """Model representing a chat conversation."""

    title: str = Field(..., description="Title of the conversation")
