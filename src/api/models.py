"""API request and response models."""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field

from models.base import PydanticUUID


class ChatRequest(BaseModel):
    """Chat request model."""

    message: str = Field(..., description="The message content")
    user_id: str = Field(..., description="User identifier")
    conversation_id: PydanticUUID = Field(..., description="Conversation identifier")
    time_zone: str = Field(default="UTC", description="User's timezone")
    first_day_of_week: int = Field(default=0, description="First day of week (0=Sunday, 1=Monday, etc.)", ge=0, le=6)

    @property
    def thread_id(self) -> str:
        """Get thread_id from conversation_id."""
        return str(self.conversation_id)

    def to_messages(self) -> List[HumanMessage]:
        """Convert request to LangChain messages."""
        return [HumanMessage(content=self.message)]


class ChatResponse(BaseModel):
    """Chat response model."""

    message: Optional[str] = Field(default=None, description="Assistant's complete response message")
    conversation_id: Optional[PydanticUUID] = Field(default=None, description="Conversation identifier")
    delta: Optional[str] = Field(default=None, description="New content to append to the response (for streaming)")
    done: Optional[bool] = Field(default=None, description="Whether this is the final event (for streaming)")
    error: Optional[str] = Field(default=None, description="Error message if an error occurred")


class ConversationCreate(BaseModel):
    """Request model for creating a conversation."""

    title: str = Field(..., description="Title of the conversation")
    user_id: str = Field(..., description="User identifier")


class ConversationUpdate(BaseModel):
    """Request model for updating a conversation."""

    title: Optional[str] = Field(default=None, description="New title for the conversation")


class ConversationResponse(BaseModel):
    """Response model for conversation operations."""

    id: str = Field(..., description="Conversation identifier")
    title: str = Field(..., description="Title of the conversation")
    user_id: str = Field(..., description="User identifier")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")


class MessageCreate(BaseModel):
    """Request model for creating a message."""

    content: str = Field(..., description="Content of the message")
    user_id: str = Field(..., description="User identifier")


class MessageResponse(BaseModel):
    """Response model for message operations."""

    id: str = Field(..., description="Message identifier")
    conversation_id: str = Field(..., description="Conversation identifier")
    content: str = Field(..., description="Content of the message")
    role: str = Field(..., description="Role of the message sender")
    user_id: str = Field(..., description="User identifier")
    created_at: datetime = Field(..., description="Creation timestamp")


class ConversationListResponse(BaseModel):
    """Response model for listing conversations."""

    conversations: List[ConversationResponse] = Field(..., description="List of conversations")
    total: int = Field(..., description="Total number of conversations")


class MessageListResponse(BaseModel):
    """Response model for listing messages."""

    messages: List[MessageResponse] = Field(..., description="List of messages")
    total: int = Field(..., description="Total number of messages")
