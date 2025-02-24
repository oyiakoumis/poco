"""API request and response models."""
from typing import List, Optional

from langchain.schema import HumanMessage
from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """Chat request model."""
    
    message: str = Field(..., description="The message content")
    user_id: str = Field(..., description="User identifier")
    thread_id: str = Field(..., description="Thread identifier")
    time_zone: str = Field(default="UTC", description="User's timezone")
    first_day_of_week: int = Field(
        default=0,
        description="First day of week (0=Sunday, 1=Monday, etc.)",
        ge=0,
        le=6
    )

    def to_messages(self) -> List[HumanMessage]:
        """Convert request to LangChain messages."""
        return [HumanMessage(content=self.message)]


class ChatResponse(BaseModel):
    """Chat response model."""
    
    message: str = Field(..., description="Assistant's response message")
