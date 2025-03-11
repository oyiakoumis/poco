"""Message model for chat history."""

from enum import Enum
from typing import Dict

from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from pydantic import Field, field_validator

from models.base import BaseDocument, PydanticUUID
from utils.logging import logger
from utils.media_storage import generate_multiple_blob_presigned_urls


class MessageRole(str, Enum):
    """Enum for message roles."""

    HUMAN = "human"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class Message(BaseDocument):
    """Model representing a chat message."""

    conversation_id: PydanticUUID = Field(..., description="ID of the conversation this message belongs to")
    role: MessageRole = Field(..., description="Role of the message sender")
    message: AnyMessage = Field(..., description="Actual message from LangChain")
    metadata: Dict = Field(default_factory=dict, description="Additional metadata for the message")

    @field_validator("message", mode="before")
    def validate_message(cls, message_dict, info):
        """Deserialize the message before model validation."""
        # Get the role from the validation context
        values = info.data
        role = values.get("role")

        if role == MessageRole.HUMAN:
            return HumanMessage.model_validate(message_dict)
        elif role == MessageRole.ASSISTANT:
            return AIMessage.model_validate(message_dict)
        elif role == MessageRole.SYSTEM:
            return SystemMessage.model_validate(message_dict)
        elif role == MessageRole.TOOL:
            return ToolMessage.model_validate(message_dict)
        else:
            raise ValueError(f"Invalid message role: {role}")

    async def get_image_urls(self) -> None:
        """Process image media in human messages and update content with presigned URLs."""
        # Only process human messages with media
        if self.role != MessageRole.HUMAN or not self.metadata or self.metadata.get("media_count", 0) <= 0:
            return

        # Log when media is present
        media_count = self.metadata["media_count"]
        logger.info(f"Processing message with {media_count} media item(s)")

        # Create multimodal content
        content = []

        # Add text content
        content.append({"type": "text", "text": self.message.content})

        # Add images with presigned URLs using concurrent generation
        url_generation_failures = 0
        url_generation_successes = 0

        # Extract all blob names from media items
        media_items = self.metadata["media_items"]
        blob_names = [media_item["blob_name"] for media_item in media_items]

        # Generate all presigned URLs concurrently
        url_results = await generate_multiple_blob_presigned_urls(blob_names)

        # Process results and add to content
        for media_item in media_items:
            blob_name = media_item["blob_name"]
            presigned_url = url_results.get(blob_name)

            if presigned_url:
                # Add image with presigned URL to content
                content.append({"type": "image_url", "image_url": {"url": presigned_url}})
                url_generation_successes += 1
                logger.debug(f"Successfully generated presigned URL for image: {blob_name}")
            else:
                # URL generation failed for this blob
                url_generation_failures += 1

        # Log summary of URL generation results
        if url_generation_failures > 0 or url_generation_successes > 0:
            logger.info(f"Presigned URL generation summary: {url_generation_successes} successful, {url_generation_failures} failed")

        # Update message content with multimodal content
        self.message.content = content
