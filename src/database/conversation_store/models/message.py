"""Message model for chat history."""

from enum import Enum
from typing import Dict, List, Optional
from uuid import uuid4

from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from pydantic import Field, model_validator

from api.models import MediaItem
from api.services.media_service import BlobStorageService
from models.base import BaseDocument, PydanticUUID
from utils.logging import logger


class MessageRole(str, Enum):
    """Enum for message roles."""

    HUMAN = "human"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class Message(BaseDocument):
    """Model representing a chat message."""

    conversation_id: PydanticUUID = Field(..., description="ID of the conversation this message belongs to")
    role: Optional[MessageRole] = Field(default=None, description="Role of the message sender")
    message: AnyMessage = Field(..., description="Actual message from LangChain")
    metadata: Dict = Field(default_factory=dict, description="Additional metadata for the message")

    def model_post_init(self, __context) -> None:
        """Ensure message ID matches document ID after initialization."""
        # Set message.id to match the document ID
        if hasattr(self, "message") and hasattr(self, "id"):
            self.message.id = str(self.id)

    @model_validator(mode="before")
    def validate_role_and_message(cls, data):
        """Validate and process role and message together to avoid circular dependencies."""
        role = data.get("role")
        message = data.get("message")

        # Map message types to corresponding roles
        message_type_to_role = {
            HumanMessage: MessageRole.HUMAN,
            AIMessage: MessageRole.ASSISTANT,
            SystemMessage: MessageRole.SYSTEM,
            ToolMessage: MessageRole.TOOL,
        }

        # Handle dict message case
        if isinstance(message, dict):
            # If message is a dict, role must be provided
            if role is None:
                raise ValueError("Role must be provided if message is a dict")

            # Deserialize message based on role
            if role == MessageRole.HUMAN:
                message = HumanMessage.model_validate(message)
            elif role == MessageRole.ASSISTANT:
                message = AIMessage.model_validate(message)
            elif role == MessageRole.SYSTEM:
                message = SystemMessage.model_validate(message)
            elif role == MessageRole.TOOL:
                message = ToolMessage.model_validate(message)
            else:
                raise ValueError(f"Invalid message role: {role}")
        elif isinstance(message, (HumanMessage, AIMessage, SystemMessage, ToolMessage)):
            # Check if message is one of the recognized types and handle role
            for message_type, corresponding_role in message_type_to_role.items():
                if isinstance(message, message_type):
                    # If role is not provided, infer it from message type
                    if role is None:
                        role = corresponding_role
                    # If role is provided, ensure it matches the message type
                    elif role != corresponding_role:
                        raise ValueError(f"Role mismatch: {role} provided but message is of type {message_type.__name__}")
                    break
        else:
            raise ValueError("Message must be a dict or an instance of HumanMessage, AIMessage, SystemMessage, or ToolMessage")

        # Update the data with our processed values
        data["role"] = role
        data["message"] = message
        return data

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
        media_items: List[MediaItem] = self.metadata["media_items"]
        blob_names = [media_item.blob_name for media_item in media_items]

        # Generate all presigned URLs concurrently
        url_results = await BlobStorageService().generate_multiple_blob_presigned_urls(blob_names)

        # Process results and add to content
        for blob_name in blob_names:
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
