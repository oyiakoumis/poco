"""Message model for chat history."""

from enum import Enum
from typing import Dict, List, Union, Any
import asyncio

from pydantic import Field
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage, AnyMessage

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
    message: AnyMessage = Field(..., description="Actual message from LangChain")
    role: MessageRole = Field(..., description="Role of the message sender")
    metadata: Dict = Field(default_factory=dict, description="Additional metadata for the message")

    async def validate_message(self, message: Dict) -> Union[HumanMessage, AIMessage, SystemMessage, ToolMessage]:
        if self.role == MessageRole.HUMAN:
            langchain_message = HumanMessage.model_validate(message)
            # Check if message has image media
            if self.metadata and self.metadata.get("media_count", 0) > 0:
                # Log when media is present
                media_count = self.metadata["media_count"]
                logger.info(f"Processing message with {media_count} media item(s)")

                # Create multimodal content
                content = []

                # Add text content
                content.append({"type": "text", "text": langchain_message.content})

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
                langchain_message.content = content
            return langchain_message
        elif self.role == MessageRole.ASSISTANT:
            return AIMessage.model_validate(message)
        elif self.role == MessageRole.SYSTEM:
            return SystemMessage.model_validate(message)
        elif self.role == MessageRole.TOOL:
            return ToolMessage.model_validate(message)
        else:
            raise ValueError(f"Invalid message role: {self.role}")
