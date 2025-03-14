"""Conversation management service for WhatsApp messages."""

import asyncio
from typing import List, Optional, Tuple
from uuid import UUID, uuid4

from langchain_core.messages import AnyMessage, HumanMessage, SystemMessage

from agents.assistant import ASSISTANT_SYSTEM_MESSAGE
from api.models import MediaItem
from database.conversation_store.conversation_manager import ConversationManager
from database.conversation_store.models.message import Message, MessageRole
from utils.azure_blob_lock import AzureBlobLockManager
from utils.logging import logger
from api.utils.text import Command, extract_message_after_command, is_command


class ConversationService:
    """Service for managing conversations and messages."""

    def __init__(self, conversation_db: ConversationManager, lock_manager: AzureBlobLockManager):
        """Initialize the conversation service.

        Args:
            conversation_db: The conversation database manager
            lock_manager: The Azure Blob lock manager
        """
        self.conversation_db = conversation_db
        self.lock_manager = lock_manager

    async def get_or_create_conversation(self, user_id: str, message_body: str) -> Tuple[UUID, bool, Optional[str]]:
        """Get an existing conversation or create a new one.

        Args:
            user_id: The user ID
            message_body: The message body

        Returns:
            A tuple containing:
            - The conversation ID
            - A boolean indicating if a new conversation was created
            - The processed message body (command removed if present)
        """
        new_conversation_created = False
        processed_message = message_body

        # Check if the user wants to start a new conversation
        if is_command(message_body, Command.NEW_CONVERSATION):
            conversation_id = uuid4()
            await self.conversation_db.create_conversation(user_id, str(conversation_id), conversation_id)

            # Remove the command from the message body for processing
            processed_message = extract_message_after_command(message_body, Command.NEW_CONVERSATION)
            new_conversation_created = True

        # Use the latest conversation if it exists
        elif latest_conversation := await self.conversation_db.get_latest_conversation(user_id):
            conversation_id = latest_conversation.id

        # Else create a new conversation
        else:
            conversation_id = uuid4()
            await self.conversation_db.create_conversation(user_id, str(conversation_id), conversation_id)
            new_conversation_created = True

        return conversation_id, new_conversation_created, processed_message

    async def acquire_conversation_lock(self, conversation_id: UUID) -> Optional[dict]:
        """Acquire a lock for the conversation.

        Args:
            conversation_id: The conversation ID

        Returns:
            The lock object if acquired, None otherwise
        """
        return self.lock_manager.acquire_lock(conversation_id)

    def release_conversation_lock(self, lock: dict) -> bool:
        """Release a conversation lock.

        Args:
            lock: The lock object

        Returns:
            True if the lock was released, False otherwise
        """
        return self.lock_manager.release_lock(lock)

    async def prepare_messages(
        self,
        user_id: str,
        conversation_id: UUID,
        message_body: str,
        new_conversation: bool,
        metadata: dict,
    ) -> List[Message]:
        """Prepare messages for processing.

        Args:
            user_id: The user ID
            conversation_id: The conversation ID
            message_body: The message body
            new_conversation: Whether this is a new conversation
            metadata: Additional metadata for the message

        Returns:
            A list of messages to be processed
        """
        # Prepare input messages
        new_messages = []

        # Add system message for new conversations
        if new_conversation:
            new_messages.append(
                Message(user_id=user_id, conversation_id=conversation_id, role=MessageRole.SYSTEM, message=SystemMessage(content=ASSISTANT_SYSTEM_MESSAGE))
            )

        # Add the user message
        new_messages.append(
            Message(
                user_id=user_id,
                conversation_id=conversation_id,
                role=MessageRole.HUMAN,
                message=HumanMessage(content=message_body),
                metadata=metadata,
            )
        )

        return new_messages

    async def get_conversation_history(self, user_id: str, conversation_id: UUID, new_conversation: bool) -> List[Message]:
        """Get the conversation history.

        Args:
            user_id: The user ID
            conversation_id: The conversation ID
            new_conversation: Whether this is a new conversation

        Returns:
            A list of messages in the conversation history
        """
        if new_conversation:
            return []

        return await self.conversation_db.list_messages(user_id, conversation_id)

    async def process_image_urls(self, messages: List[Message]) -> None:
        """Process image URLs for all messages.

        Args:
            messages: The list of messages to process
        """
        await asyncio.gather(*[message.get_image_urls() for message in messages])

    async def store_messages(self, user_id: str, messages: List[Message]) -> None:
        """Store messages in the database.

        Args:
            user_id: The user ID
            messages: The messages to store
        """
        await self.conversation_db.create_messages(user_id, messages)
