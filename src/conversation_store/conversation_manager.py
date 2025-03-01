"""Manager for conversation and message operations."""

from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID, uuid4

import pymongo
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

from conversation_store.exceptions import (
    ConversationNotFoundError,
    InvalidConversationError,
    InvalidMessageError,
    MessageNotFoundError,
)
from conversation_store.models.conversation import Conversation
from conversation_store.models.message import Message, MessageRole
from utils.logging import logger


class ConversationManager:
    """Manager for conversation and message operations."""

    DATABASE: str = "document_store"  # Reuse the same database
    COLLECTION_CONVERSATIONS: str = "conversations"
    COLLECTION_MESSAGES: str = "messages"

    def __init__(self, mongodb_client: AsyncIOMotorClient) -> None:
        """Initialize manager with MongoDB client.
        Note: Use ConversationManager.setup() to create a properly initialized instance."""
        self.client = mongodb_client
        self._db: AsyncIOMotorDatabase = self.client.get_database(self.DATABASE)
        self._conversations: AsyncIOMotorCollection = self._db.get_collection(self.COLLECTION_CONVERSATIONS)
        self._messages: AsyncIOMotorCollection = self._db.get_collection(self.COLLECTION_MESSAGES)

    @classmethod
    async def setup(cls, mongodb_client: AsyncIOMotorClient) -> "ConversationManager":
        """Factory method to create and setup a ConversationManager instance."""
        try:
            # Create manager instance
            manager = cls(mongodb_client)

            # Setup conversations collection indexes
            await manager._conversations.create_indexes(
                [
                    # Index for listing user's conversations
                    pymongo.IndexModel([("user_id", 1)], background=True),
                    # Index for conversation lookups
                    pymongo.IndexModel([("user_id", 1), ("_id", 1)], background=True),
                    # Index for title search
                    pymongo.IndexModel([("title", "text")], background=True),
                ]
            )

            # Setup messages collection indexes
            await manager._messages.create_indexes(
                [
                    # Index for listing conversation messages
                    pymongo.IndexModel(
                        [("conversation_id", 1), ("timestamp", 1)],
                        background=True,
                    ),
                    # Index for message lookups
                    pymongo.IndexModel(
                        [("user_id", 1), ("conversation_id", 1), ("_id", 1)],
                        background=True,
                    ),
                ]
            )

            return manager

        except Exception as e:
            raise InvalidConversationError(f"Failed to setup indexes: {str(e)}")

    async def create_conversation(self, user_id: str, title: str) -> UUID:
        """Creates a new conversation without an initial message."""
        try:
            logger.info(f"Creating conversation '{title}' for user {user_id}")

            # Create conversation with UUID
            conversation_id = uuid4()
            conversation = Conversation(
                id=str(conversation_id),
                user_id=user_id,
                title=title,
            )

            await self._conversations.insert_one(conversation.model_dump(by_alias=True))

            logger.info(f"Conversation created with ID: {conversation_id}")
            return conversation_id

        except Exception as e:
            raise InvalidConversationError(f"Failed to create conversation: {str(e)}")

    async def get_conversation(self, user_id: str, conversation_id: UUID) -> Conversation:
        """Retrieves a specific conversation."""
        try:
            logger.debug(f"Getting conversation {conversation_id} for user {user_id}")
            doc = await self._conversations.find_one({"_id": str(conversation_id), "user_id": user_id})
            if not doc:
                raise ConversationNotFoundError(f"Conversation {conversation_id} not found")
            return Conversation.model_validate(doc)

        except ConversationNotFoundError:
            raise
        except Exception as e:
            raise InvalidConversationError(f"Failed to get conversation: {str(e)}")

    async def conversation_exists(self, user_id: str, conversation_id: UUID) -> bool:
        """Checks if a conversation exists."""
        try:
            logger.debug(f"Checking if conversation {conversation_id} exists for user {user_id}")
            doc = await self._conversations.find_one(
                {"_id": str(conversation_id), "user_id": user_id}, projection={"_id": 1}  # Only retrieve the ID field for efficiency
            )
            return doc is not None
        except Exception as e:
            logger.error(f"Error checking if conversation exists: {str(e)}")
            return False

    async def list_conversations(self, user_id: str, limit: int = 50, skip: int = 0) -> List[Conversation]:
        """Lists all conversations for a user."""
        try:
            logger.info(f"Listing conversations for user {user_id}")
            cursor = self._conversations.find({"user_id": user_id})
            # Sort by last updated
            cursor = cursor.sort([("updated_at", -1)])
            # Apply pagination
            cursor = cursor.skip(skip).limit(limit)

            conversations = []
            async for doc in cursor:
                conversations.append(Conversation.model_validate(doc))
            return conversations

        except Exception as e:
            raise InvalidConversationError(f"Failed to list conversations: {str(e)}")

    async def update_conversation(
        self,
        user_id: str,
        conversation_id: UUID,
        title: Optional[str] = None,
    ) -> None:
        """Updates a conversation."""
        try:
            logger.info(f"Updating conversation {conversation_id} for user {user_id}")
            # Verify conversation exists and belongs to user
            conversation = await self.get_conversation(user_id, conversation_id)

            # Prepare update data
            update_data = {"updated_at": datetime.now(tz=timezone.utc)}
            if title is not None:
                update_data["title"] = title

            # Update in database
            result = await self._conversations.update_one(
                {"_id": str(conversation_id), "user_id": user_id},
                {"$set": update_data},
            )

            if result.modified_count == 0:
                logger.warning(f"No changes made to conversation {conversation_id}")

        except ConversationNotFoundError:
            raise
        except Exception as e:
            raise InvalidConversationError(f"Failed to update conversation: {str(e)}")

    async def delete_conversation(self, user_id: str, conversation_id: UUID) -> None:
        """Deletes a conversation and all its messages."""
        try:
            logger.info(f"Deleting conversation {conversation_id} and its messages for user {user_id}")
            # Verify conversation exists and belongs to user
            await self.get_conversation(user_id, conversation_id)

            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Delete conversation and its messages
                    await self._messages.delete_many(
                        {
                            "user_id": user_id,
                            "conversation_id": str(conversation_id),
                        },
                        session=session,
                    )

                    result = await self._conversations.delete_one(
                        {
                            "_id": str(conversation_id),
                            "user_id": user_id,
                        },
                        session=session,
                    )

                    if result.deleted_count == 0:
                        raise ConversationNotFoundError(f"Conversation {conversation_id} not found")
                    logger.info("Conversation and messages deleted successfully")

        except ConversationNotFoundError:
            raise
        except Exception as e:
            raise InvalidConversationError(f"Failed to delete conversation: {str(e)}")

    async def create_message(self, user_id: str, conversation_id: UUID, content: str, role: MessageRole, metadata: Optional[Dict] = None) -> UUID:
        """Creates a new message in a conversation."""
        try:
            logger.info(f"Creating message in conversation {conversation_id} for user {user_id}")
            # Verify conversation exists and belongs to user
            await self.get_conversation(user_id, conversation_id)

            # Create message with UUID
            message_id = uuid4()
            message = Message(
                id=str(message_id),
                user_id=user_id,
                conversation_id=str(conversation_id),
                content=content,
                role=role,
                metadata=metadata or {},
            )

            # Insert message and update conversation timestamp in a transaction
            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    await self._messages.insert_one(
                        message.model_dump(by_alias=True),
                        session=session,
                    )

                    # Update conversation timestamp
                    await self._conversations.update_one(
                        {"_id": str(conversation_id), "user_id": user_id},
                        {"$set": {"updated_at": datetime.now(tz=timezone.utc)}},
                        session=session,
                    )

            logger.info(f"Message created with ID: {message_id}")
            return message_id

        except ConversationNotFoundError:
            raise
        except Exception as e:
            raise InvalidMessageError(f"Failed to create message: {str(e)}")

    async def get_message(self, user_id: str, message_id: UUID) -> Message:
        """Retrieves a specific message."""
        try:
            logger.debug(f"Getting message {message_id} for user {user_id}")
            doc = await self._messages.find_one({"_id": str(message_id), "user_id": user_id})
            if not doc:
                raise MessageNotFoundError(f"Message {message_id} not found")
            return Message.model_validate(doc)

        except MessageNotFoundError:
            raise
        except Exception as e:
            raise InvalidMessageError(f"Failed to get message: {str(e)}")

    async def list_messages(self, user_id: str, conversation_id: UUID, limit: int = 100, skip: int = 0) -> List[Message]:
        """Lists all messages in a conversation."""
        try:
            logger.info(f"Listing messages for conversation {conversation_id}")
            # Verify conversation exists and belongs to user
            await self.get_conversation(user_id, conversation_id)

            cursor = self._messages.find({"user_id": user_id, "conversation_id": str(conversation_id)})
            # Sort by timestamp (oldest first)
            cursor = cursor.sort([("created_at", 1)])
            # Apply pagination
            cursor = cursor.skip(skip).limit(limit)

            messages = []
            async for doc in cursor:
                messages.append(Message.model_validate(doc))
            return messages

        except ConversationNotFoundError:
            raise
        except Exception as e:
            raise InvalidMessageError(f"Failed to list messages: {str(e)}")

    async def delete_message(self, user_id: str, message_id: UUID) -> None:
        """Deletes a message."""
        try:
            logger.info(f"Deleting message {message_id} for user {user_id}")
            result = await self._messages.delete_one({"_id": str(message_id), "user_id": user_id})

            if result.deleted_count == 0:
                raise MessageNotFoundError(f"Message {message_id} not found")
            logger.info("Message deleted successfully")

        except MessageNotFoundError:
            raise
        except Exception as e:
            raise InvalidMessageError(f"Failed to delete message: {str(e)}")
