from typing import Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorCollection

from models.agent_models import ConversationContext, ChatMessage


class ConversationManager:
    """Manager for conversation contexts"""

    def __init__(self, collection: AsyncIOMotorCollection):
        self.collection = collection

    async def get_context(self, user_id: str) -> Optional[ConversationContext]:
        """Get conversation context for a user"""
        doc = await self.collection.find_one({"user_id": user_id})
        if doc:
            # Convert conversation history dictionaries to ChatMessage objects
            history = []
            for msg in doc.get("conversation_history", []):
                if isinstance(msg, dict) and "role" in msg and "content" in msg:
                    history.append(ChatMessage(role=msg["role"], content=msg["content"]))
            
            return ConversationContext(
                user_id=doc["user_id"],
                conversation_history=history,
                last_collection=doc.get("last_collection"),
                last_document_ids=doc.get("last_document_ids"),
            )
        return None

    async def update_context(
        self, user_id: str, query: str, response: str, collection_name: Optional[str] = None, document_ids: Optional[List[str]] = None
    ) -> None:
        """Update conversation context with new interaction"""
        # Get existing context or create new one
        context = await self.get_context(user_id)
        if not context:
            context = ConversationContext(user_id=user_id, conversation_history=[], last_collection=None, last_document_ids=None)

        # Add new interaction to history as chat messages
        context.conversation_history.extend([
            ChatMessage(role="user", content=query),
            ChatMessage(role="assistant", content=response)
        ])

        # Keep only last 10 interactions
        if len(context.conversation_history) > 10:
            context.conversation_history = context.conversation_history[-10:]

        # Update last accessed collection and documents
        context.last_collection = collection_name
        context.last_document_ids = document_ids

        # Save to database
        await self.collection.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "user_id": context.user_id,
                    "conversation_history": [msg.dict() for msg in context.conversation_history],
                    "last_collection": context.last_collection,
                    "last_document_ids": context.last_document_ids,
                }
            },
            upsert=True,
        )

    async def clear_context(self, user_id: str) -> None:
        """Clear conversation context for a user"""
        await self.collection.delete_one({"user_id": user_id})
