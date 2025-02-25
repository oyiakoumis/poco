"""API dependencies."""

import asyncio
from typing import AsyncGenerator, Tuple

from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorClient

from api.config import settings
from conversation_store.conversation_manager import ConversationManager
from document_store.dataset_manager import DatasetManager


async def get_database() -> AsyncGenerator[Tuple[DatasetManager, ConversationManager], None]:
    """Get database connections."""
    client = AsyncIOMotorClient(settings.database_url)
    client.get_io_loop = asyncio.get_running_loop

    try:
        # Setup both managers with the same client
        dataset_manager = await DatasetManager.setup(client)
        conversation_manager = await ConversationManager.setup(client)
        yield dataset_manager, conversation_manager
    finally:
        client.close()


async def get_db(managers: Tuple[DatasetManager, ConversationManager] = Depends(get_database)) -> DatasetManager:
    """Dependency for getting dataset manager."""
    return managers[0]


async def get_conversation_db(managers: Tuple[DatasetManager, ConversationManager] = Depends(get_database)) -> ConversationManager:
    """Dependency for getting conversation manager."""
    return managers[1]
