"""Database setup and initialization using a singleton pattern."""

import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

from database.conversation_store.conversation_manager import ConversationManager
from database.document_store.dataset_manager import DatasetManager
from settings import settings
from utils.logging import logger
from utils.singleton import Singleton


class DatabaseManager(metaclass=Singleton):
    """Singleton database manager class for handling MongoDB connections and managers."""

    def __init__(self):
        """Initialize the database manager."""
        logger.info("Creating new DatabaseManager instance")
        logger.info("Initializing DatabaseManager")
        self._client = AsyncIOMotorClient(settings.database_url)
        self._client.get_io_loop = asyncio.get_running_loop
        self._dataset_manager = None
        self._conversation_manager = None

    async def setup_dataset_manager(self):
        """Initialize and return the dataset manager."""
        if self._dataset_manager is None:
            logger.info("Setting up dataset manager")
            self._dataset_manager = await DatasetManager.setup(self._client)
        return self._dataset_manager

    async def setup_conversation_manager(self):
        """Initialize and return the conversation manager."""
        if self._conversation_manager is None:
            logger.info("Setting up conversation manager")
            self._conversation_manager = await ConversationManager.setup(self._client)
        return self._conversation_manager

    def close(self):
        """Close database connection."""
        logger.info("Closing database connection")
        if self._client:
            self._client.close()
