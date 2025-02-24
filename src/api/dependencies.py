"""API dependencies."""
from typing import AsyncGenerator

from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

from api.config import settings
from document_store.dataset_manager import DatasetManager


async def get_database() -> AsyncGenerator[DatasetManager, None]:
    """Get database connection."""
    client = AsyncIOMotorClient(settings.database_url)
    client.get_io_loop = asyncio.get_running_loop
    
    try:
        db = await DatasetManager.setup(client)
        yield db
    finally:
        client.close()


async def get_db(db: DatasetManager = Depends(get_database)) -> DatasetManager:
    """Dependency for getting database connection."""
    return db
