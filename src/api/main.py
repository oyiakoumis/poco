"""Main FastAPI application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

from api.routers import chat
from api.services.media_service import BlobStorageService
from settings import settings
from utils.azure_blob_lock import AzureBlobLockManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup (if any)
    yield

    # Code to run on shutdown (if any)
    # Close MongoDB connection
    client = AsyncIOMotorClient(settings.database_connection_string)
    client.close()
    
    # Close Azure Blob Storage connection
    blob_storage = BlobStorageService()
    await blob_storage.close()
    
    # Close Azure Blob Lock Manager
    lock_manager = AzureBlobLockManager()
    lock_manager.close()


def create_app() -> FastAPI:
    """Create FastAPI application."""
    app = FastAPI(title=settings.api_title, version=settings.api_version, description=settings.api_description, lifespan=lifespan)

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Allows all origins
        allow_credentials=True,
        allow_methods=["*"],  # Allows all methods
        allow_headers=["*"],  # Allows all headers
    )

    # Add routers
    app.include_router(chat.router)

    return app


app = create_app()
