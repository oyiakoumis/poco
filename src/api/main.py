"""Main FastAPI application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

from api.config import settings
from api.routers import chat
from database.conversation_store.conversation_manager import ConversationManager
from database.document_store.dataset_manager import DatasetManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup (if any)
    yield

    client = AsyncIOMotorClient(settings.database_url)
    if False:
        # Clear collections on shutdown
        db = client.get_database(DatasetManager.DATABASE)

        # Clear collections used by ConversationManager
        await db.get_collection(ConversationManager.COLLECTION_CONVERSATIONS).delete_many({})
        await db.get_collection(ConversationManager.COLLECTION_MESSAGES).delete_many({})

        # Clear collections used by DatasetManager
        await db.get_collection(DatasetManager.COLLECTION_DATASETS).delete_many({})
        await db.get_collection(DatasetManager.COLLECTION_RECORDS).delete_many({})

    client.close()


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
