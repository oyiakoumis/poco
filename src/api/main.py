"""Main FastAPI application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

from api.config import settings
from api.routers import chat, conversation
from conversation_store.conversation_manager import ConversationManager
from document_store.dataset_manager import DatasetManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup (if any)
    yield
    # Clear collections on shutdown
    client = AsyncIOMotorClient(settings.database_url)
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
    app.include_router(conversation.router)

    return app


app = create_app()
