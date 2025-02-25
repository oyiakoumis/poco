"""Main FastAPI application."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import settings
from api.routers import chat, conversation


def create_app() -> FastAPI:
    """Create FastAPI application."""
    app = FastAPI(title=settings.api_title, version=settings.api_version, description=settings.api_description)

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
