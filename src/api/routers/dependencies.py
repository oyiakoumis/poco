"""Dependencies for the chat router."""

from fastapi import Depends, Header, Request
from twilio.request_validator import RequestValidator

from api.utils.twilio_validator import extract_twilio_form_data, validate_twilio_request
from database.manager import DatabaseManager
from settings import settings
from utils.azure_blob_lock import AzureBlobLockManager
from utils.logging import logger


async def get_database_manager():
    """Dependency for database manager."""
    database_manager = DatabaseManager()
    return database_manager


async def get_blob_lock_manager():
    """Dependency for Azure Blob lock manager."""
    return AzureBlobLockManager()


async def validate_twilio_signature(
    request: Request,
    x_twilio_signature: str = Header(None),
    request_url: str = Header(None, alias="X-Original-URL"),
):
    """Validate that the request is coming from Twilio.

    Note: Currently disabled in production code with a TODO.

    Args:
        request: The FastAPI request object
        x_twilio_signature: The Twilio signature from the request headers
        request_url: The URL the request was sent to

    Returns:
        True if the request is valid, False otherwise
    """
    # Extract form data
    form_data = await extract_twilio_form_data(request)

    # If request_url is not provided, construct it from the settings
    url = request_url or f"{settings.api_url}:{settings.port}/chat/"

    # Validate the request
    return validate_twilio_request(form_data, x_twilio_signature, url)
