"""Twilio request validation utilities."""

from fastapi import HTTPException, Request, status
from twilio.request_validator import RequestValidator

from settings import settings
from utils.logging import logger


def validate_twilio_request(request_data: dict, signature: str, url: str) -> bool:
    """Validate that the request is coming from Twilio.

    Args:
        request_data: The request data to validate
        signature: The Twilio signature from the request headers
        url: The URL the request was sent to

    Returns:
        True if the request is valid, False otherwise
    """
    if not settings.twilio_auth_token or not signature:
        logger.warning("Missing Twilio auth token or signature")
        return False

    validator = RequestValidator(settings.twilio_auth_token)
    return validator.validate(url, request_data, signature)


async def extract_twilio_form_data(request: Request) -> dict:
    """Extract form data from a Twilio request.

    Args:
        request: The FastAPI request object

    Returns:
        A dictionary containing the form data
    """
    try:
        form_data = await request.form()
        return {key: value for key, value in form_data.items()}
    except Exception as e:
        logger.error(f"Error extracting form data: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid form data")
