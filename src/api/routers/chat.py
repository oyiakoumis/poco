"""Chat router for handling message processing."""

from enum import Enum
import time
from typing import Optional
from uuid import uuid4

import httpx
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobServiceClient
from fastapi import APIRouter, Form, Header, Request, Response
from twilio.request_validator import RequestValidator
from twilio.rest import Client
from twilio.twiml.messaging_response import MessagingResponse

from config import settings
from database.conversation_store.models.message import MessageRole
from database.manager import DatabaseManager
from messaging.models import MediaItem, WhatsAppQueueMessage
from messaging.producer import WhatsAppMessageProducer
from utils.logging import logger
from utils.text import format_message

router = APIRouter(prefix="/chat", tags=["chat"])


class Command(str, Enum):
    """Enum for WhatsApp commands."""
    NEW_CONVERSATION = "/new"


async def upload_to_blob_storage(media_url: str, content_type: str, message_id: uuid4) -> str:
    """Upload media from Twilio URL to Azure Blob Storage."""
    # Generate a unique blob name using message_id and timestamp
    file_extension = content_type.split("/")[-1]
    if file_extension == "jpeg":
        file_extension = "jpg"  # Standardize jpeg extension
    blob_name = f"{message_id}_{int(time.time())}.{file_extension}"

    # Initialize Azure Blob Storage client
    async with BlobServiceClient.from_connection_string(settings.azure_storage_connection_string) as blob_service_client:
        container_client = blob_service_client.get_container_client(settings.azure_blob_container_name)

        # Create container if it doesn't exist
        if not await container_client.exists():
            await container_client.create_container()

        # Download the media from Twilio using httpx
        async with httpx.AsyncClient() as client:
            auth = (settings.twilio_account_sid, settings.twilio_auth_token)
            media_response = await client.get(media_url, auth=auth, follow_redirects=True)
            if not media_response or not media_response.content:
                raise Exception(f"Failed to download media from Twilio")

        # Upload to Azure Blob Storage
        blob_client = container_client.get_blob_client(blob_name)
        content_settings = ContentSettings(content_type=content_type)
        await blob_client.upload_blob(media_response.content, content_settings=content_settings)

        logger.info(f"Media uploaded to Azure Blob Storage: {blob_name}")
        return blob_name


def validate_twilio_request(request_data: dict, signature: str, url: str) -> bool:
    """Validate that the request is coming from Twilio."""
    validator = RequestValidator(settings.twilio_auth_token)
    return validator.validate(url, request_data, signature)


def is_command(message: str, command: str) -> bool:
    """Check if the message starts with a specific command."""
    return message.strip().startswith(command)


def extract_message_after_command(message: str, command: str) -> str:
    """Extract the message content after a command."""
    if not message.strip().startswith(command):
        return message
        
    return message[message.find(command) + len(command):].strip()


@router.post("/whatsapp", response_class=Response)
async def process_whatsapp_message(
    request: Request,
    From: str = Form(...),
    Body: str = Form(...),
    ProfileName: Optional[str] = Form(None),
    WaId: str = Form(...),
    SmsMessageSid: str = Form(...),
    NumMedia: Optional[int] = Form(0),
    x_twilio_signature: str = Header(None),
    request_url: str = Header(None, alias="X-Original-URL"),
) -> Response:
    """Process incoming WhatsApp messages from Twilio."""
    logger.info(f"WhatsApp message received from {From}, SID: {SmsMessageSid}")

    # Validate the request is coming from Twilio
    if settings.twilio_auth_token and x_twilio_signature:
        # Create a dictionary of form fields for Twilio validation
        request_data = {
            "From": From,
            "Body": Body,
            "ProfileName": ProfileName or "",
            "WaId": WaId,
            "SmsMessageSid": SmsMessageSid,
            "NumMedia": str(NumMedia) if NumMedia is not None else "0",
        }
        # If request_url is not provided, construct it from the settings
        url = request_url or f"https://{settings.api_url}:{settings.port}/chat/whatsapp"

        if not validate_twilio_request(request_data, x_twilio_signature, url):
            logger.warning(f"Invalid Twilio signature: {x_twilio_signature}")
            # raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    # Use the WhatsApp number as the user ID
    user_id = From

    database_manager = DatabaseManager()
    conversation_db = await database_manager.setup_conversation_manager()

    # Flag to track if a new conversation was created by command
    new_conversation_created = False
    
    if is_command(Body, Command.NEW_CONVERSATION):
        # Create a new conversation regardless of existing ones
        conversation_id = uuid4()
        await conversation_db.create_conversation(user_id, str(conversation_id), conversation_id)
        # Remove the command from the message body for processing
        Body = extract_message_after_command(Body, Command.NEW_CONVERSATION)
        new_conversation_created = True
    elif latest_conversation := await conversation_db.get_latest_conversation(user_id):
        # Use the latest conversation if it exists, otherwise create a new one
        conversation_id = latest_conversation.id
    else:
        # No conversation found, create a new one
        conversation_id = uuid4()
        # Create a title based on the user's profile name or number
        logger.info(f"Creating new WhatsApp conversation: {conversation_id}")
        await conversation_db.create_conversation(user_id, str(conversation_id), conversation_id)

    # Create a message ID for the incoming message
    message_id = uuid4()

    # Extract media information (images only)
    media_items = []
    unsupported_media = False
    num_media = int(NumMedia) if NumMedia is not None else 0

    for i in range(num_media):
        form_data = await request.form()
        media_url = form_data.get(f"MediaUrl{i}")
        media_type = form_data.get(f"MediaContentType{i}")

        if media_url and media_type:
            if media_type.startswith("image/"):
                try:
                    # Upload image to Azure Blob Storage
                    blob_name = await upload_to_blob_storage(media_url, media_type, message_id)

                    # Store blob name instead of URL
                    media_items.append(MediaItem(blob_name=blob_name, content_type=media_type))
                    logger.info(f"Image uploaded to Azure Blob Storage: {blob_name}")
                except Exception as e:
                    logger.error(f"Error uploading image to Azure Blob Storage: {str(e)}")
                    # Skip this image
            else:
                # Flag that we received unsupported media
                unsupported_media = True
                logger.info(f"Unsupported media type received: {media_type}")

    # WhatsApp-specific metadata
    metadata = {
        "whatsapp_id": WaId,
        "sms_message_sid": SmsMessageSid,
        "media_count": len(media_items),
        "media_items": media_items,
        "unsupported_media": unsupported_media,
    }

    # Store user message
    await conversation_db.create_message(
        user_id=user_id,
        conversation_id=conversation_id,
        content=Body,
        role=MessageRole.USER,
        message_id=message_id,
        metadata=metadata,
    )

    try:
        # Create queue message
        queue_message = WhatsAppQueueMessage(
            from_number=From,
            body=Body,
            profile_name=ProfileName,
            wa_id=WaId,
            sms_message_sid=SmsMessageSid,
            user_id=user_id,
            conversation_id=conversation_id,
            message_id=message_id,
            request_url=request_url,
            media_count=len(media_items),
            media_items=media_items,
            unsupported_media=unsupported_media,
        )

        # Send to Azure Service Bus
        producer = WhatsAppMessageProducer()
        await producer.send_message(queue_message)

        logger.info(f"WhatsApp message queued - Thread: {conversation_id}")

        # Create immediate TwiML response with friendly message and emojis
        twiml_response = MessagingResponse()
        response_message = "✨ Thanks for your message! 🙏 We're processing it now and will get back to you shortly."
        
        if new_conversation_created:
            response_message += "\n\n🆕 A new conversation has been created as requested."
            
        if unsupported_media:
            response_message += "\n\n📝 Note: We currently only support image attachments."
        # Use format_message to include a reference to the user's message
        formatted_response = format_message(Body, response_message)
        twiml_response.message(formatted_response)

        return Response(content=str(twiml_response), media_type="application/xml")

    except Exception as e:
        logger.error(f"Error queuing WhatsApp message: {str(e)}")

        # Create error message TwiML response
        error_message = "We're experiencing technical difficulties processing your message. Our team has been notified."
        formatted_error = format_message(Body, error_message, is_error=True)

        twiml_response = MessagingResponse()
        twiml_response.message(formatted_error)

        logger.info(f"WhatsApp error notification sent - Thread: {conversation_id}")

        return Response(content=str(twiml_response), status_code=500, media_type="application/xml")
