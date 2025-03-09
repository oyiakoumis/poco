"""Chat router for handling message processing."""

from typing import Optional
from uuid import uuid4

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
from utils.media_storage import upload_to_blob_storage
from utils.text import (
    Command,
    build_notification_string,
    extract_message_after_command,
    format_message,
    is_command,
)

router = APIRouter(prefix="/chat", tags=["chat"])


def validate_twilio_request(request_data: dict, signature: str, url: str) -> bool:
    """Validate that the request is coming from Twilio."""
    validator = RequestValidator(settings.twilio_auth_token)
    return validator.validate(url, request_data, signature)


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

        # Create immediate TwiML response
        twiml_response = MessagingResponse()
        response_message = "Got it! Give me just a second..."

        # Build concise notification string if needed
        notification_str = build_notification_string({
            "new_conversation": new_conversation_created,
            "unsupported_media": unsupported_media
        })

        if notification_str:
            response_message += f"\n\n{notification_str}"
            
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
