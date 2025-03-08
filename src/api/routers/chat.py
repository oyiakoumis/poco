"""Chat router for handling message processing."""

from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, Header, Response
from twilio.request_validator import RequestValidator
from twilio.twiml.messaging_response import MessagingResponse

from api.config import settings
from api.dependencies import get_conversation_db, get_db
from conversation_store.conversation_manager import ConversationManager
from conversation_store.models.message import MessageRole
from document_store.dataset_manager import DatasetManager
from messaging.models import WhatsAppQueueMessage
from messaging.producer import WhatsAppMessageProducer
from utils.logging import logger

router = APIRouter(prefix="/chat", tags=["chat"])


def validate_twilio_request(request_data: dict, signature: str, url: str) -> bool:
    """Validate that the request is coming from Twilio."""
    validator = RequestValidator(settings.twilio_auth_token)
    return validator.validate(url, request_data, signature)


@router.post("/whatsapp", response_class=Response)
async def process_whatsapp_message(
    From: str = Form(...),
    Body: str = Form(...),
    ProfileName: Optional[str] = Form(None),
    WaId: str = Form(...),
    SmsMessageSid: str = Form(...),
    db: DatasetManager = Depends(get_db),
    conversation_db: ConversationManager = Depends(get_conversation_db),
    x_twilio_signature: str = Header(None),
    request_url: str = Header(None, alias="X-Original-URL"),
) -> Response:
    """
    Process incoming WhatsApp messages from Twilio.

    This endpoint:
    1. Validates the request is coming from Twilio
    2. Extracts the sender's WhatsApp number and message
    3. Finds or creates a conversation for this user
    4. Stores the user message in the database
    5. Sends the message to Azure Service Bus for asynchronous processing
    6. Returns a quick acknowledgment to Twilio
    """
    logger.info(f"WhatsApp message received from {From}, SID: {SmsMessageSid}")

    # Validate the request is coming from Twilio
    if settings.twilio_auth_token and x_twilio_signature:
        # Create a dictionary of form fields for Twilio validation
        request_data = {"From": From, "Body": Body, "ProfileName": ProfileName or "", "WaId": WaId, "SmsMessageSid": SmsMessageSid}
        # If request_url is not provided, construct it from the settings
        url = request_url or f"https://{settings.api_url}:{settings.port}/chat/whatsapp"

        if not validate_twilio_request(request_data, x_twilio_signature, url):
            logger.warning(f"Invalid Twilio signature: {x_twilio_signature}")
            # raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    # Use the WhatsApp number as the user ID
    user_id = From

    # Find existing conversations for this user
    conversations = await conversation_db.list_conversations(user_id)

    # Find a conversation with WhatsApp metadata or create a new one
    conversation_id = None
    for conv in conversations:
        # Check if this is a WhatsApp conversation
        if await conversation_db.conversation_exists(user_id, conv.id):
            conversation_id = conv.id
            break

    # If no conversation found, create a new one
    if not conversation_id:
        conversation_id = uuid4()
        # Create a title based on the user's profile name or number
        title = f"WhatsApp: {ProfileName or From}"
        logger.info(f"Creating new WhatsApp conversation: {conversation_id}, Title: {title}")
        await conversation_db.create_conversation(user_id, title, conversation_id)

    # Create a message ID for the incoming message
    message_id = uuid4()

    # WhatsApp-specific metadata
    metadata = {"whatsapp_id": WaId, "sms_message_sid": SmsMessageSid}

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
            metadata=metadata,
        )

        # Send to Azure Service Bus
        producer = WhatsAppMessageProducer()
        await producer.send_message(queue_message)

        logger.info(f"WhatsApp message queued - Thread: {conversation_id}")

        # Create immediate TwiML response
        twiml_response = MessagingResponse()
        twiml_response.message("Your message is being processed. We'll respond shortly.")

        return Response(content=str(twiml_response), media_type="application/xml")

    except Exception as e:
        logger.error(f"Error queuing WhatsApp message: {str(e)}")

        # Fallback to synchronous processing if queue fails
        logger.info(f"Falling back to synchronous processing - Thread: {conversation_id}")

        response_content = await process_message_core(
            message=Body,
            user_id=user_id,
            conversation_id=conversation_id,
            message_id=message_id,
            metadata=metadata,
            db=db,
            conversation_db=conversation_db,
        )

        # Create TwiML response
        twiml_response = MessagingResponse()
        twiml_response.message(response_content)

        logger.info(f"WhatsApp request completed (synchronous fallback) - Thread: {conversation_id}")

        return Response(content=str(twiml_response), media_type="application/xml")
