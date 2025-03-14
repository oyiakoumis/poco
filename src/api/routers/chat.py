"""Chat router for handling message processing."""

from typing import Optional

from fastapi import APIRouter, Depends, Form, Header, Request, Response, status
from fastapi.exceptions import HTTPException

from api.routers.dependencies import get_blob_lock_manager, get_database_manager, validate_twilio_signature
from api.services.conversation_service import ConversationService
from api.services.media_service import MediaService
from api.services.message_processor import MessageProcessor
from api.utils.response_formatter import ResponseFormatter
from database.manager import DatabaseManager
from settings import settings
from utils.azure_blob_lock import AzureBlobLockManager
from utils.logging import logger

router = APIRouter(prefix="/chat", tags=["chat"])


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
    db_manager: DatabaseManager = Depends(get_database_manager),
    lock_manager: AzureBlobLockManager = Depends(get_blob_lock_manager),
    twilio_valid: bool = Depends(validate_twilio_signature),
) -> Response:
    """Process incoming WhatsApp messages from Twilio.

    This endpoint handles incoming WhatsApp messages, processes them through
    the LangGraph, and returns a response to the user.
    """
    logger.info(f"WhatsApp message received from {From}, SID: {SmsMessageSid}")

    # Initialize services
    response_formatter = ResponseFormatter()

    # Check if the message body is empty
    if not Body or Body.strip() == "":
        logger.info(f"Empty message received from {From}, SID: {SmsMessageSid}")
        response_formatter.send_error(From, "", "Message's body cannot be empty.")
        return Response(status_code=status.HTTP_400_BAD_REQUEST)

    # Validate the request is coming from Twilio (currently disabled with TODO)
    if not twilio_valid:
        logger.warning(f"Invalid Twilio signature: {x_twilio_signature}")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    user_id = From  # Use the WhatsApp number as the user ID
    conversation_id = None
    lock = None

    try:
        # Get database managers
        conversation_db = await db_manager.setup_conversation_manager()
        dataset_db = await db_manager.setup_dataset_manager()

        # Initialize services
        conversation_service = ConversationService(conversation_db, lock_manager)
        media_service = MediaService()
        message_processor = MessageProcessor(dataset_db)

        # Get or create conversation
        conversation_id, new_conversation_created, processed_body = await conversation_service.get_or_create_conversation(user_id, Body)

        # Initialize Azure Blob lock manager and try to acquire a lock
        lock = await conversation_service.acquire_conversation_lock(conversation_id)

        # If we couldn't acquire the lock, send a busy message and return
        if not lock:
            logger.info(f"Conversation {conversation_id} is already being processed, skipping")
            response_formatter.send_error(
                From, Body, "I am a little busy right now processing your current message. Please send your next message right after."
            )
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

        # Process media
        num_media = int(NumMedia) if NumMedia is not None else 0

        # Check for unsupported media
        unsupported_media = await media_service.has_unsupported_media(request, num_media)

        # Send immediate acknowledgment
        response_formatter.send_acknowledgment(From, Body, new_conversation=new_conversation_created, unsupported_media=unsupported_media)

        # Process media
        media_items = await media_service.process_media(request, num_media)

        # Prepare metadata
        metadata = {
            "whatsapp_id": WaId,
            "sms_message_sid": SmsMessageSid,
            "media_count": len(media_items),
            "media_items": media_items,
            "unsupported_media": unsupported_media,
        }

        # Prepare new messages
        new_messages = await conversation_service.prepare_messages(user_id, conversation_id, processed_body, new_conversation_created, metadata)

        # Get conversation history
        conversation_history = await conversation_service.get_conversation_history(user_id, conversation_id, new_conversation_created)

        # Process image URLs for all messages
        await conversation_service.process_image_urls(conversation_history + new_messages)

        # Process messages through the graph
        output_messages, response, tool_summary = await message_processor.process_messages(conversation_history, new_messages, user_id, conversation_id)

        # Include summary in response if not empty
        response_content = response.content + (f"\n\n`{tool_summary}`" if tool_summary else "")

        # Store all new messages
        await conversation_service.store_messages(new_messages + output_messages)

        # Send the response
        response_formatter.send_response(From, Body, response_content)

        return Response(status_code=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Error processing WhatsApp message: {str(e)}", exc_info=True)

        # Send error message
        response_formatter = ResponseFormatter()
        response_formatter.send_error(From, Body, "We're experiencing technical difficulties processing your message. Our team has been notified.")

        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    finally:
        # Release the lock if we acquired it
        if lock and conversation_id:
            conversation_service = ConversationService(await db_manager.setup_conversation_manager(), lock_manager)
            conversation_service.release_conversation_lock(lock)
            logger.info(f"Released lock for conversation {conversation_id}")
