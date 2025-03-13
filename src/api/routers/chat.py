"""Chat router for handling message processing."""

import asyncio
import json
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, Form, Header, Request, Response
from langchain_core.messages import AnyMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from twilio.request_validator import RequestValidator
from twilio.rest import Client
from twilio.twiml.messaging_response import MessagingResponse

from agents.assistant import ASSISTANT_SYSTEM_MESSAGE
from agents.graph import create_graph
from agents.tools.tool_operation_tracker import ToolOperationTracker
from api.models import MediaItem
from settings import settings
from database.conversation_store.models.message import Message, MessageRole
from database.manager import DatabaseManager
from utils.azure_blob_lock import AzureBlobLockManager
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

    # Check if the message body is empty
    if not Body or Body.strip() == "":
        logger.info(f"Empty message received from {From}, SID: {SmsMessageSid}")

        # Create error message TwiML response
        error_message = "Message's body cannot be empty."
        formatted_error = format_message("", error_message, is_error=True)

        twiml_response = MessagingResponse()
        twiml_response.message(formatted_error)

        return Response(content=str(twiml_response), media_type="application/xml")

    # Validate the request is coming from Twilio
    if False and settings.twilio_auth_token and x_twilio_signature:  # TODO: Enable Twilio validation later
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
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    user_id = From  # Use the WhatsApp number as the user ID
    conversation_id = None
    lock = None

    try:
        # Get database managers
        database_manager = DatabaseManager()
        conversation_db = await database_manager.setup_conversation_manager()
        dataset_db = await database_manager.setup_dataset_manager()

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
            new_conversation_created = True

        # Initialize Azure Blob lock manager and try to acquire a lock
        blob_lock_manager = AzureBlobLockManager()
        lock = blob_lock_manager.acquire_lock(conversation_id)

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

        # Send immediate acknowledgment via Twilio
        response_message = "Got it! Give me just a second..."

        # Build concise notification string if needed
        notification_str = build_notification_string({"new_conversation": new_conversation_created, "unsupported_media": unsupported_media})

        if notification_str:
            response_message += f"\n\n`{notification_str}`"

        # Format the response with the user's message
        formatted_response = format_message(Body, response_message)

        # If we couldn't acquire the lock, send a busy message and return
        if not lock:
            logger.info(f"Conversation {conversation_id} is already being processed, skipping")

            # Send message to user that their message is being ignored due to ongoing processing
            error_message = "Assistant is currently busy processing another message. This message will be ignored."
            formatted_error = format_message(Body, error_message, is_error=True)

            # Return API response
            return Response(content=formatted_error, media_type="application/xml")

        # Send the acknowledgment message
        try:
            twilio_client = Client(settings.twilio_account_sid, settings.twilio_auth_token)
            ack_message = twilio_client.messages.create(body=formatted_response, from_=settings.twilio_phone_number, to=From)
            logger.info(f"Acknowledgment sent via WhatsApp - Thread: {conversation_id}, SID: {ack_message.sid}")
        except Exception as twilio_e:
            logger.error(f"Failed to send WhatsApp acknowledgment: {str(twilio_e)}")
            # Continue processing even if acknowledgment fails

        input_messages = []
        if new_conversation_created:
            input_messages += [
                Message(user_id=user_id, conversation_id=conversation_id, role=MessageRole.SYSTEM, message=SystemMessage(content=ASSISTANT_SYSTEM_MESSAGE))
            ]

        input_messages += [
            Message(
                user_id=user_id,
                conversation_id=conversation_id,
                role=MessageRole.HUMAN,
                message=HumanMessage(content=Body),
                metadata={
                    "whatsapp_id": WaId,
                    "sms_message_sid": SmsMessageSid,
                    "media_count": len(media_items),
                    "media_items": media_items,
                    "unsupported_media": unsupported_media,
                },
            )
        ]

        # Get conversation history
        conversation_history = []
        if not new_conversation_created:
            conversation_history = await conversation_db.list_messages(user_id, conversation_id)

        # Get IDs of existing messages
        existing_message_ids = {msg.id for msg in conversation_history}

        # Combine input messages with existing conversation history
        conversation_history += input_messages

        # Get image URLs for all messages
        await asyncio.gather(*[message.get_image_urls() for message in conversation_history])

        # Get the graph
        graph = create_graph(dataset_db)
        graph = graph.compile(checkpointer=MemorySaver())

        # Configuration for the graph
        config = RunnableConfig(
            configurable={
                "thread_id": str(conversation_id),
                "user_id": user_id,
                "time_zone": "UTC",
                "first_day_of_the_week": 0,
            },
            recursion_limit=25,
        )

        # Process the message through the graph
        result = await graph.ainvoke({"messages": [message.message for message in conversation_history]}, config)
        logger.info(f"Graph processing completed - Thread: {conversation_id}")

        # Identify new messages by comparing IDs
        output_messages: List[AnyMessage] = [msg for msg in result["messages"] if msg.id not in existing_message_ids]

        # Messages to store in the database
        messages_to_store = input_messages + [Message(user_id=user_id, conversation_id=conversation_id, message=msg) for msg in output_messages]

        # Store all new messages
        await conversation_db.create_messages(user_id, messages_to_store)

        # Get the last message for the WhatsApp response
        response: AnyMessage = result["messages"][-1]

        # Track tool operations and generate summary
        tracker = ToolOperationTracker()

        # Filter for tool messages with successful operations
        tool_messages = [
            msg
            for msg in output_messages
            if isinstance(msg, ToolMessage)
            and hasattr(msg, "name")
            and msg.name in tracker.get_supported_tools()
            and hasattr(msg, "status")
            and msg.status == "success"
        ]

        # Track each tool message
        for msg in tool_messages:
            tracker.track_tool_message(msg.name, msg.content)

        # Generate summary
        tool_summary = tracker.build_tool_summary_string()

        # Include summary in response if not empty
        response_content = response.content
        if tool_summary:
            response_content = f"{response_content}\n\n`{tool_summary}`"

        # Format the response with the user's message
        formatted_response = format_message(Body, response_content)

        # Return API response
        return Response(content=formatted_response, media_type="application/xml")

    except Exception as e:
        logger.error(f"Error processing WhatsApp message: {str(e)}", exc_info=True)

        # Create error message TwiML response
        error_message = "We're experiencing technical difficulties processing your message. Our team has been notified."
        formatted_error = format_message(Body, error_message, is_error=True)

        twiml_response = MessagingResponse()
        twiml_response.message(formatted_error)

        return Response(content=str(twiml_response), status_code=500, media_type="application/xml")
    finally:
        # Release the lock if we acquired it
        if lock:
            blob_lock_manager.release_lock(lock)
            logger.info(f"Released lock for conversation {conversation_id}")
