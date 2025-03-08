"""Worker process for consuming WhatsApp messages from Azure Service Bus."""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from uuid import uuid4, UUID

from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from twilio.rest import Client

from agents.graph import create_graph
from config import settings as api_settings
from database.manager import DatabaseManager
from database.conversation_store.models.message import Message, MessageRole
from config import settings
from messaging.consumer import WhatsAppMessageConsumer
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger
from utils.text import format_message
from langgraph.checkpoint.memory import MemorySaver
from database.conversation_store.conversation_manager import ConversationManager
from database.conversation_store.exceptions import ConversationNotFoundError


async def generate_blob_presigned_url(blob_name: str) -> str:
    """
    Generate a presigned URL for a blob in Azure Blob Storage.

    Args:
        blob_name: Name of the blob in Azure Blob Storage

    Returns:
        Presigned URL with 24-hour expiry
    """
    async with BlobServiceClient.from_connection_string(settings.azure_storage_connection_string) as blob_service_client:
        # Extract account name from the blob service client
        account_name = blob_service_client.account_name
        container_name = settings.azure_blob_container_name
        account_key = settings.azure_storage_account_key

        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=24),
        )

        # Create the presigned URL
        presigned_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        return presigned_url


async def convert_message_to_langchain_format(msg: Message) -> HumanMessage | AIMessage:
    """Convert a single message to LangChain format."""
    if msg.role == MessageRole.USER:
        # Check if message has image media
        if msg.metadata and msg.metadata.get("media_count", 0) > 0:
            # Create multimodal content
            content = []

            # Add text content if present
            if msg.content:
                content.append({"type": "text", "text": msg.content})

            # Add images with presigned URLs
            url_generation_failures = 0
            url_generation_successes = 0

            for media_item in msg.metadata.get("media_items", []):
                # Get the blob name from the media item
                blob_name = media_item["blob_name"]
                try:
                    # Generate presigned URL for the blob
                    presigned_url = await generate_blob_presigned_url(blob_name)

                    # Add image with presigned URL to content
                    content.append({"type": "image_url", "image_url": {"url": presigned_url}})

                    url_generation_successes += 1
                    logger.debug(f"Successfully generated presigned URL for image: {blob_name}")
                except Exception as e:
                    logger.error(f"Error generating presigned URL for blob {blob_name}: {str(e)}")
                    url_generation_failures += 1

            # Log summary of URL generation results
            if url_generation_failures > 0 or url_generation_successes > 0:
                logger.info(f"Presigned URL generation summary: {url_generation_successes} successful, {url_generation_failures} failed")

            return HumanMessage(content=content)
        else:
            # Regular text message - content is just a string
            return HumanMessage(content=msg.content)
    elif msg.role == MessageRole.ASSISTANT:
        return AIMessage(content=msg.content)
    else:
        # Handle unexpected message roles
        logger.warning(f"Unexpected message role: {msg.role}")
        return None


async def get_conversation_history(conversation_id: UUID, user_id: str, conversation_manager: ConversationManager) -> List[HumanMessage | AIMessage]:
    """
    Get conversation history as LangChain messages.

    Args:
        conversation_id: UUID of the conversation
        user_id: ID of the user
        conversation_manager: Manager for accessing conversation data
        twilio_client: Initialized Twilio client
    """
    try:
        # Get messages from the conversation
        messages = await conversation_manager.list_messages(user_id, conversation_id)
        if not messages:
            logger.info(f"No messages found for conversation {conversation_id}")
            return []

        # Convert to LangChain messages
        langchain_messages = []
        for msg in messages:
            langchain_msg = await convert_message_to_langchain_format(msg)
            if langchain_msg:
                langchain_messages.append(langchain_msg)

        return langchain_messages
    except ConversationNotFoundError:
        logger.info(f"Conversation not found: {conversation_id}")
        return []
    except Exception as e:
        logger.error(f"Error getting conversation history for {conversation_id}: {str(e)}", exc_info=True)
        return []


async def process_whatsapp_message(message: WhatsAppQueueMessage) -> Dict[str, str]:
    """Process a WhatsApp message from the queue."""
    logger.info(f"Processing WhatsApp message from queue: {message.sms_message_sid}")

    # Get database managers
    database_manager = DatabaseManager()
    dataset_db = await database_manager.setup_dataset_manager()
    conversation_db = await database_manager.setup_conversation_manager()

    # Initialize Twilio client
    twilio_client = Client(api_settings.twilio_account_sid, api_settings.twilio_auth_token)

    try:
        # Get conversation history - our updated function will handle multimodal messages
        messages = await get_conversation_history(message.conversation_id, message.user_id, conversation_db)

        # Get the graph
        graph = create_graph(dataset_db)
        graph = graph.compile(checkpointer=MemorySaver())

        # Configuration for the graph
        config = RunnableConfig(
            configurable={
                "thread_id": str(message.conversation_id),
                "user_id": message.user_id,
                "time_zone": "UTC",
                "first_day_of_the_week": 0,
            },
            recursion_limit=25,
        )

        # Process the message through the graph
        result = await graph.ainvoke({"messages": messages}, config)
        logger.info(f"Graph processing completed - Thread: {message.conversation_id}")

        # Extract the assistant's response from the result
        if result and "messages" in result and result["messages"] and isinstance(result["messages"][-1], AIMessage):
            response_content = result["messages"][-1].content
        else:
            logger.warning(f"No valid response generated from graph - Thread: {message.conversation_id}")
            response_content = "I apologize, but I couldn't process your request."

        # Store the assistant's response
        assistant_message_id = uuid4()
        await conversation_db.create_message(
            user_id=message.user_id,
            conversation_id=message.conversation_id,
            content=response_content,
            role=MessageRole.ASSISTANT,
            message_id=assistant_message_id,
        )

        # Format the response with the user's message
        formatted_response = format_message(message.body, response_content)

        # Send response using the already initialized Twilio client
        twilio_message = twilio_client.messages.create(body=formatted_response, from_=api_settings.twilio_phone_number, to=message.from_number)

        logger.info(f"Message processing completed and sent via WhatsApp - Thread: {message.conversation_id}, SID: {twilio_message.sid}")

        return {"status": "success", "message_sid": twilio_message.sid}

    except Exception as e:
        logger.error(f"Error processing WhatsApp message: {str(e)}", exc_info=True)

        # Create a more informative error message
        error_message = "We're experiencing technical difficulties processing your request. Our team has been notified and is working on it."
        formatted_error = format_message(message.body, error_message, is_error=True)

        # Send error message via WhatsApp without storing in conversation
        try:
            # Reuse the already initialized Twilio client
            twilio_message = twilio_client.messages.create(body=formatted_error, from_=api_settings.twilio_phone_number, to=message.from_number)
            logger.info(f"Error notification sent via WhatsApp - Thread: {message.conversation_id}, SID: {twilio_message.sid}")
            return {"status": "error", "message_sid": twilio_message.sid}
        except Exception as twilio_e:
            logger.error(f"Failed to send WhatsApp error notification: {str(twilio_e)}")
            return {"status": "error", "message": "Failed to send WhatsApp notification"}


async def run_worker():
    """Run the worker process to consume messages from the queue."""
    consumer = WhatsAppMessageConsumer(process_whatsapp_message)
    try:
        while True:
            await consumer.process_messages()
            await asyncio.sleep(1)  # Prevent CPU spinning
    except KeyboardInterrupt:
        logger.info("Worker process stopped")
    except Exception as e:
        logger.error(f"Worker process error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run_worker())
