"""Worker process for consuming WhatsApp messages from Azure Service Bus."""

import asyncio
import base64
import httpx
import sys
from typing import Dict, List
from uuid import uuid4, UUID

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from twilio.rest import Client

from agents.graph import create_graph
from api.config import settings as api_settings
from database.manager import DatabaseManager
from database.conversation_store.models.message import MessageRole
from messaging.consumer import WhatsAppMessageConsumer
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger
from utils.text import format_message
from langgraph.checkpoint.memory import MemorySaver
from database.conversation_store.conversation_manager import ConversationManager
from database.conversation_store.exceptions import ConversationNotFoundError


async def convert_message_to_langchain_format(msg) -> HumanMessage | AIMessage:
    """Convert a single message to LangChain format."""
    if msg.role == MessageRole.USER:
        # Check if message has image media
        if msg.metadata and msg.metadata.get("media_count", 0) > 0:
            # Create multimodal content
            content = []

            # Add text content if present
            if msg.content:
                content.append({"type": "text", "text": msg.content})

            # Add image URLs
            for media_item in msg.metadata.get("media_items", []):
                content.append({"type": "image_url", "image_url": {"url": media_item["url"]}})

            return HumanMessage(content=content)
        else:
            # Regular text message
            return HumanMessage(content=msg.content)
    elif msg.role == MessageRole.ASSISTANT:
        return AIMessage(content=msg.content)
    else:
        # Handle unexpected message roles
        logger.warning(f"Unexpected message role: {msg.role}")
        return None


async def process_image_urls_for_last_message(langchain_messages: List[HumanMessage | AIMessage], twilio_client: Client) -> None:
    """
    Process image URLs in the last message, converting them to base64.
    
    This function modifies the langchain_messages list in-place.
    Only successfully converted images are included in the final message.
    Uses the provided Twilio client to fetch images as they require authentication.
    
    Args:
        langchain_messages: List of LangChain messages to process
        twilio_client: Initialized Twilio client to use for fetching media
    """
    # Check if we have messages and the last one is a HumanMessage with multimodal content
    if not langchain_messages or not isinstance(langchain_messages[-1], HumanMessage):
        return
    
    last_message = langchain_messages[-1]
    if not isinstance(last_message.content, list):
        return
    
    # Create a new content list that will only include text and successfully converted images
    new_content = []
    conversion_failures = 0
    conversion_successes = 0
    
    # Process each content item in the last message
    for content_item in last_message.content:
        # Always keep text content
        if content_item["type"] == "text":
            new_content.append(content_item)
            continue
            
        # Process image content
        if content_item["type"] == "image_url" and "image_url" in content_item:
            image_url = content_item["image_url"]["url"]
            try:
                # Use Twilio client to fetch image (handles authentication)
                media_response = twilio_client.request(method='GET', uri=image_url)
                
                # Get content type from response headers if available
                content_type = "image/jpeg"  # Default
                if hasattr(media_response, 'headers') and 'Content-Type' in media_response.headers:
                    content_type = media_response.headers['Content-Type']
                
                # Convert to base64
                image_data = base64.b64encode(media_response.content).decode('utf-8')
                
                # Add successfully converted image to new content
                new_content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{content_type};base64,{image_data}"}
                })
                
                conversion_successes += 1
                logger.debug(f"Successfully converted image to base64: {image_url[:50]}...")
            except Exception as e:
                logger.error(f"Error converting image to base64 with Twilio client: {str(e)} - {image_url[:50]}...")
                conversion_failures += 1
    
    # Replace the original content with the new content
    last_message.content = new_content
    
    # Log summary of conversion results
    if conversion_failures > 0 or conversion_successes > 0:
        logger.info(f"Image conversion summary: {conversion_successes} successful, {conversion_failures} failed")


async def get_conversation_history(
    conversation_id: UUID, 
    user_id: str, 
    conversation_manager: ConversationManager,
    twilio_client: Client
) -> List[HumanMessage | AIMessage]:
    """
    Get conversation history as LangChain messages.
    
    For the last message in the conversation, any image URLs are converted to base64
    to ensure they can be processed by OpenAI without authentication issues.
    
    Args:
        conversation_id: UUID of the conversation
        user_id: ID of the user
        conversation_manager: Manager for accessing conversation data
        twilio_client: Initialized Twilio client to use for fetching media
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
        
        # Process image URLs in the last message
        await process_image_urls_for_last_message(langchain_messages, twilio_client)

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

    # Initialize Twilio client - will be reused throughout the process
    twilio_client = Client(api_settings.twilio_account_sid, api_settings.twilio_auth_token)

    try:
        # Get conversation history - our updated function will handle multimodal messages
        messages = await get_conversation_history(
            message.conversation_id, 
            message.user_id, 
            conversation_db,
            twilio_client
        )

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
