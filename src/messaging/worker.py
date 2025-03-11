"""Worker process for consuming WhatsApp messages from Azure Service Bus."""

import asyncio
import sys
from typing import Dict, List
from uuid import UUID, uuid4

from langchain_core.messages import AIMessage, HumanMessage, AnyMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from twilio.rest import Client

from agents.graph import create_graph
from config import settings as api_settings
from database.conversation_store.conversation_manager import ConversationManager
from database.conversation_store.exceptions import (
    InvalidConversationError,
)
from database.conversation_store.models.message import Message, MessageRole
from database.manager import DatabaseManager
from messaging.consumer import WhatsAppMessageConsumer
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger
from utils.text import format_message


async def get_conversation_history(conversation_id: UUID, user_id: str, conversation_manager: ConversationManager) -> List[HumanMessage | AIMessage]:
    """Get conversation history as LangChain messages."""
    # Get messages from the conversation
    messages = await conversation_manager.list_messages(user_id, conversation_id)
    if not messages:
        logger.error(f"No messages found for conversation {conversation_id}")
        raise InvalidConversationError(f"No messages found for conversation {conversation_id}")

    await asyncio.gather(*[message.get_image_urls() for message in messages])

    return [message.message for message in messages]


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

        response: AnyMessage = result["messages"][-1]

        # Store the assistant's response
        assistant_message_id = uuid4()
        await conversation_db.create_message(
            user_id=message.user_id,
            conversation_id=message.conversation_id,
            message=response,
            role=MessageRole.ASSISTANT,
            message_id=assistant_message_id,
        )

        # Format the response with the user's message
        formatted_response = format_message(message.body, response.content)

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
