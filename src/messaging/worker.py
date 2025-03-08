"""Worker process for consuming WhatsApp messages from Azure Service Bus."""

import asyncio
import sys
from typing import Dict, List
from uuid import uuid4, UUID

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from twilio.rest import Client

from agents.graph import create_graph
from api.config import settings as api_settings
from api.dependencies import get_conversation_db, get_db
from conversation_store.models.message import MessageRole
from messaging.consumer import WhatsAppMessageConsumer
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger
from langgraph.checkpoint.memory import MemorySaver
from conversation_store.conversation_manager import ConversationManager
from conversation_store.exceptions import ConversationNotFoundError


async def get_conversation_history(conversation_id: UUID, user_id: str, conversation_db: ConversationManager) -> List[HumanMessage | AIMessage]:
    """Get conversation history as LangChain messages."""
    try:
        # Get messages from the conversation
        messages = await conversation_db.list_messages(user_id, conversation_id)

        # Convert to LangChain messages
        langchain_messages = []
        for msg in messages:
            if msg.role == MessageRole.USER:
                langchain_messages.append(HumanMessage(content=msg.content))
            elif msg.role == MessageRole.ASSISTANT:
                langchain_messages.append(AIMessage(content=msg.content))

        return langchain_messages
    except ConversationNotFoundError:
        # If conversation not found, return empty list
        return []
    except Exception as e:
        logger.error(f"Error getting conversation history: {str(e)}")
        return []


async def process_whatsapp_message(message: WhatsAppQueueMessage) -> Dict[str, str]:
    """Process a WhatsApp message from the queue."""
    logger.info(f"Processing WhatsApp message from queue: {message.sms_message_sid}")

    # Get dependencies
    db = get_db()
    conversation_db = get_conversation_db()

    try:
        # Get conversation history
        messages = await get_conversation_history(message.conversation_id, message.user_id, conversation_db)

        # Get the graph
        graph = create_graph(db)
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

        # Initialize Twilio client and send response
        twilio_client = Client(api_settings.twilio_account_sid, api_settings.twilio_auth_token)
        twilio_message = twilio_client.messages.create(
            body=response_content,
            from_=api_settings.twilio_phone_number,
            to=message.from_number
        )
        
        logger.info(f"Message processing completed and sent via WhatsApp - Thread: {message.conversation_id}, SID: {twilio_message.sid}")

        return {"status": "success", "message_sid": twilio_message.sid}

    except Exception as e:
        logger.error(f"Error processing WhatsApp message: {str(e)}", exc_info=True)
        
        # Create a more informative error message
        error_message = "We're experiencing technical difficulties processing your request. Our team has been notified and is working on it."
        
        # Send error message via WhatsApp without storing in conversation
        try:
            twilio_client = Client(api_settings.twilio_account_sid, api_settings.twilio_auth_token)
            twilio_message = twilio_client.messages.create(
                body=error_message,
                from_=api_settings.twilio_phone_number,
                to=message.from_number
            )
            logger.info(f"Error notification sent via WhatsApp - Thread: {message.conversation_id}, SID: {twilio_message.sid}")
            return {"status": "error", "message_sid": twilio_message.sid}
        except Exception as twilio_e:
            logger.error(f"Failed to send WhatsApp error notification: {str(twilio_e)}")
            return {"status": "error", "message": "Failed to send WhatsApp notification"}


async def run_worker():
    """Run the worker process to consume messages from the queue."""
    logger.info("Starting WhatsApp message worker")

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
