"""Chat router for handling message processing."""

from typing import List

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver

from agents.graph import create_graph
from api.dependencies import get_conversation_db, get_db
from api.models import ChatRequest, ChatResponse
from conversation_store.conversation_manager import ConversationManager
from conversation_store.exceptions import ConversationNotFoundError
from conversation_store.models.message import MessageRole
from document_store.dataset_manager import DatasetManager
from utils.logging import logger

router = APIRouter(prefix="/chat", tags=["chat"])


async def get_conversation_history(conversation_id: ObjectId, user_id: str, conversation_db: ConversationManager) -> List[HumanMessage | AIMessage]:
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


@router.post("/", response_model=ChatResponse)
async def process_message(
    request: ChatRequest, db: DatasetManager = Depends(get_db), conversation_db: ConversationManager = Depends(get_conversation_db)
) -> ChatResponse:
    """Process a chat message."""
    logger.info(f"Starting message processing - Thread: {request.thread_id}, User: {request.user_id}")

    try:
        # Check if conversation exists
        conversation_exists = await conversation_db.conversation_exists(request.user_id, request.conversation_id)

        if not conversation_exists:
            # If conversation doesn't exist, raise a 404 error
            logger.error(f"Conversation {request.conversation_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conversation {request.conversation_id} not found. Create a conversation first."
            )
        
        # Conversation exists, store user message
        await conversation_db.create_message(
            user_id=request.user_id,
            conversation_id=request.conversation_id,
            content=request.message,
            role=MessageRole.USER,
        )

        # Get conversation history (including the message we just added)
        messages = await get_conversation_history(request.conversation_id, request.user_id, conversation_db)

        # Get the graph
        graph = create_graph(db)
        graph = graph.compile(checkpointer=MemorySaver())

        # Configuration for the graph
        config = RunnableConfig(
            configurable={
                "thread_id": request.thread_id,
                "user_id": request.user_id,
                "time_zone": request.time_zone,
                "first_day_of_the_week": request.first_day_of_week,
            },
            recursion_limit=25,
        )

        # Process message and get final assistant response
        assistant_message = None

        # Process all events from the graph
        async for event in graph.astream({"messages": messages}, config, stream_mode="updates"):
            # Log stream event
            logger.debug(f"Stream event received.")

            # Get only assistant message
            if isinstance(event, dict) and "assistant" in event:
                assistant_data = event["assistant"]
                if isinstance(assistant_data, dict) and "messages" in assistant_data:
                    event_messages = assistant_data["messages"]
                    if event_messages and len(event_messages) > 0 and isinstance(event_messages[-1], AIMessage):
                        assistant_message = event_messages[-1].content
                        logger.info("Assistant response generated")

        if not assistant_message:
            logger.warning("No assistant message generated, using fallback response")
            assistant_message = "I apologize, but I couldn't process your request."

        # Store assistant response in conversation
        await conversation_db.create_message(
            user_id=request.user_id,
            conversation_id=request.conversation_id,
            content=assistant_message,
            role=MessageRole.ASSISTANT,
        )

        logger.info("Message processing completed")
        return ChatResponse(message=assistant_message, conversation_id=request.conversation_id)

    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process message",
        )
