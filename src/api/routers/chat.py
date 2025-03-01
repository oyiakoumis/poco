"""Chat router for handling message processing."""

from typing import List
from uuid import UUID

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


@router.post("/", response_model=ChatResponse)
async def process_message(
    request: ChatRequest, db: DatasetManager = Depends(get_db), conversation_db: ConversationManager = Depends(get_conversation_db)
) -> ChatResponse:
    """
    Process a chat message and return the response.

    This endpoint:
    1. Validates the conversation exists
    2. Stores the user message
    3. Retrieves conversation history
    4. Processes the message through the graph
    5. Stores the response in the database
    6. Returns the complete response to the client
    """
    logger.info(f"Starting message processing - Thread: {request.thread_id}, User: {request.user_id}")

    try:
        # Check if conversation exists
        conversation_exists = await conversation_db.conversation_exists(request.user_id, request.conversation_id)

        if not conversation_exists:
            logger.error(f"Conversation {request.conversation_id} not found")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Conversation {request.conversation_id} not found. Create a conversation first.")

        # Store user message
        await conversation_db.create_message(
            user_id=request.user_id,
            conversation_id=request.conversation_id,
            content=request.message,
            role=MessageRole.USER,
        )

        # Get conversation history
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

        # Process the message through the graph
        result = await graph.ainvoke({"messages": messages}, config)

        # Extract the assistant's response from the result
        if result and "messages" in result and result["messages"] and isinstance(result["messages"][-1], AIMessage):
            response_content = result["messages"][-1].content
        else:
            response_content = "I apologize, but I couldn't process your request."

        # Store the assistant's response
        await conversation_db.create_message(
            user_id=request.user_id,
            conversation_id=request.conversation_id,
            content=response_content,
            role=MessageRole.ASSISTANT,
        )

        # Return the complete response
        return ChatResponse(message=response_content, conversation_id=request.conversation_id)

    except ConversationNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process message: {str(e)}",
        )
