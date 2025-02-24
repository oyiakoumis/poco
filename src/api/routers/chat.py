"""Chat router for handling message processing."""

import json

from fastapi import APIRouter, Depends
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver

from agents.workflow import get_graph
from api.dependencies import get_db
from api.models import ChatRequest, ChatResponse
from document_store.dataset_manager import DatasetManager
from utils.logging import logger

router = APIRouter(prefix="/chat", tags=["chat"])


@router.post("/", response_model=ChatResponse)
async def process_message(request: ChatRequest, db: DatasetManager = Depends(get_db)) -> ChatResponse:
    """Process a chat message."""
    logger.info(f"Starting message processing - Thread: {request.thread_id}, User: {request.user_id}")

    # Get the graph
    graph = get_graph(db)
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

    # Convert request to messages
    messages = request.to_messages()

    # Process message and get final assistant response
    assistant_message = None

    # Process all events from the graph
    async for event in graph.astream({"messages": messages}, config, stream_mode="updates"):
        # Log stream event
        logger.debug(f"Stream event received.")

        # Get only assistant messages (filter out tool messages)
        if isinstance(event, dict) and "assistant" in event:
            assistant_data = event["assistant"]
            if isinstance(assistant_data, dict) and "messages" in assistant_data:
                messages = assistant_data["messages"]
                if messages and len(messages) > 0 and isinstance(messages[-1], AIMessage):
                    assistant_message = messages[-1].content
                    logger.info("Assistant response generated")

    if not assistant_message:
        logger.warning("No assistant message generated, using fallback response")
        assistant_message = "I apologize, but I couldn't process your request."

    logger.info("Message processing completed")
    return ChatResponse(message=assistant_message)
