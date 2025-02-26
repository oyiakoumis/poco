"""Service for handling streaming responses."""

import json
from typing import AsyncGenerator, Dict, Any, Optional

from fastapi import HTTPException, status
from langchain_core.messages import AIMessage

from api.models import ChatResponse
from conversation_store.conversation_manager import ConversationManager
from conversation_store.models.message import MessageRole
from models.base import PydanticObjectId
from utils.logging import logger


class StreamingService:
    """Service for handling streaming responses."""

    @staticmethod
    async def stream_chat_response(
        graph, 
        messages: list, 
        config: dict, 
        conversation_db: ConversationManager,
        user_id: str,
        conversation_id: PydanticObjectId
    ) -> AsyncGenerator[str, None]:
        """
        Stream chat response as SSE events.
        
        Args:
            graph: The LangGraph instance
            messages: The conversation history
            config: Configuration for the graph
            conversation_db: The conversation database manager
            user_id: The user ID
            conversation_id: The conversation ID
            
        Yields:
            SSE formatted events
        """
        full_response = ""
        error_occurred = False
        
        try:
            # Process all events from the graph
            async for event in graph.astream({"messages": messages}, config, stream_mode="updates"):
                # Extract new content from the event
                delta = StreamingService._extract_delta_from_event(event, full_response)
                
                if delta:
                    # Update the full response
                    full_response += delta
                    
                    # Yield the delta as an SSE event
                    yield StreamingService._format_sse_event(ChatResponse(
                        delta=delta, 
                        done=False
                    ))
            
            # Store the complete response
            await StreamingService._store_response(
                conversation_db, 
                user_id, 
                conversation_id, 
                full_response or "I apologize, but I couldn't process your request."
            )
            
            # Send the final event with the complete message for backward compatibility
            yield StreamingService._format_sse_event(ChatResponse(
                delta="", 
                done=True, 
                message=full_response,  # Include full message in final event
                conversation_id=conversation_id  # PydanticObjectId handles serialization
            ))
                
        except Exception as e:
            error_occurred = True
            logger.error(f"Error in streaming response: {str(e)}")
            
            # Yield an error event with error message and done=true
            yield StreamingService._format_sse_event(ChatResponse(
                error=str(e),
                done=True,
                message=f"Error: {str(e)}"  # Include error message for backward compatibility
            ))
            
            # Re-raise for FastAPI to handle
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to process message: {str(e)}",
            )
        finally:
            # If no response was generated but no error occurred, store a fallback message
            if not full_response and not error_occurred:
                fallback_message = "I apologize, but I couldn't process your request."
                await StreamingService._store_response(
                    conversation_db, 
                    user_id, 
                    conversation_id, 
                    fallback_message
                )
                
                # Send the fallback message and a "done" event with the complete message
                yield StreamingService._format_sse_event(ChatResponse(
                    delta=fallback_message, 
                    done=False
                ))
                yield StreamingService._format_sse_event(ChatResponse(
                    delta="", 
                    done=True, 
                    message=fallback_message,  # Include full message in final event
                    conversation_id=conversation_id  # PydanticObjectId handles serialization
                ))
    
    @staticmethod
    def _extract_delta_from_event(event: Dict[str, Any], current_response: str) -> Optional[str]:
        """
        Extract the new content (delta) from an event.
        
        Args:
            event: The event from the graph
            current_response: The current accumulated response
            
        Returns:
            The new content to append, or None if no new content
        """
        if not isinstance(event, dict) or "assistant" not in event:
            return None
            
        assistant_data = event["assistant"]
        if not isinstance(assistant_data, dict) or "messages" not in assistant_data:
            return None
            
        event_messages = assistant_data["messages"]
        if not event_messages or not isinstance(event_messages[-1], AIMessage):
            return None
            
        # Get the current message content
        current_content = event_messages[-1].content
        
        # Determine the delta (new content)
        if len(current_content) > len(current_response):
            return current_content[len(current_response):]
            
        return None
    
    @staticmethod
    def _format_sse_event(response: ChatResponse) -> str:
        """
        Format a ChatResponse as an SSE event.
        
        Args:
            response: The ChatResponse object to format
            
        Returns:
            SSE formatted event
        """
        # Convert to JSON using Pydantic's model_dump_json method
        return f"data: {response.model_dump_json(exclude_none=True)}\n\n"
    
    @staticmethod
    async def _store_response(
        conversation_db: ConversationManager,
        user_id: str,
        conversation_id: PydanticObjectId,
        content: str
    ) -> None:
        """
        Store the assistant's response in the conversation.
        
        Args:
            conversation_db: The conversation database manager
            user_id: The user ID
            conversation_id: The conversation ID
            content: The content to store
        """
        await conversation_db.create_message(
            user_id=user_id,
            conversation_id=conversation_id,
            content=content,
            role=MessageRole.ASSISTANT,
        )
