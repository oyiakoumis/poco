"""Chat router for handling message processing."""

from typing import Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from twilio.twiml.messaging_response import MessagingResponse
from twilio.request_validator import RequestValidator

from agents.graph import create_graph
from api.config import settings
from api.dependencies import get_conversation_db, get_db
from api.models import ChatRequest, ChatResponse, WhatsAppWebhookRequest
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


def validate_twilio_request(request_data: dict, signature: str, url: str) -> bool:
    """Validate that the request is coming from Twilio."""
    validator = RequestValidator(settings.twilio_auth_token)
    return validator.validate(url, request_data, signature)


async def process_message_core(
    message: str,
    user_id: str,
    conversation_id: UUID,
    message_id: UUID,
    time_zone: str = "UTC",
    first_day_of_week: int = 0,
    metadata: Optional[Dict] = None,
    db: Optional[DatasetManager] = None,
    conversation_db: Optional[ConversationManager] = None,
) -> str:
    """
    Core message processing logic shared between regular chat and WhatsApp.
    
    Returns the assistant's response message.
    """
    logger.info(f"Processing message - Thread: {conversation_id}, User: {user_id}")
    
    try:
        # Check if conversation exists
        conversation_exists = await conversation_db.conversation_exists(user_id, conversation_id)

        if not conversation_exists:
            logger.error(f"Conversation {conversation_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Conversation {conversation_id} not found. Create a conversation first."
            )

        # Store user message
        await conversation_db.create_message(
            user_id=user_id,
            conversation_id=conversation_id,
            content=message,
            role=MessageRole.USER,
            message_id=message_id,
            metadata=metadata,
        )

        # Get conversation history
        messages = await get_conversation_history(conversation_id, user_id, conversation_db)

        # Get the graph
        graph = create_graph(db)
        graph = graph.compile(checkpointer=MemorySaver())

        # Configuration for the graph
        config = RunnableConfig(
            configurable={
                "thread_id": str(conversation_id),
                "user_id": user_id,
                "time_zone": time_zone,
                "first_day_of_the_week": first_day_of_week,
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
        assistant_message_id = uuid4()
        await conversation_db.create_message(
            user_id=user_id,
            conversation_id=conversation_id,
            content=response_content,
            role=MessageRole.ASSISTANT,
            message_id=assistant_message_id,
        )

        return response_content

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


@router.post("/", response_model=ChatResponse)
async def process_message(
    request: ChatRequest, 
    db: DatasetManager = Depends(get_db), 
    conversation_db: ConversationManager = Depends(get_conversation_db)
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

    response_content = await process_message_core(
        message=request.message,
        user_id=request.user_id,
        conversation_id=request.conversation_id,
        message_id=request.message_id,
        time_zone=request.time_zone,
        first_day_of_week=request.first_day_of_week,
        db=db,
        conversation_db=conversation_db,
    )
    
    # Return the complete response
    return ChatResponse(message=response_content, conversation_id=request.conversation_id)


@router.post("/whatsapp", response_class=Response)
async def process_whatsapp_message(
    request: WhatsAppWebhookRequest,
    db: DatasetManager = Depends(get_db),
    conversation_db: ConversationManager = Depends(get_conversation_db),
    x_twilio_signature: str = Header(None),
    request_url: str = Header(None, alias="X-Original-URL")
) -> Response:
    """
    Process incoming WhatsApp messages from Twilio.
    
    This endpoint:
    1. Validates the request is coming from Twilio
    2. Extracts the sender's WhatsApp number and message
    3. Finds or creates a conversation for this user
    4. Processes the message through the existing graph
    5. Returns a TwiML response to send back to WhatsApp
    """
    logger.info(f"Received WhatsApp message from {request.From}: {request.Body}")
    
    # Validate the request is coming from Twilio
    if settings.twilio_auth_token and x_twilio_signature:
        request_data = request.model_dump()
        # If request_url is not provided, construct it from the settings
        url = request_url or f"https://{settings.host}:{settings.port}/chat/whatsapp"
        
        if not validate_twilio_request(request_data, x_twilio_signature, url):
            logger.warning(f"Invalid Twilio signature: {x_twilio_signature}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid Twilio signature"
            )
    
    # Use the WhatsApp number as the user ID
    user_id = request.From
    
    # Find existing conversations for this user
    conversations = await conversation_db.list_conversations(user_id)
    
    # Find a conversation with WhatsApp metadata or create a new one
    conversation_id = None
    for conv in conversations:
        # Check if this is a WhatsApp conversation
        if await conversation_db.conversation_exists(user_id, UUID(conv.id)):
            conversation_id = UUID(conv.id)
            break
    
    # If no conversation found, create a new one
    if not conversation_id:
        conversation_id = uuid4()
        # Create a title based on the user's profile name or number
        title = f"WhatsApp: {request.ProfileName or request.From}"
        await conversation_db.create_conversation(user_id, title, conversation_id)
    
    # Create a message ID for the incoming message
    message_id = uuid4()
    
    # WhatsApp-specific metadata
    metadata = {
        "whatsapp_id": request.WaId,
        "sms_message_sid": request.SmsMessageSid
    }
    
    # Process the message using the shared core function
    response_content = await process_message_core(
        message=request.Body,
        user_id=user_id,
        conversation_id=conversation_id,
        message_id=message_id,
        metadata=metadata,
        db=db,
        conversation_db=conversation_db,
    )
    
    # Create TwiML response
    twiml_response = MessagingResponse()
    twiml_response.message(response_content)
    
    return Response(
        content=str(twiml_response),
        media_type="application/xml"
    )
