"""Conversation router for handling conversation operations."""

from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.dependencies import get_conversation_db
from api.models import (
    ConversationCreate,
    ConversationListResponse,
    ConversationResponse,
    ConversationUpdate,
    MessageCreate,
    MessageListResponse,
    MessageResponse,
)
from conversation_store.conversation_manager import ConversationManager
from conversation_store.exceptions import (
    ConversationNotFoundError,
    InvalidConversationError,
    InvalidMessageError,
)
from conversation_store.models.message import MessageRole
from utils.logging import logger

router = APIRouter(prefix="/conversations", tags=["conversations"])


@router.post("/", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED)
async def create_conversation(request: ConversationCreate, db: ConversationManager = Depends(get_conversation_db)) -> ConversationResponse:
    """Create a new conversation with an initial message."""
    try:
        # Create conversation with first message
        conversation_id = await db.create_conversation(
            user_id=request.user_id,
            title=request.title,
            first_message=request.first_message,
        )

        # Get the created conversation
        conversation = await db.get_conversation(request.user_id, conversation_id)

        # Convert to response model
        return ConversationResponse(
            id=str(conversation.id),
            title=conversation.title,
            user_id=conversation.user_id,
            created_at=conversation.created_at,
            updated_at=conversation.updated_at,
        )
    except InvalidConversationError as e:
        logger.error(f"Failed to create conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/", response_model=ConversationListResponse)
async def list_conversations(
    user_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    db: ConversationManager = Depends(get_conversation_db),
) -> ConversationListResponse:
    """List all conversations for a user."""
    try:
        # Get conversations
        conversations = await db.list_conversations(user_id, limit=limit, skip=skip)

        # Convert to response models
        response_conversations = [
            ConversationResponse(
                id=str(conv.id),
                title=conv.title,
                user_id=conv.user_id,
                created_at=conv.created_at,
                updated_at=conv.updated_at,
            )
            for conv in conversations
        ]

        # Count total conversations (without pagination)
        # In a real application, you might want to optimize this with a separate count query
        total = len(await db.list_conversations(user_id, limit=0, skip=0))

        return ConversationListResponse(conversations=response_conversations, total=total)
    except Exception as e:
        logger.error(f"Failed to list conversations: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(conversation_id: str, user_id: str, db: ConversationManager = Depends(get_conversation_db)) -> ConversationResponse:
    """Get a specific conversation."""
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(conversation_id)

        # Get conversation
        conversation = await db.get_conversation(user_id, obj_id)

        # Convert to response model
        return ConversationResponse(
            id=str(conversation.id),
            title=conversation.title,
            user_id=conversation.user_id,
            created_at=conversation.created_at,
            updated_at=conversation.updated_at,
        )
    except ConversationNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")
    except InvalidConversationError as e:
        logger.error(f"Failed to get conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error getting conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.put("/{conversation_id}", response_model=ConversationResponse)
async def update_conversation(
    conversation_id: str,
    request: ConversationUpdate,
    user_id: str,
    db: ConversationManager = Depends(get_conversation_db),
) -> ConversationResponse:
    """Update a conversation."""
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(conversation_id)

        # Update conversation
        await db.update_conversation(
            user_id=user_id,
            conversation_id=obj_id,
            title=request.title,
        )

        # Get updated conversation
        conversation = await db.get_conversation(user_id, obj_id)

        # Convert to response model
        return ConversationResponse(
            id=str(conversation.id),
            title=conversation.title,
            user_id=conversation.user_id,
            created_at=conversation.created_at,
            updated_at=conversation.updated_at,
        )
    except ConversationNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")
    except InvalidConversationError as e:
        logger.error(f"Failed to update conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error updating conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(conversation_id: str, user_id: str, db: ConversationManager = Depends(get_conversation_db)) -> None:
    """Delete a conversation and all its messages."""
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(conversation_id)

        # Delete conversation
        await db.delete_conversation(user_id, obj_id)
    except ConversationNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")
    except InvalidConversationError as e:
        logger.error(f"Failed to delete conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error deleting conversation: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.get("/{conversation_id}/messages", response_model=MessageListResponse)
async def list_messages(
    conversation_id: str,
    user_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: ConversationManager = Depends(get_conversation_db),
) -> MessageListResponse:
    """List all messages in a conversation."""
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(conversation_id)

        # Get messages
        messages = await db.list_messages(user_id, obj_id, limit=limit, skip=skip)

        # Convert to response models
        response_messages = [
            MessageResponse(
                id=str(msg.id),
                conversation_id=str(msg.conversation_id),
                content=msg.content,
                role=msg.role.value,
                user_id=msg.user_id,
                created_at=msg.created_at,
            )
            for msg in messages
        ]

        # Count total messages (without pagination)
        # In a real application, you might want to optimize this with a separate count query
        total = len(await db.list_messages(user_id, obj_id, limit=0, skip=0))

        return MessageListResponse(messages=response_messages, total=total)
    except ConversationNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")
    except InvalidMessageError as e:
        logger.error(f"Failed to list messages: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error listing messages: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")


@router.post("/{conversation_id}/messages", response_model=MessageResponse, status_code=status.HTTP_201_CREATED)
async def create_message(
    conversation_id: str,
    request: MessageCreate,
    db: ConversationManager = Depends(get_conversation_db),
) -> MessageResponse:
    """Create a new message in a conversation."""
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(conversation_id)

        # Create message
        message_id = await db.create_message(
            user_id=request.user_id,
            conversation_id=obj_id,
            content=request.content,
            role=MessageRole.USER,
        )

        # Get created message
        message = await db.get_message(request.user_id, message_id)

        # Convert to response model
        return MessageResponse(
            id=str(message.id),
            conversation_id=str(message.conversation_id),
            content=message.content,
            role=message.role.value,
            user_id=message.user_id,
            created_at=message.created_at,
        )
    except ConversationNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found")
    except InvalidMessageError as e:
        logger.error(f"Failed to create message: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating message: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error")
