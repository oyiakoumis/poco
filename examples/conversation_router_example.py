"""Example usage of conversation router endpoints demonstrating API interactions."""

import asyncio
import json
from typing import Dict, List, Optional, Union
from uuid import uuid4

import httpx
from fastapi import FastAPI, status
from fastapi.testclient import TestClient

from api.dependencies import get_conversation_db
from api.main import app
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
from conversation_store.models.message import MessageRole


class TestConversationManager(ConversationManager):
    """Test implementation using different collections."""

    DATABASE = "conversation_router_test"
    COLLECTION_CONVERSATIONS = "test_conversations"
    COLLECTION_MESSAGES = "test_messages"


# Create a global client to ensure the same connection is used throughout the test
from motor.motor_asyncio import AsyncIOMotorClient
from constants import DATABASE_CONNECTION_STRING

# Create a single client instance for all requests
mongo_client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
mongo_client.get_io_loop = asyncio.get_running_loop


# Override dependency for testing
async def get_test_conversation_db():
    """Get test conversation database."""
    # Use the global client to ensure the same connection is used
    manager = await TestConversationManager.setup(mongo_client)
    print(f"Using test database: {TestConversationManager.DATABASE}")
    print(f"Using collections: {TestConversationManager.COLLECTION_CONVERSATIONS}, {TestConversationManager.COLLECTION_MESSAGES}")
    yield manager  # Yield the manager to maintain the same instance throughout the request


# Override the dependency in the app
app.dependency_overrides[get_conversation_db] = get_test_conversation_db


# Function to clean up the test database after the test
async def cleanup_test_database():
    """Clean up the test database."""
    await mongo_client.drop_database(TestConversationManager.DATABASE)
    mongo_client.close()


def print_response(response, title: str):
    """Print formatted response."""
    print(f"\n=== {title} ===")
    print(f"Status Code: {response.status_code}")
    if response.status_code != 204:  # No content responses don't have a body
        try:
            print(f"Response: {json.dumps(response.json(), indent=2)}")
        except json.JSONDecodeError:
            print(f"Response: {response.text}")


def main():
    """Run example operations testing the conversation router endpoints."""
    with TestClient(app) as client:
        # Use same user_id format as main application
        user_id = "user_123"

        # 1. Conversation Operations
        print("\n=== Conversation Operations ===")

        # Create conversation
        create_data = ConversationCreate(user_id=user_id, title="Test Conversation")
        print(f"Creating conversation with data: {create_data.model_dump()}")
        response = client.post("/conversations/", json=create_data.model_dump())
        print_response(response, "Create Conversation")
        assert response.status_code == status.HTTP_201_CREATED
        assert "id" in response.json(), "Response should contain conversation ID"
        assert response.json()["user_id"] == user_id, "Response should contain correct user_id"
        assert response.json()["title"] == "Test Conversation", "Response should contain correct title"
        conversation_id = response.json()["id"]
        print(f"Created conversation with ID: {conversation_id}")

        # List conversations
        print(f"Listing conversations for user: {user_id}")
        response = client.get(f"/conversations/?user_id={user_id}")
        print_response(response, "List Conversations")
        assert response.status_code == status.HTTP_200_OK
        assert "conversations" in response.json(), "Response should contain 'conversations' field"
        assert "total" in response.json(), "Response should contain 'total' field"
        assert response.json()["total"] >= 1, "Should have at least 1 conversation"
        assert len(response.json()["conversations"]) >= 1, "Should have at least 1 conversation in the list"
        assert any(conv["id"] == conversation_id for conv in response.json()["conversations"]), "Created conversation should be in the list"
        print(f"Found {response.json()['total']} conversations in the response")

        # Get specific conversation
        response = client.get(f"/conversations/{conversation_id}?user_id={user_id}")
        print_response(response, "Get Conversation")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["title"] == "Test Conversation"
        assert response.json()["id"] == conversation_id, "Response should contain correct conversation ID"
        assert response.json()["user_id"] == user_id, "Response should contain correct user_id"

        # Update conversation
        update_data = ConversationUpdate(title="Updated Test Conversation")
        response = client.put(f"/conversations/{conversation_id}?user_id={user_id}", json=update_data.model_dump())
        print_response(response, "Update Conversation")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["title"] == "Updated Test Conversation"
        assert response.json()["id"] == conversation_id, "Conversation ID should not change after update"
        assert response.json()["user_id"] == user_id, "User ID should not change after update"

        # 2. Message Operations
        print("\n=== Message Operations ===")

        # Create message
        message_data = MessageCreate(
            user_id=user_id,
            content="Hello, this is a test message",
        )
        response = client.post(f"/conversations/{conversation_id}/messages", json=message_data.model_dump())
        print_response(response, "Create Message")
        assert response.status_code == status.HTTP_201_CREATED
        assert "id" in response.json(), "Response should contain message ID"
        assert response.json()["user_id"] == user_id, "Response should contain correct user_id"
        assert response.json()["content"] == "Hello, this is a test message", "Response should contain correct content"
        assert response.json()["conversation_id"] == conversation_id, "Response should contain correct conversation_id"
        message_id = response.json()["id"]

        # Create another message
        message_data = MessageCreate(
            user_id=user_id,
            content="This is a second test message",
        )
        response = client.post(f"/conversations/{conversation_id}/messages", json=message_data.model_dump())
        print_response(response, "Create Second Message")
        assert response.status_code == status.HTTP_201_CREATED
        assert "id" in response.json(), "Response should contain message ID"
        assert response.json()["content"] == "This is a second test message", "Response should contain correct content"
        second_message_id = response.json()["id"]
        assert second_message_id != message_id, "Second message should have a different ID"

        # List messages
        response = client.get(f"/conversations/{conversation_id}/messages?user_id={user_id}")
        print_response(response, "List Messages")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["total"] == 2, "Should have exactly 2 messages"
        assert "messages" in response.json(), "Response should contain 'messages' field"
        assert len(response.json()["messages"]) == 2, "Should return 2 messages"
        message_ids = [msg["id"] for msg in response.json()["messages"]]
        assert message_id in message_ids, "First message should be in the list"
        assert second_message_id in message_ids, "Second message should be in the list"

        # List messages with pagination
        response = client.get(f"/conversations/{conversation_id}/messages?user_id={user_id}&skip=1&limit=1")
        print_response(response, "List Messages with Pagination")
        assert response.status_code == status.HTTP_200_OK
        assert len(response.json()["messages"]) == 1, "Should return exactly 1 message with limit=1"
        assert response.json()["total"] == 2, "Total should still be 2 even with pagination"

        # 3. Error Handling Examples
        print("\n=== Error Handling Examples ===")

        # Try to get a non-existent conversation
        non_existent_id = str(uuid4())
        response = client.get(f"/conversations/{non_existent_id}?user_id={user_id}")
        print_response(response, "Get Non-existent Conversation")
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "detail" in response.json(), "Error response should contain 'detail' field"
        assert "not found" in response.json()["detail"].lower(), "Error message should indicate resource not found"

        # Try to create a message in a non-existent conversation
        message_data = MessageCreate(
            user_id=user_id,
            content="This message should fail",
        )
        response = client.post(f"/conversations/{non_existent_id}/messages", json=message_data.model_dump())
        print_response(response, "Create Message in Non-existent Conversation")
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "detail" in response.json(), "Error response should contain 'detail' field"
        assert "not found" in response.json()["detail"].lower(), "Error message should indicate resource not found"

        # 4. Delete Operations
        print("\n=== Delete Operations ===")

        # Delete conversation
        response = client.delete(f"/conversations/{conversation_id}?user_id={user_id}")
        print_response(response, "Delete Conversation")
        assert response.status_code == status.HTTP_204_NO_CONTENT

        # Verify conversation is deleted
        response = client.get(f"/conversations/{conversation_id}?user_id={user_id}")
        print_response(response, "Get Deleted Conversation")
        assert response.status_code == status.HTTP_404_NOT_FOUND

        # List conversations to confirm deletion
        response = client.get(f"/conversations/?user_id={user_id}")
        print_response(response, "List Conversations After Deletion")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["total"] == 0

        print("\n=== Test Completed Successfully ===")
        print("\nAll assertions passed successfully!")


if __name__ == "__main__":
    main()
