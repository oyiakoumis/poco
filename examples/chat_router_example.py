"""Example usage of chat router endpoints demonstrating API interactions."""

import asyncio
import json
from uuid import uuid4

from fastapi import status
from fastapi.testclient import TestClient

from api.dependencies import get_conversation_db, get_db
from api.main import app
from api.models import (
    ChatRequest,
    ConversationCreate,
)
from conversation_store.conversation_manager import ConversationManager
from document_store.dataset_manager import DatasetManager


class TestConversationManager(ConversationManager):
    """Test implementation using different collections."""

    DATABASE = "chat_router_test"
    COLLECTION_CONVERSATIONS = "test_conversations"
    COLLECTION_MESSAGES = "test_messages"


class TestDatasetManager(DatasetManager):
    """Test implementation for dataset manager."""

    DATABASE = "chat_router_test"
    COLLECTION_DATASETS = "test_datasets"
    COLLECTION_RECORDS = "test_records"


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


async def get_test_db():
    """Get test dataset database."""
    # Use the global client to ensure the same connection is used
    manager = await TestDatasetManager.setup(mongo_client)
    print(f"Using test database: {TestDatasetManager.DATABASE}")
    print(f"Using collections: {TestDatasetManager.COLLECTION_DATASETS}, {TestDatasetManager.COLLECTION_RECORDS}")
    yield manager  # Yield the manager to maintain the same instance throughout the request


# Override the dependencies in the app
app.dependency_overrides[get_conversation_db] = get_test_conversation_db
app.dependency_overrides[get_db] = get_test_db


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
    """Run example operations testing the chat router endpoints."""
    with TestClient(app) as client:
        # Use same user_id format as main application
        user_id = "user_123"
        thread_id = "thread_123"

        # 1. Create a conversation first
        print("\n=== Setup: Create Conversation ===")
        create_data = ConversationCreate(user_id=user_id, title="Test Chat Conversation")
        print(f"Creating conversation with data: {create_data.model_dump()}")
        response = client.post("/conversations/", json=create_data.model_dump())
        print_response(response, "Create Conversation")
        assert response.status_code == status.HTTP_201_CREATED
        assert "id" in response.json(), "Response should contain conversation ID"
        assert response.json()["user_id"] == user_id, "Response should contain correct user_id"
        assert response.json()["title"] == "Test Chat Conversation", "Response should contain correct title"
        conversation_id = response.json()["id"]
        print(f"Created conversation with ID: {conversation_id}")

        # 2. Chat Operations
        print("\n=== Chat Operations ===")

        # Send a chat message
        chat_data = ChatRequest(
            user_id=user_id,
            conversation_id=conversation_id,
            thread_id=thread_id,
            message="Hello, can you help me with something?",
            time_zone="UTC",
            first_day_of_week=0,
        )

        print(f"Sending chat message: {chat_data.model_dump()}")

        # Send the chat message and get the response
        response = client.post("/chat/", json=chat_data.model_dump())
        print(f"Status Code: {response.status_code}")

        # Print the response
        print_response(response, "Chat Response")
        
        # Ensure the response is successful
        assert response.status_code == status.HTTP_200_OK, f"Expected 200 OK, got {response.status_code}"
        assert "message" in response.json(), "Response should contain 'message' field"
        assert isinstance(response.json()["message"], str), "Message field should be a string"
        assert len(response.json()["message"]) > 0, "Message should not be empty"
        assert "conversation_id" in response.json(), "Response should contain 'conversation_id' field"

        # 3. Error Handling Examples
        print("\n=== Error Handling Examples ===")

        # Try to send a message to a non-existent conversation
        non_existent_id = str(uuid4())
        chat_data = ChatRequest(
            user_id=user_id,
            conversation_id=non_existent_id,
            thread_id=thread_id,
            message="This message should fail",
            time_zone="UTC",
            first_day_of_week=0,
        )

        response = client.post("/chat/", json=chat_data.model_dump())
        print_response(response, "Send Message to Non-existent Conversation")
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "detail" in response.json(), "Error response should contain 'detail' field"
        assert "not found" in response.json()["detail"].lower(), "Error message should indicate resource not found"

        print("\n=== Test Completed ===")

        # Clean up the test database
        print("\nCleaning up test database...")
        asyncio.run(cleanup_test_database())
        print("Test database cleaned up successfully.")
        
        # Verify all assertions passed
        print("\nAll assertions passed successfully!")


if __name__ == "__main__":
    main()
