"""Example usage of ConversationManager class demonstrating conversation and message operations."""

import asyncio

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient

from constants import DATABASE_CONNECTION_STRING
from conversation_store.conversation_manager import ConversationManager
from conversation_store.models.message import MessageRole


class ExampleConversationManager(ConversationManager):
    """Example implementation using different collections."""

    DATABASE = "conversation_store_example"
    COLLECTION_CONVERSATIONS = "example_conversations"
    COLLECTION_MESSAGES = "example_messages"


async def main():
    """Run example operations."""
    # Initialize MongoDB client
    # Use the same connection string as the main application
    client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
    client.get_io_loop = asyncio.get_running_loop

    try:
        # Setup manager
        manager = await ExampleConversationManager.setup(client)

        # Use same user_id format as main application
        user_id = "user_123"

        # 1. Conversation Operations
        print("\n=== Conversation Operations ===")

        # Create conversation with initial message
        conversation_id = await manager.create_conversation(
            user_id=user_id,
            title="Example Conversation",
            first_message="Hello, this is my first message!",
        )
        print(f"Created conversation: {conversation_id}")

        # Check if conversation exists
        exists = await manager.conversation_exists(user_id, conversation_id)
        print(f"Conversation exists: {exists}")

        # Get specific conversation
        conversation = await manager.get_conversation(user_id, conversation_id)
        print(f"Retrieved conversation: {conversation.title}")

        # List conversations
        conversations = await manager.list_conversations(user_id)
        print(f"Found {len(conversations)} conversations")

        # Update conversation
        await manager.update_conversation(
            user_id=user_id,
            conversation_id=conversation_id,
            title="Updated Example Conversation",
        )
        print("Updated conversation title")

        # Get updated conversation
        updated_conversation = await manager.get_conversation(user_id, conversation_id)
        print(f"Updated conversation title: {updated_conversation.title}")

        # 2. Message Operations
        print("\n=== Message Operations ===")

        # Create assistant message
        assistant_message_id = await manager.create_message(
            user_id=user_id,
            conversation_id=conversation_id,
            content="Hello! I'm an AI assistant. How can I help you today?",
            role=MessageRole.ASSISTANT,
        )
        print(f"Created assistant message: {assistant_message_id}")

        # Create user message with metadata
        user_message_id = await manager.create_message(
            user_id=user_id,
            conversation_id=conversation_id,
            content="Can you help me with a Python question?",
            role=MessageRole.USER,
            metadata={"source": "web", "browser": "Chrome"},
        )
        print(f"Created user message with metadata: {user_message_id}")

        # Get specific message
        message = await manager.get_message(user_id, assistant_message_id)
        print(f"Retrieved message: {message.content}")

        # List messages in conversation
        messages = await manager.list_messages(user_id, conversation_id)
        print(f"Found {len(messages)} messages in conversation")
        for idx, msg in enumerate(messages):
            print(f"  {idx+1}. [{msg.role.value}]: {msg.content}")

        # Create a few more messages to demonstrate pagination
        for i in range(3):
            await manager.create_message(
                user_id=user_id,
                conversation_id=conversation_id,
                content=f"Additional message {i+1}",
                role=MessageRole.USER if i % 2 == 0 else MessageRole.ASSISTANT,
            )
        
        # List messages with pagination
        print("\n=== Message Pagination ===")
        paginated_messages = await manager.list_messages(user_id, conversation_id, limit=2, skip=1)
        print(f"Retrieved {len(paginated_messages)} messages with pagination (limit=2, skip=1)")
        for idx, msg in enumerate(paginated_messages):
            print(f"  {idx+1}. [{msg.role.value}]: {msg.content}")

        # 3. Delete Operations
        print("\n=== Delete Operations ===")

        # Delete a message
        await manager.delete_message(user_id, user_message_id)
        print(f"Deleted message: {user_message_id}")

        # Verify message is deleted
        try:
            await manager.get_message(user_id, user_message_id)
            print("Error: Message should have been deleted")
        except Exception as e:
            print(f"Successfully verified message deletion: {str(e)}")

        # Create a second conversation for testing
        second_conversation_id = await manager.create_conversation(
            user_id=user_id,
            title="Second Conversation",
            first_message="This is a second conversation",
        )
        print(f"Created second conversation: {second_conversation_id}")

        # List all conversations again
        conversations = await manager.list_conversations(user_id)
        print(f"Now have {len(conversations)} conversations")

        # Delete the first conversation and all its messages
        await manager.delete_conversation(user_id, conversation_id)
        print(f"Deleted conversation: {conversation_id}")

        # Verify conversation is deleted
        try:
            await manager.get_conversation(user_id, conversation_id)
            print("Error: Conversation should have been deleted")
        except Exception as e:
            print(f"Successfully verified conversation deletion: {str(e)}")

        # List conversations again to confirm deletion
        remaining_conversations = await manager.list_conversations(user_id)
        print(f"Remaining conversations: {len(remaining_conversations)}")

        # 4. Error Handling Examples
        print("\n=== Error Handling Examples ===")

        # Try to get a non-existent conversation
        non_existent_id = ObjectId()
        try:
            await manager.get_conversation(user_id, non_existent_id)
            print("Error: Should have raised ConversationNotFoundError")
        except Exception as e:
            print(f"Successfully caught error for non-existent conversation: {str(e)}")

        # Try to get a non-existent message
        try:
            await manager.get_message(user_id, non_existent_id)
            print("Error: Should have raised MessageNotFoundError")
        except Exception as e:
            print(f"Successfully caught error for non-existent message: {str(e)}")

        # Cleanup
        print("\n=== Cleanup ===")
        await manager.delete_conversation(user_id, second_conversation_id)
        print("Deleted all remaining conversations")

    finally:
        # Cleanup database
        await client.drop_database(ExampleConversationManager.DATABASE)
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
