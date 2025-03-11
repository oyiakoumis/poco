"""Example usage of ConversationManager class demonstrating conversation and message operations."""

import asyncio
from uuid import UUID, uuid4

from motor.motor_asyncio import AsyncIOMotorClient
from langchain_core.messages import HumanMessage, AIMessage

from constants import DATABASE_CONNECTION_STRING
from database.conversation_store.conversation_manager import ConversationManager
from database.conversation_store.models.message import MessageRole


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

        # Create conversation with UUID
        conversation_id = uuid4()
        conversation_id = await manager.create_conversation(
            user_id=user_id,
            title="Example Conversation",
            conversation_id=conversation_id,
        )
        print(f"Created conversation: {conversation_id}")
        assert conversation_id is not None, "Conversation ID should not be None"
        assert isinstance(conversation_id, UUID), "Conversation ID should be a string"

        # Check if conversation exists
        exists = await manager.conversation_exists(user_id, conversation_id)
        print(f"Conversation exists: {exists}")
        assert exists is True, "Newly created conversation should exist"

        # Get specific conversation
        conversation = await manager.get_conversation(user_id, conversation_id)
        print(f"Retrieved conversation: {conversation.title}")
        assert conversation.id == conversation_id, "Retrieved conversation ID should match"
        assert conversation.user_id == user_id, "Retrieved conversation user_id should match"
        assert conversation.title == "Example Conversation", "Retrieved conversation title should match"

        # List conversations
        conversations = await manager.list_conversations(user_id)
        print(f"Found {len(conversations)} conversations")
        assert len(conversations) >= 1, "Should have at least one conversation"
        assert any(conv.id == conversation_id for conv in conversations), "Created conversation should be in the list"

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
        assert updated_conversation.title == "Updated Example Conversation", "Conversation title should be updated"
        assert updated_conversation.id == conversation_id, "Conversation ID should not change after update"
        assert updated_conversation.user_id == user_id, "User ID should not change after update"

        # 2. Message Operations
        print("\n=== Message Operations ===")

        # Create assistant message with UUID
        assistant_message_id = uuid4()
        assistant_message_id = await manager.create_message(
            user_id=user_id,
            conversation_id=conversation_id,
            message=AIMessage("Hello! I'm an AI assistant. How can I help you today?", id=assistant_message_id),
            role=MessageRole.ASSISTANT,
            message_id=assistant_message_id,
        )
        print(f"Created assistant message: {assistant_message_id}")
        assert assistant_message_id is not None, "Message ID should not be None"
        assert isinstance(assistant_message_id, UUID), "Message ID should be a string"

        # Create user message with metadata and UUID
        user_message_id = uuid4()
        user_message_id = await manager.create_message(
            user_id=user_id,
            conversation_id=conversation_id,
            message=HumanMessage("Can you help me with a Python question?", id=user_message_id),
            role=MessageRole.HUMAN,
            message_id=user_message_id,
            metadata={"source": "web", "browser": "Chrome"},
        )
        print(f"Created user message with metadata: {user_message_id}")
        assert user_message_id is not None, "Message ID should not be None"
        assert user_message_id != assistant_message_id, "User message ID should be different from assistant message ID"

        # Get specific message
        message = await manager.get_message(user_id, assistant_message_id)
        print(f"Retrieved message: {message.message.content}")
        assert message.id == assistant_message_id, "Retrieved message ID should match"
        assert message.user_id == user_id, "Retrieved message user_id should match"
        assert message.conversation_id == conversation_id, "Retrieved message conversation_id should match"
        assert message.message.content == "Hello! I'm an AI assistant. How can I help you today?", "Retrieved message content should match"
        assert message.role == MessageRole.ASSISTANT, "Retrieved message role should match"

        # List messages in conversation
        messages = await manager.list_messages(user_id, conversation_id)
        print(f"Found {len(messages)} messages in conversation")
        assert len(messages) == 2, "Should have exactly 2 messages"
        message_ids = [msg.id for msg in messages]
        assert assistant_message_id in message_ids, "Assistant message should be in the list"
        assert user_message_id in message_ids, "User message should be in the list"
        for idx, msg in enumerate(messages):
            print(f"  {idx+1}. [{msg.role.value}]: {msg.message.content}")

        # Demonstrate create_messages method for creating multiple messages at once
        print("\n=== Batch Message Creation ===")
        batch_messages = [
            HumanMessage("Can you explain how to use Python's asyncio?"),
            AIMessage("Asyncio is a library to write concurrent code using the async/await syntax."),
            HumanMessage("Can you show me an example?"),
            AIMessage("Sure! Here's a simple example:\n\n```python\nimport asyncio\n\nasync def main():\n    print('Hello')\n    await asyncio.sleep(1)\n    print('World')\n\nasyncio.run(main())\n```")
        ]
        
        batch_message_ids = await manager.create_messages(
            user_id=user_id,
            conversation_id=conversation_id,
            messages=batch_messages,
        )
        print(f"Created {len(batch_message_ids)} messages in a single batch operation")
        assert len(batch_message_ids) == 4, "Should have created 4 messages in batch"
        
        # Verify the batch messages were created with correct roles
        batch_messages_from_db = await manager.list_messages(user_id, conversation_id)
        print(f"Total messages in conversation after batch: {len(batch_messages_from_db)}")
        
        # The last 4 messages should be our batch messages
        last_four_messages = batch_messages_from_db[-4:]
        assert len(last_four_messages) == 4, "Should have 4 batch messages"
        
        # Verify roles were correctly determined by message type
        assert last_four_messages[0].role == MessageRole.HUMAN, "First batch message should have HUMAN role"
        assert last_four_messages[1].role == MessageRole.ASSISTANT, "Second batch message should have ASSISTANT role"
        assert last_four_messages[2].role == MessageRole.HUMAN, "Third batch message should have HUMAN role"
        assert last_four_messages[3].role == MessageRole.ASSISTANT, "Fourth batch message should have ASSISTANT role"
        
        print("Successfully verified batch message creation with automatic role determination")
        
        # Create a few more messages to demonstrate pagination
        additional_message_ids = []
        for i in range(3):
            msg_id_uuid = uuid4()
            msg_id = await manager.create_message(
                user_id=user_id,
                conversation_id=conversation_id,
                message=HumanMessage(f"Additional message {i+1}", id=msg_id_uuid) if i % 2 == 0 else AIMessage(f"Response {i+1}", id=msg_id_uuid),
                role=MessageRole.HUMAN if i % 2 == 0 else MessageRole.ASSISTANT,
                message_id=msg_id_uuid,
            )
            additional_message_ids.append(msg_id)
            assert msg_id is not None, f"Additional message {i+1} ID should not be None"

        # Verify we now have 5 messages total
        all_messages = await manager.list_messages(user_id, conversation_id)
        assert len(all_messages) == 9, "Should have 9 messages total after adding 3 more"

        # List messages with pagination
        print("\n=== Message Pagination ===")
        paginated_messages = await manager.list_messages(user_id, conversation_id, limit=2, skip=1)
        print(f"Retrieved {len(paginated_messages)} messages with pagination (limit=2, skip=1)")
        assert len(paginated_messages) == 2, "Should have exactly 2 messages with limit=2"
        for idx, msg in enumerate(paginated_messages):
            print(f"  {idx+1}. [{msg.role.value}]: {msg.message.content}")

        # 3. Delete Operations
        print("\n=== Delete Operations ===")

        # Delete a message
        await manager.delete_message(user_id, user_message_id)
        print(f"Deleted message: {user_message_id}")

        # Verify message is deleted
        try:
            await manager.get_message(user_id, user_message_id)
            assert False, "Should have raised an exception for deleted message"
        except Exception as e:
            print(f"Successfully verified message deletion: {str(e)}")
            assert "not found" in str(e).lower(), "Exception should indicate message not found"

        # Verify we now have 4 messages after deletion
        remaining_messages = await manager.list_messages(user_id, conversation_id)
        assert len(remaining_messages) == 8, "Should have 8 messages after deleting 1"

        # Create a second conversation for testing with UUID
        second_conversation_id_uuid = uuid4()
        second_conversation_id = await manager.create_conversation(
            user_id=user_id,
            title="Second Conversation",
            conversation_id=second_conversation_id_uuid,
        )
        print(f"Created second conversation: {second_conversation_id}")
        assert second_conversation_id is not None, "Second conversation ID should not be None"
        assert second_conversation_id != conversation_id, "Second conversation should have a different ID"

        # List all conversations again
        conversations = await manager.list_conversations(user_id)
        print(f"Now have {len(conversations)} conversations")
        assert len(conversations) == 2, "Should have exactly 2 conversations"
        conversation_ids = [conv.id for conv in conversations]
        assert conversation_id in conversation_ids, "First conversation should be in the list"
        assert second_conversation_id in conversation_ids, "Second conversation should be in the list"

        # Delete the first conversation and all its messages
        await manager.delete_conversation(user_id, conversation_id)
        print(f"Deleted conversation: {conversation_id}")

        # Verify conversation is deleted
        try:
            await manager.get_conversation(user_id, conversation_id)
            assert False, "Should have raised an exception for deleted conversation"
        except Exception as e:
            print(f"Successfully verified conversation deletion: {str(e)}")
            assert "not found" in str(e).lower(), "Exception should indicate conversation not found"

        # List conversations again to confirm deletion
        remaining_conversations = await manager.list_conversations(user_id)
        print(f"Remaining conversations: {len(remaining_conversations)}")
        assert len(remaining_conversations) == 1, "Should have exactly 1 conversation remaining"
        assert remaining_conversations[0].id == second_conversation_id, "Remaining conversation should be the second one"

        # 4. Error Handling Examples
        print("\n=== Error Handling Examples ===")

        # Try to get a non-existent conversation
        non_existent_id = uuid4()
        try:
            await manager.get_conversation(user_id, non_existent_id)
            assert False, "Should have raised an exception for non-existent conversation"
        except Exception as e:
            print(f"Successfully caught error for non-existent conversation: {str(e)}")
            assert "not found" in str(e).lower(), "Exception should indicate conversation not found"

        # Try to get a non-existent message
        try:
            await manager.get_message(user_id, non_existent_id)
            assert False, "Should have raised an exception for non-existent message"
        except Exception as e:
            print(f"Successfully caught error for non-existent message: {str(e)}")
            assert "not found" in str(e).lower(), "Exception should indicate message not found"

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
    print("\nAll assertions passed successfully!")
