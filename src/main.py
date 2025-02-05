import os
from typing import Dict, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

from agents.query_processor import QueryProcessor
from agents.collection_router import CollectionRouter
from agents.document_router import DocumentRouter
from agents.action_agent import Action
from database_manager.document_db import DocumentDB
from managers.conversation_manager import ConversationManager
from models.agent_models import AgentResponse, ConversationContext
from workflow.graph_manager import process_query

# Load environment variables
load_dotenv()

# Set default environment variables for testing
if not os.getenv("DATABASE_CONNECTION_STRING"):
    os.environ["DATABASE_CONNECTION_STRING"] = "mongodb://localhost:27017"
if not os.getenv("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = "your-api-key-here"


class NaturalLanguageDB:
    """Main interface for natural language database interactions"""

    def __init__(self):
        # Initialize MongoDB connection
        connection_string = os.getenv("DATABASE_CONNECTION_STRING")
        if not connection_string:
            raise ValueError("DATABASE_CONNECTION_STRING environment variable not set")

        client = AsyncIOMotorClient(connection_string)
        database = client.get_database("natural_language_db")

        # Initialize core components
        self.db = DocumentDB(database)
        self.conversation_manager = ConversationManager(database.get_collection("conversation_contexts"))

        # Initialize agents
        self.query_processor = QueryProcessor()
        self.collection_router = CollectionRouter(self.db)
        self.document_router = DocumentRouter(self.db)
        self.action_agent = Action(self.db)

    async def process_message(self, user_id: str, message: str) -> AgentResponse:
        """Process a natural language message"""
        try:
            # Get conversation context
            context = await self.conversation_manager.get_context(user_id)
            if not context:
                context = ConversationContext(user_id=user_id, conversation_history=[], last_collection=None, last_document_ids=None)

            # Process the query through the agent workflow
            response = await process_query(query=message, context=context, user_id=user_id)

            # Update conversation context
            if response.success:
                await self.conversation_manager.update_context(
                    user_id=user_id,
                    query=message,
                    response=response.message,
                    collection_name=response.data.get("collection_name") if response.data else None,
                    document_ids=response.data.get("document_ids") if response.data else None,
                )

            return response

        except Exception as e:
            return AgentResponse(success=False, message=f"Failed to process message: {str(e)}", error=str(e))

    async def clear_context(self, user_id: str) -> None:
        """Clear conversation context for a user"""
        await self.conversation_manager.clear_context(user_id)


# Example usage
async def main():
    # Initialize the natural language database interface
    nldb = NaturalLanguageDB()

    # Example queries
    queries = [
        "Add eggs to my list of groceries",
        "Mark all my tasks for today as completed",
        "What's on my grocery list?",
        "Add a task to review the project tomorrow",
        "Show me all tasks due this week",
    ]

    # Process each query
    user_id = "example_user"
    for query in queries:
        print(f"\nProcessing query: {query}")
        response = await nldb.process_message(user_id, query)
        print(f"Success: {response.success}")
        print(f"Message: {response.message}")
        if response.error:
            print(f"Error: {response.error}")
        if response.data:
            print(f"Data: {response.data}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
