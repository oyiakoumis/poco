import os

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

from agents.action_agent import Action
from agents.collection_router import CollectionRouter
from agents.document_router import DocumentRouter
from agents.query_processor import QueryProcessor
from constants import DATABASE_CONNECTION_STRING
from database_manager.document_db import DocumentDB
from models.agent_models import AgentResponse
from workflow.graph_manager import process_query

# Load environment variables
load_dotenv()


class NaturalLanguageDB:
    """Main interface for natural language database interactions"""

    def __init__(self):
        # Initialize MongoDB connection for document storage only
        client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
        database = client.get_database("natural_language_db")

        # Initialize core components
        self.db = DocumentDB(database)

        # Initialize agents
        self.query_processor = QueryProcessor()
        self.collection_router = CollectionRouter(self.db)
        self.document_router = DocumentRouter(self.db)
        self.action_agent = Action(self.db)

    async def process_message(self, user_id: str, message: str) -> AgentResponse:
        """Process a natural language message"""
        try:
            # Process the query through the agent workflow
            response = await process_query(query=message, user_id=user_id)

            return response

        except Exception as e:
            return AgentResponse(success=False, message=f"Failed to process message: {str(e)}", error=str(e))


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
