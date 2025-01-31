from langchain.schema import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from langchain_openai import OpenAIEmbeddings

from constants import DATABASE_CONNECTION_STRING
from print_event import print_event, print_message


def main() -> None:
    from database_manager.database import Database
    from database_manager.connection import Connection

    connection = Connection(DATABASE_CONNECTION_STRING)
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

    database = Database("task_manager", connection, embeddings)

    collection = database.create_collection("tasks", {"title": "string", "description": "string"}, description="A collection of tasks")
    database.registry.search_similar_collections(collection, num_results=5, min_score=0.5)

    document = collection.insert_one({"title": "Task 1", "description": "Description 1"})
    collection.search_similar(document)


if __name__ == "__main__":
    main()
