from langchain.schema import HumanMessage
from langchain_core.runnables import RunnableConfig
from langchain_openai import OpenAIEmbeddings
from langgraph.checkpoint.memory import MemorySaver

from constants import DATABASE_CONNECTION_STRING
from database_manager.collection_definition import CollectionDefinition
from database_manager.schema_field import DataType, SchemaField
from print_event import print_event, print_message


def main() -> None:
    from database_manager.connection import Connection
    from database_manager.database import Database

    connection = Connection(DATABASE_CONNECTION_STRING)
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

    database = Database("task_manager", connection, embeddings)
    database.connect(restart=True)

    schema = {
        "title": SchemaField("title", "the title of the task", DataType.STRING, required=True),
        "description": SchemaField("description", "the description of the task", DataType.STRING, required=False),
    }
    collection = database.create_collection("tasks", schema, description="A collection of tasks")
    definition = CollectionDefinition("tasks", database.registry, "A collection of tasks", schema)
    retrieved_collections = database.registry.search_similar_collections(definition)

    document = collection.insert_one({"title": "Task 1", "description": "Description 1"})
    retrieved_document = collection.search_similar(document)


if __name__ == "__main__":
    main()
