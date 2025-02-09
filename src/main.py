import asyncio

from langchain.schema import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from motor.motor_asyncio import AsyncIOMotorClient

from agent.workflow import get_graph
from constants import DATABASE_CONNECTION_STRING
from document_store.dataset_manager import DatasetManager
from print_event import print_event, print_message


async def main():
    # Connect to the database
    client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
    client.get_io_loop = asyncio.get_running_loop
    
    try:
        db = await DatasetManager.setup(client)

        # Get the graph
        graph = get_graph(db)

        graph = graph.compile(checkpointer=MemorySaver())

        # Configuration for the graph
        config = RunnableConfig(configurable={"thread_id": "1", "user_id": "user_123"}, recursion_limit=10)

        for message in [HumanMessage(content="Show me my todo list")]:
            print_message(message, "Human")
            # Process and print each event
            async for namespace, event in graph.astream({"messages": [message]}, config, stream_mode="updates", subgraphs=True):
                print_event(namespace, event)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
