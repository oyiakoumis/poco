import asyncio

from langchain.schema import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver
from motor.motor_asyncio import AsyncIOMotorClient

from agents.workflow import get_graph
from constants import DATABASE_CONNECTION_STRING
from document_store.dataset_manager import DatasetManager
from print_event import print_event


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
        config = RunnableConfig(configurable={"thread_id": "1", "user_id": "user_123", "time_zone": "UTC", "first_day_of_the_week": 0}, recursion_limit=25)

        messages = [HumanMessage(content="What do I need to do today?")]

        # Print human message using print_event
        print_event((), {"Human": {"messages": messages}})

        # Process and print each event
        async for namespace, event in graph.astream({"messages": messages}, config, stream_mode="updates", subgraphs=True):
            print_event(namespace, event)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
