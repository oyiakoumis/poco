import asyncio

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph
from motor.motor_asyncio import AsyncIOMotorClient

from agents.assistant import Assistant
from agents.state import State
from database.document_store.dataset_manager import DatasetManager
from settings import settings


def create_graph(db: DatasetManager) -> StateGraph:
    """Create the graph with nodes and edges."""
    graph = StateGraph(State)

    # Add nodes
    graph.add_node("assistant", Assistant(db))

    # Add Edges
    graph.set_entry_point("assistant")
    graph.set_finish_point("assistant")

    return graph


async def setup_graph():
    """Setup database and create compiled graph."""
    # Connect to the database
    client = AsyncIOMotorClient(settings.database_connection_string)
    client.get_io_loop = asyncio.get_running_loop

    try:
        db = await DatasetManager.setup(client)
        graph = create_graph(db)
        return graph.compile(checkpointer=MemorySaver()), client
    except Exception as e:
        client.close()
        raise e


# For langgraph CLI - keeps client alive
async def get_compiled_graph():
    """Get compiled graph for langgraph CLI."""
    graph, _ = await setup_graph()
    return graph
