from langgraph.graph import StateGraph

from agents.assistant import Assistant
from document_store.dataset_manager import DatasetManager
from state import State


def get_graph(db: DatasetManager) -> StateGraph:
    graph = StateGraph(State)

    # Add nodes
    graph.add_node("assistant", Assistant(db))

    # Add Edges
    graph.set_entry_point("assistant")
    graph.set_finish_point("assistant")

    return graph
