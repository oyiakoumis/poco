from langgraph.graph import MessagesState, StateGraph

from agent.assistant import Assistant
from document_store.dataset_manager import DatasetManager


def get_graph(db: DatasetManager) -> StateGraph:
    graph = StateGraph(MessagesState)

    # Add nodes
    graph.add_node("assistant", Assistant(db))

    # Add Edges
    graph.set_entry_point("assistant")

    return graph
