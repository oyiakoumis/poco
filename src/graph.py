from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from database_connector import DatabaseConnector
from nodes.assistant import Assistant
from state import MessagesState


def get_graph(db_connector: DatabaseConnector) -> StateGraph:
    graph = StateGraph(MessagesState)

    # Add nodes
    graph.add_node("assistant", Assistant(db_connector))

    # Add Edges
    graph.set_entry_point("assistant")

    return graph
