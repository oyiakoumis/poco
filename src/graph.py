from langgraph.graph import StateGraph, END
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import ToolNode

from database_connector import DatabaseConnector
from models.intent_model import IntentModel
from nodes.assistant import get_assistant_node, assistant_primary_tools
from nodes.database_operator import DatabaseOperatorNode
from nodes.utils import create_convert_to_model_node
from routes.route_assistant import route_assistant
from routes.route_convert_intent_model import route_convert_intent_model
from routes.route_intent_extractor import route_intent_extractor
from state import MessagesState, QueryProcessorState
from nodes.process_query import get_process_query_node
from nodes.extract_intent import get_intent_extractor_node, extract_intent_tools


def get_graph(query_processor_graph: CompiledStateGraph) -> StateGraph:
    graph = StateGraph(MessagesState)
    # Add nodes
    graph.add_node("assistant", get_assistant_node())
    graph.add_node("assistant_primary_tools", ToolNode(assistant_primary_tools))
    graph.add_node("process_query_node", get_process_query_node(query_processor_graph))

    # Add edges
    graph.set_entry_point("assistant")
    graph.add_conditional_edges("assistant", route_assistant)
    graph.add_edge("assistant_primary_tools", "assistant")
    graph.add_edge("process_query_node", "assistant")

    return graph


def get_query_processor_graph(db_connector: DatabaseConnector) -> CompiledStateGraph:
    graph = StateGraph(QueryProcessorState)
    graph.add_node("intent_extractor", get_intent_extractor_node())
    graph.add_node("intent_extractor_tools", ToolNode(extract_intent_tools))
    graph.add_node("convert_to_intent_model", create_convert_to_model_node(IntentModel))
    graph.add_node("database_operator", DatabaseOperatorNode(db_connector))

    # Add edges
    graph.set_entry_point("intent_extractor")
    graph.add_conditional_edges("intent_extractor", route_intent_extractor)
    graph.add_edge("intent_extractor_tools", "intent_extractor")
    graph.add_conditional_edges("convert_to_intent_model", route_convert_intent_model)
    graph.add_edge("database_operator", END)

    return graph.compile()
