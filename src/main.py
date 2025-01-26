from typing import Any, Dict, Set

from dotenv import load_dotenv
from langchain.schema import HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.runnables import RunnableConfig

from models.extract_intent import IntentModel
from nodes.assistant import get_assistant_node, assistant_primary_tools
from nodes.utils import create_convert_to_model_node
from routes.route_assistant import route_assistant
from routes.route_intent_extractor import route_intent_extractor
from state import State
from nodes.extract_intent import entering_extract_intent_node, get_intent_extractor_node
from nodes.extract_intent import extract_intent_tools

load_dotenv()


def _print_event(event: Dict[str, Any], printed_ids: Set[str], max_length: int = 1500) -> None:
    """
    Processes and prints information about an event from the graph.

    Args:
        event (Dict[str, Any]): The event dictionary containing dialog state and messages.
        printed_ids (Set[str]): A set of message IDs that have already been printed.
        max_length (int): Maximum length of the message to print. Longer messages will be truncated.
    """
    current_state = event.get("dialog_state")
    if current_state:
        print(f"Currently in: {current_state[-1]}")

    message = event.get("messages")
    if message:
        if isinstance(message, list):
            message = message[-1]  # Get the latest message
        if message.id not in printed_ids:
            msg_repr = message.pretty_repr(html=True)
            if len(msg_repr) > max_length:
                msg_repr = f"{msg_repr[:max_length]} ... (truncated)"
            print(msg_repr)
            printed_ids.add(message.id)


def main() -> None:
    # Initialize workflow
    workflow = StateGraph(State)

    # Add nodes
    workflow.add_node("assistant", get_assistant_node())
    workflow.add_node("assistant_primary_tools", ToolNode(assistant_primary_tools))
    workflow.add_node("entering_extract_intent", entering_extract_intent_node)
    workflow.add_node("intent_extractor", get_intent_extractor_node())
    workflow.add_node("intent_extractor_tools", ToolNode(extract_intent_tools))
    workflow.add_node("convert_to_intent_model", create_convert_to_model_node(IntentModel))

    # Add edges
    workflow.set_entry_point("assistant")
    workflow.add_conditional_edges(
        "assistant",
        route_assistant,
        {"assistant_primary_tools": "assistant_primary_tools", "enter_extract_intent": "entering_extract_intent", "terminate": END},
    )
    workflow.add_edge("assistant_primary_tools", "assistant")
    workflow.add_edge("entering_extract_intent", "intent_extractor")
    workflow.add_conditional_edges(
        "intent_extractor", route_intent_extractor, {"convert_intent": "convert_to_intent_model", "extract_intent": "intent_extractor_tools"}
    )
    workflow.add_edge("intent_extractor_tools", "intent_extractor")
    workflow.add_edge("convert_to_intent_model", END)

    # Initialize memory and graph
    memory = MemorySaver()
    graph = workflow.compile(checkpointer=memory)

    # Configuration for the graph
    config = RunnableConfig(configurable={"thread_id": "1"}, recursion_limit=10)

    # Track already printed message IDs
    printed_ids: Set[str] = set()

    # Stream events from the graph
    initial_message = HumanMessage(content="Show me all my todos for today")
    events = graph.stream({"messages": [initial_message]}, config, stream_mode="values")

    # Process and print each event
    for event in events:
        _print_event(event, printed_ids)


if __name__ == "__main__":
    main()
