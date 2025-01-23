from typing import Any, Dict, Set

from dotenv import load_dotenv
from langchain.schema import HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import tools_condition
from langgraph.prebuilt import ToolNode

from state import State
from nodes.extract_intent import get_intent_extractor_node
from tools.extract_intent import extract_intent_tools

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
    """
    Main function to set up and run the state graph with a human message.
    """
    # Initialize workflow
    workflow = StateGraph(State)

    # Add nodes
    workflow
    workflow.add_node("intent_extractor", get_intent_extractor_node())
    workflow.add_node("intent_extractor_tools", ToolNode(extract_intent_tools))

    # Add edges
    workflow.add_edge(START, "intent_extractor")
    workflow.add_conditional_edges("intent_extractor", tools_condition, {"tools": "intent_extractor_tools", END: END})
    workflow.add_edge("intent_extractor_tools", "intent_extractor")

    # Initialize memory and graph
    memory = MemorySaver()
    graph = workflow.compile(checkpointer=memory)

    # Configuration for the graph
    config = {"configurable": {"thread_id": "1"}}

    # Track already printed message IDs
    printed_ids: Set[str] = set()

    # Stream events from the graph
    initial_message = HumanMessage(content="Add 'buy milk' to my list of groceries.")
    events = graph.stream({"messages": [initial_message]}, config, stream_mode="values")

    # Process and print each event
    for event in events:
        _print_event(event, printed_ids)


if __name__ == "__main__":
    main()
