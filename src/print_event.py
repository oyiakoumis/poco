from typing import Any, Dict, Optional, Tuple

from langgraph.graph.message import AnyMessage
from rich.console import Console
from rich.panel import Panel

console = Console()


def format_namespace(namespace):
    return namespace[-1].split(":")[0] + " subgraph" if len(namespace) > 0 else "parent graph"


def print_event(namespace: Tuple[str], event: Dict[str, Any], max_length: int = 3000) -> None:
    node_name = list(event.keys())[0]

    message: AnyMessage = event[node_name]["messages"]

    # Handle ToolMessage or list of messages
    if isinstance(message, list):
        message = message[-1]

    namespace_str = format_namespace(namespace)

    print_message(message, node_name, namespace_str, max_length)


def print_message(message: AnyMessage, origin: str, namespace: Optional[str] = None, max_length: int = 3000):
    msg_repr = message.pretty_repr(html=True)
    if len(msg_repr) > max_length:
        truncated_indicator = f" ... (truncated {len(msg_repr) - max_length} characters)"
        msg_repr = f"{msg_repr[:max_length]}{truncated_indicator}"

    panel = Panel(
        msg_repr,
        title=f"Message from [magenta]{origin}[/magenta]" + (f" in [blue]{namespace}[/blue]" if namespace else ""),
        style="white on black",
    )
    console.print(panel)
    console.print("\n")  # Ensure spacing between updates
