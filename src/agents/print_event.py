from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union

from langgraph.graph.message import AnyMessage
from rich.console import Console
from rich.panel import Panel

console = Console()


@dataclass
class EventContent:
    """Container for event content and metadata."""

    content: Union[AnyMessage, Dict[str, Any], list[AnyMessage]]
    node_name: str
    namespace: str
    is_structured: bool = False


def format_namespace(namespace: Tuple[str]) -> str:
    """Format namespace for display."""
    return namespace[-1].split(":")[0] + " subgraph" if namespace else "parent graph"


def extract_event_content(event: Dict[str, Any]) -> EventContent:
    """Extract and validate content from event."""
    node_name = list(event.keys())[0]
    node_data = event[node_name]

    if "structured_response" in node_data:
        return EventContent(content=node_data["structured_response"], node_name=node_name, namespace="", is_structured=True)

    if "messages" not in node_data:
        raise ValueError(f"No valid content found in event from {node_name}")

    messages = node_data["messages"]
    if isinstance(messages, list):
        messages = messages[-1]

    return EventContent(content=messages, node_name=node_name, namespace="", is_structured=False)


def create_panel(content: str, title: str) -> Panel:
    """Create a formatted panel with content."""
    return Panel(content, title=title, style="white on black")


def format_title(node_name: str, namespace: Optional[str] = None) -> str:
    """Format panel title with node name and optional namespace."""
    base_title = f"[magenta]{node_name}[/magenta]"

    now =  datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if namespace:
        return f"Message from {base_title} in [blue]{namespace}[/blue] at {now}"
    return f"Message from {base_title} at {now}"


def truncate_content(content: str, max_length: int) -> str:
    """Truncate content if it exceeds max length."""
    if len(content) <= max_length:
        return content

    truncated_indicator = f" ... (truncated {len(content) - max_length} characters)"
    return f"{content[:max_length]}{truncated_indicator}"


def print_event(namespace: Tuple[str], event: Dict[str, Any], max_length: int = 3000) -> None:
    """Print event content with appropriate formatting."""
    try:
        event_content = extract_event_content(event)
        event_content.namespace = format_namespace(namespace)

        if event_content.is_structured:
            title = f"Structured Response from {event_content.node_name}"
            content = str(event_content.content)
        else:
            title = format_title(event_content.node_name, event_content.namespace)
            content = event_content.content.pretty_repr(html=True)

        content = truncate_content(content, max_length)
        panel = create_panel(content, title)

        console.print(panel)
        console.print("\n")  # Ensure spacing between updates

    except ValueError as e:
        console.print(f"[red]Error: {str(e)}[/red]\n")
