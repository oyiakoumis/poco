from typing import Any, Dict, Optional, Tuple
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.syntax import Syntax
from langgraph.graph.message import AnyMessage

console = Console()


def format_namespace(namespace: Tuple[str]) -> str:
    return namespace[-1].split(":")[0] + " subgraph" if len(namespace) > 0 else "parent graph"


def format_markdown_content(content: str) -> Markdown:
    return Markdown(content)


def print_event(namespace: Tuple[str], event: Dict[str, Any], max_length: int = 3000) -> None:
    node_name = list(event.keys())[0]
    message: AnyMessage = event[node_name]["messages"]

    # Handle ToolMessage or list of messages
    if isinstance(message, list):
        message = message[-1]

    namespace_str = format_namespace(namespace)
    print_message(message, node_name, namespace_str, max_length)


def print_message(message: AnyMessage, origin: str, namespace: Optional[str] = None, max_length: int = 3000) -> None:
    # Get message representation with HTML formatting
    msg_repr = message.pretty_repr(html=True)

    # Handle truncation if needed
    if len(msg_repr) > max_length:
        truncated_indicator = f" ... (truncated {len(msg_repr) - max_length} characters)"
        msg_repr = f"{msg_repr[:max_length]}{truncated_indicator}"

    # Convert message to Rich Markdown if it contains markdown syntax
    if any(marker in msg_repr for marker in ["#", "```", "*", "_", "-"]):
        content = format_markdown_content(msg_repr)
    else:
        content = msg_repr

    # Create panel with enhanced styling
    panel = Panel(
        content,
        title=f"Message from [magenta]{origin}[/magenta]" + (f" in [blue]{namespace}[/blue]" if namespace else ""),
        style="white on black",
        border_style="bright_blue",
        padding=(1, 2),
    )

    # Print panel and add spacing
    console.print(panel)
    console.print("\n")
