import json
from typing import Annotated, List

from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.graph import END
from langgraph.types import Command

from agents.models.components import Component


@tool
def output_formatter(components: List[Component]):
    "Format query results into UI-friendly JSON responses with appropriate UI components"

    return Command(
        graph=Command.PARENT,
        goto=END,
        update={"messages": [AIMessage(json.dumps([component.model_dump() for component in components]))]},
    )
