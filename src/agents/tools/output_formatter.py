from typing import Annotated, List

from langchain_core.messages import ToolMessage
from langchain_core.tools import InjectedToolCallId, tool
from langgraph.graph import END
from langgraph.types import Command

from agents.models.components import Component, FormattedResponse


@tool
def output_formatter(components: List[Component], tool_call_id: Annotated[str, InjectedToolCallId]):
    "Format query results into UI-friendly JSON responses with appropriate UI components"

    return Command(
        graph=Command.PARENT,
        goto=END,
        update={
            "messages": [
                ToolMessage(FormattedResponse(response=components).model_dump_json(), tool_call_id=tool_call_id),
            ]
        },
    )
