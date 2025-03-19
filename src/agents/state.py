from typing import Annotated, Dict, Any, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.prebuilt.chat_agent_executor import IsLastStep, RemainingSteps
from langgraph.graph.message import add_messages


class State(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    export_file_attachment: Optional[Dict[str, Any]] = None

    # The following fields are used by the langgraph react agent
    is_last_step: IsLastStep
    remaining_steps: RemainingSteps
