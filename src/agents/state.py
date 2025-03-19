from typing import Annotated, Dict, Any, Optional

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel


class State(BaseModel):
    messages: Annotated[list[AnyMessage], add_messages]
    export_file_attachment: Optional[Dict[str, Any]] = None
