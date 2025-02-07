from typing import Annotated

from pydantic import BaseModel
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages


class State(BaseModel):
    messages: Annotated[list[AnyMessage], add_messages]
