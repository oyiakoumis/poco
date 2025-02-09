from typing import Annotated

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel


class State(BaseModel):
    messages: Annotated[list[AnyMessage], add_messages]
