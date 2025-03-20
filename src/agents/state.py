from typing import Annotated, Any, Dict, Iterator, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from langgraph.prebuilt.chat_agent_executor import IsLastStep, RemainingSteps
from pydantic import BaseModel


class State(BaseModel):
    messages: Annotated[list[BaseMessage], add_messages]
    export_file_attachment: Optional[Dict[str, Any]] = None

    # The following fields are used by the langgraph react agent
    is_last_step: IsLastStep
    remaining_steps: RemainingSteps

    def __getitem__(self, key):
        """Enable dictionary-like access with square brackets."""
        return getattr(self, key)

    def __setitem__(self, key, value):
        """Enable dictionary-like setting with square brackets."""
        setattr(self, key, value)

    def get(self, key, default=None):
        """Dictionary-like get method with default value."""
        try:
            return getattr(self, key)
        except AttributeError:
            return default

    def keys(self) -> Iterator[str]:
        """Return an iterator over the model's field names."""
        return self.model_fields.keys()

    def items(self):
        """Return (key, value) pairs of the model's fields."""
        return {k: getattr(self, k, None) for k in self.model_fields}.items()

    def values(self):
        """Return an iterator over the model's values."""
        return [getattr(self, k, None) for k in self.model_fields]
