from langchain.schema import HumanMessage
from langchain_core.messages import AnyMessage, ToolMessage
from langchain_core.runnables import Runnable
from pydantic import BaseModel

from state import QueryProcessorState


def with_forced_response(runnable: Runnable, messages: list[AnyMessage], max_retries: int = 5):
    retries = 0
    while retries < max_retries:
        try:
            result = runnable.invoke(messages)
        except Exception as e:
            raise RuntimeError(f"Failed to invoke runnable: {e}") from e

        if _is_empty_response(result):
            # Append corrective message and update state
            messages = messages + [HumanMessage(content="Respond with a real output.")]
            retries += 1
        else:
            return result
    else:
        raise RuntimeError(f"Maximum retries ({max_retries}) reached without valid response")


def _is_empty_response(result: AnyMessage) -> bool:
    """Check if the response should be considered empty."""
    if result.tool_calls:
        return False

    content = result.content
    if not content:
        return True

    if isinstance(content, list):
        return not any(item.get("text") for item in content[:1] if isinstance(item, dict))  # Check first item only

    return False


def create_convert_to_model_node(Model: BaseModel):
    def convert_to_model_node(state: QueryProcessorState):
        # Construct the final answer from the arguments of the last tool call
        tool_call = state.messages[-1].tool_calls[0]
        intent = Model(**tool_call["args"])

        tool_message = ToolMessage(content=f"Succesfully converted to {Model.__name__}", tool_call_id=tool_call["id"])
        # We return the final answer
        return {"intent": intent, "messages": [tool_message]}

    return convert_to_model_node
