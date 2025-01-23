from langchain_core.messages import AnyMessage
from langchain_core.runnables import Runnable

from state import State


class Assistant:
    def __init__(self, runnable: Runnable, max_retries: int = 5):
        self.runnable = runnable
        self.max_retries = max_retries

    def __call__(self, state: State) -> State:
        retries = 0

        while retries < self.max_retries:
            try:
                result = self.runnable.invoke(state.messages)
            except Exception as e:
                raise RuntimeError(f"Failed to invoke runnable: {e}") from e

            if self._is_empty_response(result):
                # Append corrective message and update state
                state["messages"] = [*state.get("messages", []), {"role": "user", "content": "Respond with a real output."}]
                retries += 1
            else:
                break
        else:
            raise RuntimeError(f"Maximum retries ({self.max_retries}) reached without valid response")

        return {"messages": result}

    def _is_empty_response(self, result: AnyMessage) -> bool:
        """Check if the response should be considered empty."""
        if result.tool_calls:
            return False

        content = result.content
        if not content:
            return True

        if isinstance(content, list):
            return not any(item.get("text") for item in content[:1] if isinstance(item, dict))  # Check first item only

        return False
