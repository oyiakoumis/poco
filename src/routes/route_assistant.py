from langgraph.graph import END

from nodes.assistant import assistant_primary_tools
from state import MessagesState
from tools.extract_intent import ToExtractIntent


def route_assistant(state: MessagesState):
    messages = state.messages
    last_message = messages[-1]
    if len(last_message.tool_calls) == 1 and last_message.tool_calls[0]["name"] == ToExtractIntent.__name__:
        return "process_query_node"
    elif len(last_message.tool_calls) == 1 and last_message.tool_calls[0]["name"] in [tool.name for tool in assistant_primary_tools]:
        return "assistant_primary_tools"
    else:
        return END
