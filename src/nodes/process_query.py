from langchain.schema import HumanMessage
from langgraph.graph.state import CompiledStateGraph
from langchain_core.messages import ToolMessage

from state import MessagesState


def get_process_query_node(query_processor_graph: CompiledStateGraph) -> dict:
    def process_query_node(state: MessagesState) -> dict:
        tool_call = state.messages[-1].tool_calls[-1]
        tool_call_id = tool_call["id"]
        user_query = tool_call["args"]["user_query"]

        result = query_processor_graph.invoke({"user_query": user_query, "messages": HumanMessage(content=user_query)})

        return {
            "messages": [
                ToolMessage(
                    content=result["messages"][-1],
                    tool_call_id=tool_call_id,
                )
            ],
        }

    return process_query_node
