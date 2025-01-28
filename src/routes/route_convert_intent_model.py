from langgraph.graph import StateGraph, END

from state import QueryProcessorState


def route_convert_intent_model(state: QueryProcessorState):
    intent = state.intent.intent
    if intent == "create_table":
        return "database_operator"
    elif intent == "add":
        return END
    elif intent == "update":
        return END
    elif intent == "delete":
        return END
    elif intent == "query":
        return END
