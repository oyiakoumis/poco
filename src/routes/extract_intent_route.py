from models.extract_intent import IntentModel
from state import State


def route_intent_extractor(state: State):
    messages = state.messages
    last_message = messages[-1]
    if len(last_message.tool_calls) == 1 and last_message.tool_calls[0]["name"] == IntentModel.__name__:
        return "convert_intent"
    else:
        return "extract_intent"
