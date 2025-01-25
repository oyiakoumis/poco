from models.extract_intent import IntentModel
from state import State
from tools.extract_intent import ToExtractIntent


def should_enter_extract_intent(state: State):
    messages = state.messages
    last_message = messages[-1]
    if len(last_message.tool_calls) == 1 and last_message.tool_calls[0]["name"] == ToExtractIntent.__name__:
        return "enter_extract_intent"
    else:
        return "terminate"


def should_convert_to_intent_model(state: State):
    messages = state.messages
    last_message = messages[-1]
    if len(last_message.tool_calls) == 1 and last_message.tool_calls[0]["name"] == IntentModel.__name__:
        return "convert_intent"
    else:
        return "extract_intent"
