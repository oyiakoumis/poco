from models.extract_intent import IntentModel
from state import State


# Define the function that determines whether to continue or not
def should_convert_to_intent_model(state: State):
    messages = state.messages
    last_message = messages[-1]
    # If there is only one tool call and it is the IntentModel tool call we convert the response to IntentModel
    if len(last_message.tool_calls) == 1 and last_message.tool_calls[0]["name"] == IntentModel.__name__:
        return "convert"
    # Otherwise we will use the tool node again
    else:
        return "extract"
