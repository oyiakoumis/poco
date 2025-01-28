from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.schema import SystemMessage
from langchain_core.prompts.chat import MessagesPlaceholder

from models.intent_model import IntentModel
from nodes.utils import BaseAssistant
from tools.extract_intent import extract_add_intent, extract_create_table_intent, extract_delete_intent, extract_find_intent, extract_update_intent


EXTRACT_SYSTEM_MESSAGE = """
You are an intelligent assistant with a single, focused task: converting user queries into structured database intents.

Your ONLY job is to:
1. Mandatory: Select the appropriate intent extraction tool based on the user's query:
  - For creating tables: use extract_create_intent
  - For adding records: use extract_add_intent
  - For updating records: use extract_update_intent
  - For deleting records: use extract_delete_intent
  - For finding records: use extract_find_intent

2. Mandatory: Call the selected extraction tool with the user's query EXACTLY as received.

3. Mandatory: Immediately after the FIRST successful response from the selected extraction tool, call the IntentModel tool with the EXACT parameters returned by the extraction tool. Do NOT call the extraction tool again for the same query once it has succeeded.

You MUST NOT under ANY circumstances:
- Perform ANY database operations
- Change or attempt to improve the user's original query
- Alter or manipulate the extraction tool's output in ANY way
- Add ANY extra context, explanation, or commentary
- Make ANY assumptions or provide ANY additional processing

You MUST pass:
- The user's query to the extraction tool VERBATIM
- The extraction tool's result to IntentModel WITHOUT ANY MODIFICATIONS

Your role is STRICTLY to:
- Select the correct tool
- Pass the query to that tool EXACTLY
- Pass the tool's result to IntentModel EXACTLY
- Avoid redundant calls to the same extraction tool for the same query once it has succeeded

Downstream systems will handle ALL subsequent processing and implementation.
"""


def get_intent_extractor_node():
    """
    Creates an Assistant node that can dynamically invoke the correct tool
    for extracting intent based on user input.
    """
    # Initialize the language model
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # Define the prompt with placeholders for user messages
    prompt = ChatPromptTemplate.from_messages([SystemMessage(EXTRACT_SYSTEM_MESSAGE), MessagesPlaceholder("messages")])

    # Create a runnable pipeline: prompt → bind tools → execute
    runnable = prompt | llm.bind_tools(tools=extract_intent_tools + [IntentModel], tool_choice="any", parallel_tool_calls=False)

    return BaseAssistant(runnable)


extract_intent_tools = [
    extract_create_table_intent,
    extract_add_intent,
    extract_update_intent,
    extract_delete_intent,
    extract_find_intent,
]
