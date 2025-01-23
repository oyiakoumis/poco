from langchain.schema import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

from models.extract_intent import (
    AddRecordsModel,
    CreateTableModel,
    DeleteRecordsModel,
    QueryRecordsModel,
    UpdateRecordsModel,
)

# Define system messages for each intent
CREATE_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for creating tables.
The model includes the following fields:
- intent: Always 'create'.
- target_table: The name of the table to create.
- schema: The schema of the table, including field names, types, nullable status, and options for select fields.
"""

ADD_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for adding records.
The model includes the following fields:
- intent: Always 'add'.
- target_table: The name of the table to add records to.
- records: A list of records to add, where each record is a dictionary of field-value pairs.
"""

UPDATE_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for updating records.
The model includes the following fields:
- intent: Always 'update'.
- target_table: The name of the table to update.
- records: A dictionary of field-value pairs to update.
- conditions: A list of conditions to filter the records to update.
"""

DELETE_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for deleting records.
The model includes the following fields:
- intent: Always 'delete'.
- target_table: The name of the table to delete records from.
- conditions: A list of conditions to identify the records to delete.
"""

FIND_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for querying records.
The model includes the following fields:
- intent: Always 'find'.
- target_table: The name of the table to query records from.
- conditions: A list of conditions to filter records (optional).
- query_fields: A list of fields to retrieve (optional).
- limit: Maximum number of records to retrieve (optional).
- order_by: A list of fields to sort results (optional).
"""


# Define individual tools for each intent
@tool
def extract_create_intent(user_input: str) -> CreateTableModel:
    """Extracts a structured intent model for creating tables."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=CREATE_SYSTEM_MESSAGE), HumanMessage(content=user_input)]
    structured_llm = llm.with_structured_output(CreateTableModel)
    return structured_llm.invoke(messages)


@tool
def extract_add_intent(user_input: str) -> AddRecordsModel:
    """Extracts a structured intent model for adding records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=ADD_SYSTEM_MESSAGE), HumanMessage(content=user_input)]
    structured_llm = llm.with_structured_output(AddRecordsModel)
    return structured_llm.invoke(messages)


@tool
def extract_update_intent(user_input: str) -> UpdateRecordsModel:
    """Extracts a structured intent model for updating records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=UPDATE_SYSTEM_MESSAGE), HumanMessage(content=user_input)]
    structured_llm = llm.with_structured_output(UpdateRecordsModel)
    return structured_llm.invoke(messages)


@tool
def extract_delete_intent(user_input: str) -> DeleteRecordsModel:
    """Extracts a structured intent model for deleting records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=DELETE_SYSTEM_MESSAGE), HumanMessage(content=user_input)]
    structured_llm = llm.with_structured_output(DeleteRecordsModel)
    return structured_llm.invoke(messages)


@tool
def extract_find_intent(user_input: str) -> QueryRecordsModel:
    """Extracts a structured intent model for querying records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=FIND_SYSTEM_MESSAGE), HumanMessage(content=user_input)]
    structured_llm = llm.with_structured_output(QueryRecordsModel)
    return structured_llm.invoke(messages)


extract_intent_tools = [
    extract_add_intent,
    extract_create_intent,
    extract_update_intent,
    extract_delete_intent,
    extract_find_intent,
]
