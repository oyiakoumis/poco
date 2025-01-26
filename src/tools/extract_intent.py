from langchain.schema import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from models.extract_intent import (
    AddRecordsModel,
    CreateTableModel,
    DeleteRecordsModel,
    QueryRecordsModel,
    UpdateRecordsModel,
)


class ToExtractIntent(BaseModel):
    user_request: str = Field(
        description="The user query after resolving all implicit references (e.g., 'it,' 'they') and relative temporal references (e.g., 'today,' 'last week') into explicit ones. This refined query is prepared for downstream database-related processing without verifying or clarifying vague entities."
    )


# Define system messages for each intent
CREATE_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for creating tables.
"""

ADD_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for adding records.
"""

UPDATE_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for updating records.
"""

DELETE_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for deleting records.
"""

FIND_SYSTEM_MESSAGE = """
You are an intelligent assistant that extracts a structured intent model for querying records.
"""


# Define individual tools for each intent
class ExtractIntentArgs(BaseModel):
    user_query: str = Field(None, description="The user query to be converted into structured database intent")


@tool(args_schema=ExtractIntentArgs)
def extract_create_intent(user_query: str) -> CreateTableModel:
    """Extracts a structured intent model for creating tables."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=CREATE_SYSTEM_MESSAGE), HumanMessage(content=user_query)]
    structured_llm = llm.with_structured_output(CreateTableModel)
    return structured_llm.invoke(messages)


@tool(args_schema=ExtractIntentArgs)
def extract_add_intent(user_query: str) -> AddRecordsModel:
    """Extracts a structured intent model for adding records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=ADD_SYSTEM_MESSAGE), HumanMessage(content=user_query)]
    structured_llm = llm.with_structured_output(AddRecordsModel)
    return structured_llm.invoke(messages)


@tool(args_schema=ExtractIntentArgs)
def extract_update_intent(user_query: str) -> UpdateRecordsModel:
    """Extracts a structured intent model for updating records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=UPDATE_SYSTEM_MESSAGE), HumanMessage(content=user_query)]
    structured_llm = llm.with_structured_output(UpdateRecordsModel)
    return structured_llm.invoke(messages)


@tool(args_schema=ExtractIntentArgs)
def extract_delete_intent(user_query: str) -> DeleteRecordsModel:
    """Extracts a structured intent model for deleting records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=DELETE_SYSTEM_MESSAGE), HumanMessage(content=user_query)]
    structured_llm = llm.with_structured_output(DeleteRecordsModel)
    return structured_llm.invoke(messages)


@tool(args_schema=ExtractIntentArgs)
def extract_find_intent(user_query: str) -> QueryRecordsModel:
    """Extracts a structured intent model for querying records."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    messages = [SystemMessage(content=FIND_SYSTEM_MESSAGE), HumanMessage(content=user_query)]
    structured_llm = llm.with_structured_output(QueryRecordsModel)
    return structured_llm.invoke(messages)
