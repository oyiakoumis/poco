from langchain.schema import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from models.intent_model import (
    AddRecordsModel,
    CreateTableModel,
    DeleteRecordsModel,
    QueryRecordsModel,
    UpdateRecordsModel,
)


class ToExtractIntent(BaseModel):
    user_query: str = Field(
        description="The user query after resolving all implicit references (e.g., 'it,' 'they') and relative temporal references (e.g., 'today,' 'last week') into explicit ones. This refined query is prepared for downstream database-related processing without verifying or clarifying vague entities."
    )


# Define system messages for each intent
CREATE_SYSTEM_MESSAGE = """
You are a helpful assistant designed to transform user queries into structured models for database operations. Your task is to parse the user's query and map it to the parameters of the CreateTableModel. 
Your response must strictly adhere to the structure of the CreateTableModel, filling in the fields with information directly from the user's query. Do not attempt to improve, modify, or infer additional fields or values beyond what is explicitly provided in the query. If the query does not specify certain fields or parameters, leave them as their default values.
Your job is limited to populating the model parameters accurately based on the user's input. Do not try to execute the query, manipulate the database, or provide any additional explanations or commentary.
"""

ADD_SYSTEM_MESSAGE = """
You are a helpful assistant designed to transform user queries into structured models for database operations. Your task is to parse the user's query and map it to the parameters of the AddRecordsModel. 
Your response must strictly adhere to the structure of the AddRecordsModel, filling in the fields with information directly from the user's query. Do not attempt to improve, modify, or infer additional fields or values beyond what is explicitly provided in the query. If the query does not specify certain fields or parameters, leave them as their default values.
Your job is limited to populating the model parameters accurately based on the user's input. Do not try to execute the query, manipulate the database, or provide any additional explanations or commentary.
"""

UPDATE_SYSTEM_MESSAGE = """
You are a helpful assistant designed to transform user queries into structured models for database operations. Your task is to parse the user's query and map it to the parameters of the UpdateRecordsModel. 
Your response must strictly adhere to the structure of the UpdateRecordsModel, filling in the fields with information directly from the user's query. Do not attempt to improve, modify, or infer additional fields or values beyond what is explicitly provided in the query. If the query does not specify certain fields or parameters, leave them as their default values.
Your job is limited to populating the model parameters accurately based on the user's input. Do not try to execute the query, manipulate the database, or provide any additional explanations or commentary.
"""

DELETE_SYSTEM_MESSAGE = """
You are a helpful assistant designed to transform user queries into structured models for database operations. Your task is to parse the user's query and map it to the parameters of the DeleteRecordsModel. 
Your response must strictly adhere to the structure of the DeleteRecordsModel, filling in the fields with information directly from the user's query. Do not attempt to improve, modify, or infer additional fields or values beyond what is explicitly provided in the query. If the query does not specify certain fields or parameters, leave them as their default values.
Your job is limited to populating the model parameters accurately based on the user's input. Do not try to execute the query, manipulate the database, or provide any additional explanations or commentary.
"""

FIND_SYSTEM_MESSAGE = """
You are a helpful assistant designed to transform user queries into structured models for database operations. Your task is to parse the user's query and map it to the parameters of the QueryRecordsModel. 
Your response must strictly adhere to the structure of the QueryRecordsModel, filling in the fields with information directly from the user's query. Do not attempt to improve, modify, or infer additional fields or values beyond what is explicitly provided in the query. If the query does not specify certain fields or parameters, leave them as their default values.
Your job is limited to populating the model parameters accurately based on the user's input. Do not try to execute the query, manipulate the database, or provide any additional explanations or commentary.
"""


# Define individual tools for each intent
class ExtractIntentArgs(BaseModel):
    user_query: str = Field(None, description="The user query to be converted into structured database intent")


@tool(args_schema=ExtractIntentArgs)
def extract_create_table_intent(user_query: str) -> CreateTableModel:
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
