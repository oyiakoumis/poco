from langchain.schema import SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.prompts.chat import MessagesPlaceholder
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from database_connector import DatabaseConnector
from state import MessagesState
from tools.database_tools import AddRecordsOperator, CreateTableOperator, DeleteRecordsOperator, FindTableOperator, QueryRecordsOperator, UpdateRecordsOperator
from tools.extract_intent import (
    extract_add_intent,
    extract_create_table_intent,
    extract_delete_intent,
    extract_find_intent,
    extract_update_intent,
)
from tools.resolve_temporal_reference import resolve_temporal_reference

ASSISTANT_SYSTEM_MESSAGE = """
You are a specialized AI assistant with two primary functions: handling database-related queries and providing standard user assistance.

## 1. DATABASE OPERATION MODE
When a user query involves a database transaction, you MUST as the first step perform the following actions:
- Resolve all implicit and ambiguous references using the conversation's context.
- Use `resolve_temporal_reference` for any temporal expressions unless they are already explicit dates or ranges.
- **For operations that require an existing table (add, update, delete, query):**
  - Always use `find_table_operator` to validate the actual table name and schema **before extracting intent**.

### Intent Extraction
After fully resolving the user query and for ANY database operation, select the appropriate intent extraction tool and pass the fully resolved query to the selected tool:
  - `extract_create_intent` for table creation
  - `extract_add_intent` for adding records
  - `extract_update_intent` for updating records
  - `extract_delete_intent` for deleting records
  - `extract_find_intent` for retrieving records

### Database Execution
Once the structured intent is extracted:
- **Ensure all field names and types match the table schema.**
- Use the appropriate database operator:
  - `create_table_operator` for creating a table.
  - `add_records_operator` for adding new records.
  - `update_records_operator` for modifying existing records.
  - `delete_records_operator` for removing records.
  - `query_records_operator` for retrieving records.

### Clarification Protocol:
- ONLY ask for clarification if a temporal reference is ambiguous or an implicit reference lacks context.
- Ensure queries are transformed into structured database instructions with minimal user intervention.

## 2. STANDARD INTERACTION MODE
For non-database queries, offer comprehensive, user-friendly assistance:
- Act as a helpful, context-aware assistant.
- Provide detailed and relevant assistance without ANY tool usage.
"""


class Assistant:
    def __init__(self, db_connector: DatabaseConnector):
        self.tools = [
            resolve_temporal_reference,
            extract_create_table_intent,
            extract_add_intent,
            extract_update_intent,
            extract_delete_intent,
            extract_find_intent,
            CreateTableOperator(db_connector),
            FindTableOperator(db_connector),
            AddRecordsOperator(db_connector),
            UpdateRecordsOperator(db_connector),
            DeleteRecordsOperator(db_connector),
            QueryRecordsOperator(db_connector),
        ]

    def __call__(self, state: MessagesState):
        # Initialize the language model
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

        runnable = create_react_agent(llm, self.tools)

        response = runnable.invoke({"messages": [SystemMessage(ASSISTANT_SYSTEM_MESSAGE)] + state.messages})

        return {"messages": response["messages"]}
