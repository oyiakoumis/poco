from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.schema import SystemMessage, HumanMessage
from langchain_core.prompts.chat import MessagesPlaceholder
from langchain_core.messages import ToolMessage

from database_connector import DatabaseConnector
from models.intent_model import (
    AddRecordsModel,
    CreateTableModel,
    DeleteRecordsModel,
    QueryRecordsModel,
    UpdateRecordsModel,
)
from nodes.utils import with_forced_response
from state import MessagesState, QueryProcessorState
from tools.database_tools import (
    AddRecordsOperator,
    CreateTableOperator,
    DeleteRecordsOperator,
    QueryRecordsOperator,
    UpdateRecordsOperator,
)

# System message template for table description generation
GENERATE_DESCRIPTION_SYSTEM_MESSAGE = """
Based on the given dictionary, generate a detailed description of the table's purpose, content, and the type of data it is intended to store.
"""


class DatabaseOperatorNode:
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initializes the operator node with database operators.
        """
        self.create_table = CreateTableOperator(db_connector)
        self.add_records = AddRecordsOperator(db_connector)
        self.update_records = UpdateRecordsOperator(db_connector)
        self.delete_records = DeleteRecordsOperator(db_connector)
        self.query_records = QueryRecordsOperator(db_connector)

    def __call__(self, state: QueryProcessorState) -> QueryProcessorState:
        """
        Processes the given state based on the detected intent.

        Args:
            state (QueryProcessorState): The state containing the user's intent and other information.

        Returns:
            QueryProcessorState: The updated state after processing the intent.
        """
        intent = state.intent

        if isinstance(intent, CreateTableModel):
            self._handle_create_table(intent)

        elif isinstance(intent, AddRecordsModel):
            self._handle_add_records(intent)

        elif isinstance(intent, UpdateRecordsModel):
            self._handle_update_records(intent)

        elif isinstance(intent, DeleteRecordsModel):
            self._handle_delete_records(intent)

        elif isinstance(intent, QueryRecordsModel):
            self._handle_query_records(intent)

        else:
            raise ValueError("Unknown intent type")

        return state

    def _handle_create_table(self, intent: CreateTableModel):
        """
        Handles the creation of a table based on the given intent.

        Args:
            intent (CreateTableModel): The intent containing details for table creation.
        """
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        prompt = ChatPromptTemplate.from_messages(
            [
                SystemMessage(GENERATE_DESCRIPTION_SYSTEM_MESSAGE),
                MessagesPlaceholder("messages"),
            ]
        )

        runnable = prompt | llm
        table_description = with_forced_response(runnable, {"messages": MessagesState(messages=[HumanMessage(content=intent)])})

        result = self.create_table._run(
            name=intent.target_table,
            description=table_description,
            table_schema=intent.table_schema,
            indexes=intent.indexes,
        )

        return ToolMessage(content=result["message"])

    def _handle_add_records(self, intent: AddRecordsModel):
        """
        Handles the addition of records based on the given intent.
        """
        result = self.add_records._run(collection_name=intent.collection_name, documents=intent.documents)
        return ToolMessage(content=result["message"])

    def _handle_update_records(self, intent: UpdateRecordsModel):
        """
        Handles updating records based on the given intent.
        """
        result = self.update_records._run(collection_name=intent.collection_name, query=intent.query, updates=intent.updates)
        return ToolMessage(content=result["message"])

    def _handle_delete_records(self, intent: DeleteRecordsModel):
        """
        Handles the deletion of records based on the given intent.
        """
        result = self.delete_records._run(collection_name=intent.collection_name, query=intent.query)
        return ToolMessage(content=result["message"])

    def _handle_query_records(self, intent: QueryRecordsModel):
        """
        Handles querying records based on the given intent.
        """
        result = self.query_records._run(collection_name=intent.collection_name, query=intent.query)
        return ToolMessage(content=result["message"])
