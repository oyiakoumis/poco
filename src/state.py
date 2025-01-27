from typing import Annotated, Optional, Union

from langgraph.graph.message import AnyMessage, add_messages
from pydantic import BaseModel, Field

from models.intent_model import AddRecordsModel, CreateTableModel, DeleteRecordsModel, QueryRecordsModel, UpdateRecordsModel


class MessagesState(BaseModel):
    messages: Annotated[list[AnyMessage], add_messages]


class QueryProcessorState(MessagesState):
    user_query: Optional[str] = Field(
        default=None,
        description="The user query after resolving all implicit references (e.g., 'it,' 'they') and relative temporal references (e.g., 'today,' 'last week') into explicit ones. This refined query is prepared for downstream database-related processing without verifying or clarifying vague entities.",
    )
    intent: Optional[Union[CreateTableModel, AddRecordsModel, UpdateRecordsModel, DeleteRecordsModel, QueryRecordsModel]] = Field(
        default=None,
        description=(
            "The structured intent extracted from the user's query. "
            "This field dynamically represents one of the following intents: "
            "- `CreateTableModel`: For creating a table with a specified schema. "
            "- `AddRecordsModel`: For adding new records to a table. "
            "- `UpdateRecordsModel`: For updating existing records in a table based on conditions. "
            "- `DeleteRecordsModel`: For deleting records from a table based on conditions. "
            "- `QueryRecordsModel`: For querying records from a table with optional filters, ordering, and limits."
        ),
    )
