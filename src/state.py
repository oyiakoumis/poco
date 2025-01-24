from typing import Annotated, Optional, Union

from langgraph.graph.message import AnyMessage, add_messages
from pydantic import BaseModel, Field

from models.extract_intent import AddRecordsModel, CreateTableModel, DeleteRecordsModel, QueryRecordsModel, UpdateRecordsModel


class State(BaseModel):
    messages: Annotated[list[AnyMessage], add_messages]
    current_user_query: Optional[str] = None
    current_intent: Optional[Union[CreateTableModel, AddRecordsModel, UpdateRecordsModel, DeleteRecordsModel, QueryRecordsModel]] = Field(
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
