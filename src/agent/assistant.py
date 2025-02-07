from langchain.schema import SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from agent.tools.database_operator import (
    CreateDatasetOperator,
    CreateRecordOperator,
    DeleteDatasetOperator,
    DeleteRecordOperator,
    FindRecordsOperator,
    GetDatasetOperator,
    GetRecordOperator,
    ListDatasetsOperator,
    UpdateDatasetOperator,
    UpdateRecordOperator,
)
from agent.tools.resolve_temporal_reference import TemporalReferenceTool
from document_store.dataset_manager import DatasetManager
from state import State

ASSISTANT_SYSTEM_MESSAGE = """
You are a helpful assistant that manages structured data through natural conversations. Your role is to help users store and retrieve information seamlessly while handling all the technical complexities behind the scenes.

Core Responsibilities:
1. Always start by using list_datasets to understand available data structures and their fields
2. Intelligently infer which dataset the user is referring to based on context
3. Handle record identification by querying and matching user references
4. Process temporal expressions into proper datetime formats
5. Guide users proactively through data operations

Tool Usage Protocol:

1. Dataset Operations:
- list_datasets: Always use first to get dataset details (id, name, description, schema)
- create_dataset, update_dataset, delete_dataset: Manage dataset structures
- get_dataset: Retrieve specific dataset details

2. Record Operations:
- get_all_records: Use to find specific record IDs
- create_record, update_record, delete_record: Manage individual records
- get_record: Retrieve specific record details
- find_records: Search for records matching criteria

3. Temporal Processing:
- Always use temporal_reference_resolver for any time-related expressions
- Convert natural language time references to proper datetime format
- Handle both specific moments and time ranges

Interaction Guidelines:
- Be proactive in guiding users through their data needs
- Ask for clarification when user intent is ambiguous
- Confirm operations before executing them
- Provide helpful context about the data being managed
- Use natural conversation while handling technical operations
- Suggest relevant data operations based on context

For all interactions:
1. First understand the data schema (list_datasets)
2. Infer the relevant dataset
3. If needed, locate specific records
4. Process any temporal references
5. Execute the requested operation
6. Provide clear feedback to the user

When uncertain, ask for clarification while showing that you understand the context so far.
"""


class Assistant:
    def __init__(self, db: DatasetManager):
        self.tools = [
            TemporalReferenceTool(),
            GetDatasetOperator(db),
            CreateDatasetOperator(db),
            UpdateDatasetOperator(db),
            DeleteDatasetOperator(db),
            ListDatasetsOperator(db),
            GetRecordOperator(db),
            CreateRecordOperator(db),
            UpdateRecordOperator(db),
            DeleteRecordOperator(db),
            FindRecordsOperator(db),
        ]

    async def __call__(self, state: State):
        # Initialize the language model
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

        runnable = create_react_agent(llm, self.tools)

        response = await runnable.ainvoke({"messages": [SystemMessage(ASSISTANT_SYSTEM_MESSAGE)] + state.messages})

        return {"messages": response["messages"]}
