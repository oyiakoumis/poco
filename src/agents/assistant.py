from functools import partial
from typing import List

from langchain_core.messages import AIMessage, AnyMessage, SystemMessage, ToolMessage, trim_messages
from langchain_core.messages.utils import count_tokens_approximately
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_groq import ChatGroq
from langgraph.graph.graph import CompiledGraph
from langgraph.prebuilt import create_react_agent

from agents.exceptions import AssistantResponseError
from agents.state import State
from agents.tools.database_operator import (
    AddFieldOperator,
    BatchCreateRecordsOperator,
    BatchDeleteRecordsOperator,
    BatchUpdateRecordsOperator,
    CreateDatasetOperator,
    CreateRecordOperator,
    DeleteDatasetOperator,
    DeleteFieldOperator,
    DeleteRecordOperator,
    FindDatasetOperator,
    FindRecord,
    GetAllRecordsOperator,
    GetDatasetOperator,
    GetDatasetSchemaOperator,
    ListDatasetsOperator,
    QueryRecordsOperator,
    UpdateDatasetOperator,
    UpdateFieldOperator,
    UpdateRecordOperator,
)
from agents.tools.resolve_temporal_reference import TemporalReferenceTool
from database.document_store.dataset_manager import DatasetManager
from settings import settings
from utils.logging import logger

ASSISTANT_SYSTEM_MESSAGE = """
You are Poco, a friendly AI assistant helping users manage personal information in a database accessible only via provided tools.

**Core Directives:**

1.  **Database is Sole Truth & Tool-Driven:**
    *   All user information resides *only* in the database.
    *   Interact with the database *exclusively* through the provided `tools`. You have NO direct access.
    *   Conversation history is volatile; *always* use tools to verify or retrieve user data.
    *   Never expose internal IDs (like `record_id`) to the user.

2.  **Mandatory Workflow (For EVERY User Message):**
    *   **Step 1: Query First:** ALWAYS use `find_dataset` then `find_record` (default semantic search) or `query_records` (exact/non-string search) to check for relevant information *before* formulating your response. Base your response primarily on this data.
    *   **Step 2: Update Immediately:** If the user's message implies adding, changing, or removing information, execute the appropriate database tool (`create_record`, `update_record`, `delete_record`, etc.) *before* crafting your reply.
    *   **Step 3: Respond & Confirm:** Generate your response, incorporating retrieved data and clearly confirming any database actions taken (e.g., "Okay, I've added that idea," "Updated your log").

3.  **Essential Tool Usage:**
    *   **Time:** MUST use `temporal_reference_resolver` for ANY time-related phrase ("today", "next week", "last month"). You have no internal knowledge of time.
    *   **Datasets:** Use `find_dataset` first before record operations. Reuse existing datasets logically. Use clear names/descriptions.
    *   **Data Modeling:** Use appropriate field types (Date, Number, Boolean, Select, Multi Select). Prefer `Select`/`Multi Select` for categories (use `update_field` to add options).
    *   **Large Results:** Use `serialize_results=True` for `list_datasets`/`query_records` if showing results to the user. If `has_attachment=True` is returned, inform the user about the attached file.
    *   **Batch Operations:** Use `batch_*` tools when modifying multiple records.

4.  **Safety & User Interaction:**
    *   **Confirm Deletes:** REQUIRED: Get explicit user confirmation *before* executing any delete operation (`delete_dataset`, `delete_field`, `delete_record`, `batch_delete_records`). Clearly state what will be deleted.
    *   **Clarify Ambiguity:** Ask for clarification if a request is unclear before acting.
    *   **Error Handling:** Inform the user simply if a tool fails.
    *   **Store Facts Only:** Only save confirmed information, not intentions ("I might...").

5.  **Communication Style:**
    *   Be friendly, warm, and conversational. Avoid technical jargon.
    *   Use **WhatsApp Formatting ONLY**: *bold*, _italic_, ~strikethrough~, `monospace`/```code block```, `-` or `1.` lists, `>` quotes.
    *   Present information clearly.
    *   After completing the task, simply stop. Do not ask "Is there anything else?".

**Key Tool Categories (Reference):**
*   Datasets: `find_dataset`, `list_datasets`, `create_dataset`, etc.
*   Fields: `add_field`, `update_field`, `delete_field`, etc.
*   Records: `find_record`, `query_records`, `create_record`, `update_record`, `delete_record`, `batch_*`, etc.
*   Utility: `temporal_reference_resolver`
"""


class Assistant:
    TOKEN_LIMIT = 128000
    MAX_RETRIES = 3
    TEMPERATURE = 1

    def __init__(self, db: DatasetManager):
        logger.info("Initializing Assistant with tools")
        self.tools = [
            TemporalReferenceTool(),
            CreateDatasetOperator(db),
            UpdateDatasetOperator(db),
            DeleteDatasetOperator(db),
            ListDatasetsOperator(db),
            # GetDatasetOperator(db),
            GetDatasetSchemaOperator(db),
            FindDatasetOperator(db),
            FindRecord(db),
            QueryRecordsOperator(db),
            # GetAllRecordsOperator(db),
            CreateRecordOperator(db),
            UpdateRecordOperator(db),
            DeleteRecordOperator(db),
            BatchCreateRecordsOperator(db),
            BatchUpdateRecordsOperator(db),
            BatchDeleteRecordsOperator(db),
            UpdateFieldOperator(db),
            DeleteFieldOperator(db),
            AddFieldOperator(db),
        ]

    async def __call__(self, state: State):
        logger.debug(f"Processing state with {len(state.messages)} messages")
        # Initialize the language model
        llm = ChatAnthropic(model="claude-3-7-sonnet-latest", temperature=self.TEMPERATURE, max_retries=self.MAX_RETRIES)
        # llm = AzureChatOpenAI(
        #     azure_endpoint=settings.openai_api_url,
        #     api_key=settings.open_api_key,
        #     api_version="2024-05-01-preview",
        #     model="gpt-4o",
        #     temperature=self.TEMPERATURE,
        #     max_retries=self.MAX_RETRIES,
        # )
        # llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=self.TEMPERATURE, max_retries=self.MAX_RETRIES)
        # llm = ChatOpenAI(model="gpt-4.1", temperature=self.TEMPERATURE, max_retries=self.MAX_RETRIES)

        logger.debug(f"Trimming messages to token limit: {self.TOKEN_LIMIT}")
        state.messages = trim_messages(
            state.messages,
            strategy="last",
            token_counter=partial(count_tokens_approximately, chars_per_token=3.4),
            max_tokens=self.TOKEN_LIMIT,
            start_on="human",
            end_on="human",
            include_system=True,
            allow_partial=False,
        )

        runnable = create_react_agent(llm, self.tools, state_schema=State)

        # Get a valid response using the retry mechanism
        result = await self.force_response(runnable, state)
        logger.debug("LLM response received")

        return result

    async def force_response(self, runnable: CompiledGraph, state: State) -> AIMessage:
        """Attempt to get a valid response with retry logic."""
        for attempt in range(self.MAX_RETRIES):
            logger.debug(f"Invoking LLM (attempt {attempt+1}/{self.MAX_RETRIES})")
            result = await runnable.ainvoke(state)

            last_message: AnyMessage = result["messages"][-1]

            if not isinstance(last_message, ToolMessage) and last_message.content.strip():
                return result  # Valid response, return immediately

            # Handle invalid response
            logger.warning(f"Empty response on attempt {attempt+1}")
            state.messages.extend([result, SystemMessage("Please provide a non-empty response.")])

        # If we get here, all retries failed
        error_msg = f"Failed to get valid response after {self.MAX_RETRIES} attempts"
        logger.error(error_msg)
        raise AssistantResponseError(error_msg)
