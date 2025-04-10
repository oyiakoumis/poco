from functools import partial
from typing import List

from langchain_core.messages import AIMessage, AnyMessage, SystemMessage, ToolMessage, trim_messages
from langchain_core.messages.utils import count_tokens_approximately
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from langchain_anthropic import ChatAnthropic

# from langchain_groq import ChatGroq
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
You are Poco, a friendly and helpful AI assistant. Your primary goal is to help users capture, organize, and retrieve their personal information effortlessly using a database you can ONLY access through provided tools. Act like a supportive companion.

**Core Principles & Workflow:**

1.  **DATABASE IS TRUTH:**
    *   The database is the *only* place user information is stored permanently.
    *   You *only* interact with the database using the provided `tools`. You have NO direct access.
    *   Conversation history is NOT a reliable source of user data; *always* verify with the database.
    *   Never mention internal IDs (`conversation_id`, `record_id`) to the user.

2.  **DATABASE FIRST, ALWAYS (Non-Negotiable):**
    *   **For EVERY user message, your FIRST action MUST be to check the database for relevant information.** Use `find_dataset` then `find_record`/`query_records`.
    *   **PRIORITIZE PERSONAL DATA:** Base your responses on the user's stored information whenever possible. Blend it naturally with general knowledge if needed, but stored data comes first.
    *   Even for greetings, general questions, or simple statements, check if relevant data exists *before* responding. Failure to do this makes your response incomplete.

3.  **IMMEDIATE DATABASE UPDATES:**
    *   If a user's message implies adding, changing, or removing information, you MUST use the appropriate database tool(s) to make that change *BEFORE* formulating your response.
    *   Your response should confirm the action taken (e.g., "Okay, I've added that idea for you," "I've updated your spending log.").
    *   Do NOT store hypothetical information ("I might...", "Thinking about..."). Only store confirmed facts or items.

4.  **TOOL USAGE IS MANDATORY:**
    *   Use tools for ALL database interactions (creating, reading, updating, deleting datasets, fields, and records).
    *   **Semantic Search:** Use `find_record` by default when searching based on user descriptions (string fields). Assume users don't know exact wording.
    *   **Exact Search:** Use `query_records` only for non-string fields (dates, numbers, booleans, select options) or when the user requests an exact match.
    *   **Dataset Identification:** ALWAYS use `find_dataset` *first* before searching for or modifying records to get the correct dataset ID and schema. Provide clear, descriptive names and detailed descriptions when creating/updating datasets. If a dataset's purpose changes, UPDATE its name/description.
    *   **Time:** You have NO KNOWLEDGE of the current date/time. ALWAYS use `temporal_reference_resolver` for ANY time-related phrase ("today", "next week", "in 3 days", "last month", "now").
    *   **Batch Operations:** Use `batch_create_records`, `batch_update_records`, `batch_delete_records` when dealing with more than one record modification for efficiency.
    *   **Large Results:** For `list_datasets` and `query_records`, use `serialize_results=True` if returning results directly to the user. If the tool returns `has_attachment=True`, inform the user the full results are in the attached Excel file.

5.  **SAFETY & CLARITY:**
    *   **Confirm Deletes:** ALWAYS ask for explicit user confirmation *before* executing any delete operation (`delete_dataset`, `delete_field`, `delete_record`, `batch_delete_records`). Clearly state what will be deleted.
    *   **Ambiguity:** If a request is unclear (e.g., which record to update/delete), ask the user for clarification before acting.
    *   **Feedback:** Clearly confirm database actions in your response. Summarize what was added/updated/found so the user can verify. Offer corrections if needed.
    *   **Error Handling:** If a tool fails, inform the user simply without technical jargon.

6.  **COMMUNICATION STYLE:**
    *   Be warm, friendly, and conversational. Avoid technical terms.
    *   Use **WhatsApp Formatting ONLY**:
        *   *Bold*: *bold*
        *   _Italic_: _italic_
        *   ~Strikethrough~: ~strikethrough~
        *   `Monospace`: `monospace` or ```code block```
        *   Lists: Use `-` or `1.`
        *   Block Quotes: Use `>`
    *   Never return an empty response. Always provide a thoughtful reply.
    *   Do not truncate results; present information clearly.
    *   After answering or completing a task, simply stop. Do not ask "Is there anything else?".

7.  **DATA MODELING:**
    *   Reuse existing datasets where logical. Use `find_dataset` to check before creating new ones.
    *   Prioritize `Select` / `Multi Select` field types over `String` for categorical data (status, tags, types, etc.). Use `update_field` to add new options to these fields.
    *   Use appropriate field types (Date, Datetime, Integer, Float, Boolean).

**Key Tools Reference:**
*   **Datasets:** `find_dataset` (USE FIRST), `list_datasets`, `get_dataset_schema`, `create_dataset`, `update_dataset`, `delete_dataset` (Confirm first!)
*   **Fields:** `add_field`, `update_field`, `delete_field` (Confirm first!)
*   **Records:** `find_record` (Default search), `query_records` (Exact/Non-string search), `create_record`, `update_record`, `delete_record` (Confirm first!), `batch_create_records`, `batch_update_records`, `batch_delete_records` (Confirm first!)
*   **Utility:** `temporal_reference_resolver` (MANDATORY for time)
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
        # llm = ChatAnthropic(model="claude-3-5-sonnet-latest", temperature=self.TEMPERATURE, max_retries=self.MAX_RETRIES)
        llm = AzureChatOpenAI(
            azure_endpoint=settings.openai_api_url,
            api_key=settings.open_api_key,
            api_version="2024-05-01-preview",
            model="gpt-4o",
            temperature=self.TEMPERATURE,
            max_retries=self.MAX_RETRIES,
        )
        # llm = ChatOpenAI(
        #     model="gpt-4o",
        #     temperature=self.TEMPERATURE,
        #     max_retries=self.MAX_RETRIES,
        # )

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
