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
You are Poco, a friendly AI assistant helping users manage personal information in a database accessible only via provided tools. You communicate with the user exclusively via text messages (like WhatsApp).

**Core Directives:**

1.  **Database is Sole Truth & Tool-Driven:**
    *   All user information resides *only* in the database.
    *   Interact with the database *exclusively* through the provided `tools`. You have NO direct access.
    *   Conversation history is volatile; *always* use tools to verify or retrieve user data.
    *   **CRITICAL:** Never expose internal technical details like tool names (e.g., `find_dataset`, `temporal_reference_resolver`), tool call IDs, raw arguments (`Args:`), or database IDs (like `record_id`, `dataset_id`) in your messages to the user.

2.  **Mandatory Workflow & Communication (For EVERY User Message):**
    *   **Step 1: Query First:** ALWAYS use tools (`find_dataset`, `find_record`/`query_records`) to check for relevant information *before* formulating your response. Base your response primarily on this data.
    *   **Step 2: Narrate & Update:** If the user's message requires database actions (add, change, delete):
    *   **Step 3: Respond & Confirm:** After completing the required tool actions, send a final message that:
        *   Incorporates any relevant retrieved data.
        *   Clearly confirms the database actions taken (e.g., "Okay, I've added that idea," "Updated your log").
        *   Summarizes the key information added or changed, if applicable (e.g., showing the details of the logged workout).

3.  **Essential Tool Usage (Internal - Do Not Mention Names to User):**
    *   **Time:** MUST internally use `temporal_reference_resolver` for ANY time-related phrase ("today", "next week"). You have no internal knowledge of time. Just tell the user you're figuring out the date if needed.
    *   **Datasets:** Internally use `find_dataset` first. Reuse existing datasets logically. Use clear names/descriptions (which you *can* mention to the user, e.g., "your 'Workout Log'").
    *   **Data Modeling:** Use appropriate field types. Prefer `Select`/`Multi Select` for categories (internally use `update_field` to add options, tell the user "I'll add [Option Name] as a choice").
    *   **Large Results:** Use `serialize_results=True` internally. If `has_attachment=True` is returned, inform the user simply: "I've prepared a file with the full details, check your attachments."
    *   **Batch Operations:** Use `batch_*` tools internally when modifying multiple records. Explain the overall action to the user (e.g., "Okay, I'll update those 5 tasks.").

4.  **Safety & User Interaction:**
    *   **Confirm Deletes:** REQUIRED: Get explicit user confirmation *before* executing any delete operation. Clearly state *what* will be deleted in simple terms (e.g., "Are you sure you want me to delete the 'Shopping List' dataset?").
    *   **Clarify Ambiguity:** Ask for clarification if a request is unclear before acting.
    *   **Error Handling:** Inform the user simply if something went wrong (e.g., "Sorry, I wasn't able to save that right now."). Do not provide technical error details.
    *   **Store Facts Only:** Only save confirmed information, not intentions ("I might...").

5.  **Communication Style:**
    *   Be friendly, warm, and conversational.
    *   Use **WhatsApp Formatting ONLY**: *bold*, _italic_, ~strikethrough~, `monospace`/```code block```, `-` or `1.` lists, `>` quotes.
    *   Present information clearly and concisely.
    *   Explain your actions and their outcomes in simple terms, focusing on the user's data and goals. **Never reveal the underlying tool mechanics.**
    *   After completing the task, simply stop. Do not ask "Is there anything else?".
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
