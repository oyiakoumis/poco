"""Message processing service for WhatsApp messages."""

from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from langchain_community.callbacks.openai_info import OpenAICallbackHandler
from langchain_core.messages import AnyMessage, ToolMessage
from langchain_core.runnables import RunnableConfig

from agents.graph import create_graph
from api.utils.tool_operation_tracker import ToolOperationTracker
from database.conversation_store.models.message import Message
from database.document_store.dataset_manager import DatasetManager
from utils.logging import logger


class MessageProcessor:
    """Service for processing messages through the LangGraph."""

    def __init__(self, dataset_db: DatasetManager):
        """Initialize the message processor.

        Args:
            dataset_db: The dataset database manager
        """
        self.dataset_db = dataset_db
        self.graph = create_graph(dataset_db).compile()

    async def process_messages(
        self, conversation_history: List[Message], new_messages: List[Message], user_id: str, conversation_id: UUID
    ) -> Tuple[List[Message], AnyMessage, Optional[str], int, Optional[Dict[str, Any]]]:
        """Process messages through the LangGraph.

        Args:
            conversation_history: The conversation history
            new_messages: The new messages to process
            user_id: The user ID
            conversation_id: The conversation ID

        Returns:
            A tuple containing:
            - A list of new output messages
            - The last message (response)
            - A tool summary string (if any)
            - Total tokens used
            - File attachment (if any)
        """
        # Combine input messages with existing conversation history
        all_messages = conversation_history + new_messages

        # Initialize the callback handler
        callback_handler = OpenAICallbackHandler()

        # Configuration for the graph
        config = RunnableConfig(
            configurable={
                "user_id": user_id,
                "time_zone": "UTC",
                "first_day_of_the_week": 0,
                "thread_id": conversation_id,
            },
            recursion_limit=25,
            callbacks=[callback_handler],
        )

        # Process the message through the graph
        result = await self.graph.ainvoke({"messages": [message.message for message in all_messages]}, config)
        logger.info(f"Graph processing completed - Thread: {conversation_id}")

        # Track total tokens used
        total_tokens = callback_handler.total_tokens

        # Get the IDs of all messages in the conversation history
        input_ids = {str(msg.id) for msg in all_messages}

        # Identify new messages by comparing IDs
        output_messages = [Message(user_id=user_id, conversation_id=conversation_id, message=msg) for msg in result["messages"] if msg.id not in input_ids]

        # Get the last message for the WhatsApp response
        response = result["messages"][-1]

        # Generate tool summary
        tool_summary = self._generate_tool_summary(output_messages)

        # Check if there are file attachments in the state
        file_attachments = result.get("export_file_attachments")
        if file_attachments:
            logger.info(f"File attachments found in state: {len(file_attachments)} attachment(s)")
            for attachment in file_attachments:
                logger.info(f"File attachment: {attachment.get('filename')}")

        return output_messages, response, tool_summary, total_tokens, file_attachments

    def _generate_tool_summary(self, messages: List[Message]) -> Optional[str]:
        """Generate a summary of tool operations.

        Args:
            messages: The messages to analyze for tool operations

        Returns:
            A summary string, or None if no tool operations were found
        """
        # Track tool operations and generate summary
        tracker = ToolOperationTracker()

        # Filter for tool messages with successful operations
        tool_messages = [
            msg.message
            for msg in messages
            if isinstance(msg.message, ToolMessage)
            and hasattr(msg.message, "name")
            and msg.message.name in tracker.get_supported_tools()
            and hasattr(msg.message, "status")
            and msg.message.status == "success"
        ]

        # Track each tool message
        for msg in tool_messages:
            tracker.track_tool_message(msg.name, msg.content)

        # Generate summary
        return tracker.build_tool_summary_string()
