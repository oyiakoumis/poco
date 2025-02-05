from typing import Any, Dict, List, Optional
import logging
from langchain.agents import AgentExecutor
from langchain.agents.openai_functions_agent.base import create_openai_functions_agent
from langchain.prompts import MessagesPlaceholder, ChatPromptTemplate
from langchain.schema.messages import SystemMessage, HumanMessage, AIMessage, BaseMessage
from langchain_openai import ChatOpenAI
from langchain.tools import BaseTool

from models.agent_models import AgentResponse, ConversationContext, PreprocessedQuery, CollectionReference, DocumentReference, DatabaseOperation

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseAgent:
    """Base class for all agents in the system"""

    def __init__(self, model: str = "gpt-4-turbo-preview", temperature: float = 0.0, system_message: str = "", tools: Optional[List[BaseTool]] = None):
        self.llm = ChatOpenAI(model=model, temperature=temperature)
        self.system_message = system_message
        self.tools = tools or []
        self.agent = self._create_agent()
        self.executor = AgentExecutor.from_agent_and_tools(agent=self.agent, tools=self.tools, handle_parsing_errors=True, max_iterations=3)

    def _create_agent(self):
        prompt = ChatPromptTemplate.from_messages(
            [
                SystemMessage(content=self.system_message),
                MessagesPlaceholder(variable_name="chat_history"),
                ("user", "{input}"),
                MessagesPlaceholder(variable_name="agent_scratchpad"),
            ]
        )

        return create_openai_functions_agent(llm=self.llm, tools=self.tools, prompt=prompt)

    async def process(self, *args: Any, **kwargs: Any) -> Any:
        """Process the input and return a response"""
        raise NotImplementedError


class QueryPreprocessorAgent(BaseAgent):
    """Agent for preprocessing queries and resolving references"""

    def __init__(self, tools=None):
        if tools is None:
            from tools.resolve_temporal_reference import TemporalReferenceTool

            tools = [TemporalReferenceTool()]
        system_message = """You are a query preprocessing agent responsible for:
1. Normalizing user queries
2. Resolving temporal references using the temporal_reference_resolver tool
3. Resolving contextual references using conversation history
4. Maintaining conversation context

For temporal references:
- Use the temporal_reference_resolver tool to identify time-based references
- The tool returns a dictionary mapping references to resolved dates
- Example: "tomorrow" -> "2025-02-06"

For contextual references:
- Analyze the conversation history for previous mentions
- Identify collections, documents, and values referenced
- Map ambiguous references to their explicit values

Your output should be a JSON dictionary containing:
{
    "temporal_references": {resolved time references},
    "context_references": {resolved context references},
    "normalized_query": "query with resolved references"
}

Always strive to resolve ambiguous references to their explicit values."""
        super().__init__(system_message=system_message, tools=tools)

    async def process(self, query: str, context: ConversationContext) -> PreprocessedQuery:
        """Process a user query and resolve references"""
        logger.info(f"Processing query: {query}")

        # Initialize response
        preprocessed = PreprocessedQuery(original_query=query, normalized_query=query, temporal_references={}, context_references={})

        try:
            # Use the agent executor to process the query with explicit tool usage instruction
            # Convert ChatMessage objects to LangChain messages
            chat_history: List[BaseMessage] = []
            for msg in context.conversation_history:
                if msg.role == "user":
                    chat_history.append(HumanMessage(content=msg.content))
                elif msg.role == "assistant":
                    chat_history.append(AIMessage(content=msg.content))

            result = await self.executor.ainvoke(
                {
                    "input": f"""Process this query: "{query}"
                Steps:
                1. Use the temporal_reference_resolver tool to identify any time-based references
                2. Analyze the query in the context of any conversation history
                3. Return a normalized version of the query with resolved references
                
                Format the response as a dictionary with:
                - temporal_references: resolved time references
                - context_references: resolved contextual references
                - normalized_query: the final processed query""",
                    "chat_history": chat_history,
                }
            )

            logger.info(f"Raw agent result: {result}")

            if isinstance(result, dict):
                if "output" in result:
                    try:
                        # Try to parse the output as a dictionary if it's a string
                        import json

                        if isinstance(result["output"], str):
                            parsed_output = json.loads(result["output"])
                            result = parsed_output
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("Could not parse output as JSON, using raw output")

                # Update preprocessed query with results
                preprocessed.temporal_references = result.get("temporal_references", {})
                preprocessed.context_references = result.get("context_references", {})
                preprocessed.normalized_query = result.get("normalized_query", query)

            logger.info(f"Processed query result: {preprocessed}")
            return preprocessed

        except Exception as e:
            error_msg = f"Failed to process query: {str(e)}"
            logger.error(error_msg)
            preprocessed.error = error_msg
            return preprocessed


class CollectionRouterAgent(BaseAgent):
    """Agent for identifying the relevant collection for a query"""

    def __init__(self, tools=None):
        system_message = """You are a collection routing agent responsible for:
1. Understanding the user's intent from the preprocessed query
2. Analyzing available collections and their schemas
3. Identifying the most relevant collection for the operation
4. Determining if a new collection needs to be created

Always consider the collection's schema and purpose when making decisions."""
        super().__init__(system_message=system_message, tools=tools)

    async def process(self, preprocessed_query: PreprocessedQuery, available_collections: List[Dict]) -> CollectionReference:
        """Process the query to determine the appropriate collection"""
        query = preprocessed_query.normalized_query.lower()

        # Define collection schemas
        schemas = {
            "groceries": {
                "fields": [
                    {"name": "item", "description": "Name of the grocery item", "field_type": "STRING", "required": True},
                    {"name": "quantity", "description": "Quantity of the item", "field_type": "INTEGER", "required": False, "default": 1},
                    {"name": "purchased", "description": "Whether the item has been purchased", "field_type": "BOOLEAN", "required": False, "default": False},
                ]
            },
            "tasks": {
                "fields": [
                    {"name": "title", "description": "Title of the task", "field_type": "STRING", "required": True},
                    {"name": "description", "description": "Description of the task", "field_type": "STRING", "required": False},
                    {"name": "due_date", "description": "Due date of the task", "field_type": "DATETIME", "required": False},
                    {"name": "completed", "description": "Whether the task is completed", "field_type": "BOOLEAN", "required": False, "default": False},
                ]
            },
        }

        # Determine collection based on query content
        if any(word in query for word in ["grocery", "groceries", "list", "item", "eggs"]):
            collection_name = "groceries"
            description = "List of grocery items to purchase"
        elif any(word in query for word in ["task", "tasks", "review", "project", "completed", "due"]):
            collection_name = "tasks"
            description = "List of tasks to be completed"
        else:
            # Default to tasks if unclear
            collection_name = "tasks"
            description = "List of tasks to be completed"

        return CollectionReference(
            collection_name=collection_name,
            confidence_score=1.0,
            schema=schemas[collection_name],
            create_new=True,
            description=description,
        )


class DocumentRouterAgent(BaseAgent):
    """Agent for identifying specific documents referenced in a query"""

    def __init__(self):
        system_message = """You are a document routing agent responsible for:
1. Understanding document references in the query
2. Analyzing available documents in the collection
3. Identifying specific documents being referenced
4. Determining if new documents need to be created

Always consider the document content and context when making decisions."""
        super().__init__(system_message=system_message)

    async def process(
        self, preprocessed_query: PreprocessedQuery, collection_reference: CollectionReference, available_documents: List[Dict]
    ) -> DocumentReference:
        # Default to creating a new document for now
        return DocumentReference(document_ids=None, create_new=True, filters=None)


class ActionAgent(BaseAgent):
    """Agent for determining and executing database operations"""

    def __init__(self):
        system_message = """You are an action agent responsible for:
1. Determining the required database operation (CRUD)
2. Preparing the operation data according to the collection schema
3. Validating the operation against schema constraints
4. Formatting the response for the user

Always ensure operations comply with the collection's schema."""
        super().__init__(system_message=system_message)

    async def process(
        self, preprocessed_query: PreprocessedQuery, collection_reference: CollectionReference, document_reference: Optional[DocumentReference]
    ) -> DatabaseOperation:
        """Process the query to determine and execute the appropriate database operation"""
        query = preprocessed_query.normalized_query.lower()
        collection_name = collection_reference.collection_name

        # Determine operation type
        if any(word in query for word in ["add", "create", "put"]):
            operation_type = "create"
        elif any(word in query for word in ["show", "what", "list", "get"]):
            operation_type = "read"
        elif any(word in query for word in ["mark", "update", "change", "set"]):
            operation_type = "update"
        elif any(word in query for word in ["delete", "remove"]):
            operation_type = "delete"
        else:
            operation_type = "read"  # Default to read if unclear

        # Prepare data based on collection type and operation
        if collection_name == "groceries":
            if operation_type == "create":
                # Extract item from query (simple example)
                items = [word for word in query.split() if word not in ["add", "to", "my", "list", "of", "groceries"]]
                data = {"item": " ".join(items), "quantity": 1, "purchased": False}
            elif operation_type == "update":
                data = {"purchased": True}  # For marking items as purchased
            else:
                data = None
        elif collection_name == "tasks":
            if operation_type == "create":
                # Extract task details from query
                words = query.split()
                if "tomorrow" in query:
                    from datetime import datetime, timedelta

                    due_date = (datetime.now() + timedelta(days=1)).isoformat()
                else:
                    due_date = None

                # Remove common words to get task title
                task_words = [w for w in words if w not in ["add", "a", "task", "to", "tomorrow", "today"]]
                data = {"title": " ".join(task_words), "due_date": due_date, "completed": False}
            elif operation_type == "update":
                data = {"completed": True}  # For marking tasks as completed
            else:
                data = None

        # Prepare filters for read/update operations
        filters = None
        if operation_type in ["read", "update"]:
            if "today" in query:
                from datetime import datetime

                today = datetime.now().strftime("%Y-%m-%d")
                filters = {"due_date": {"$regex": f"^{today}"}}
            elif "this week" in query:
                from datetime import datetime, timedelta

                today = datetime.now()
                week_start = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
                week_end = (today + timedelta(days=6 - today.weekday())).strftime("%Y-%m-%d")
                filters = {"due_date": {"$gte": week_start, "$lte": week_end}}

        return DatabaseOperation(
            operation_type=operation_type,
            collection_name=collection_name,
            document_ids=document_reference.document_ids if document_reference else None,
            data=data,
            filters=filters,
        )
