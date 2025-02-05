from typing import Annotated, Dict, List, TypedDict, Optional
import logging
from langgraph.graph import StateGraph
from langchain.schema import BaseMessage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from agents.base_agents import QueryPreprocessorAgent, CollectionRouterAgent, DocumentRouterAgent, ActionAgent
from models.agent_models import AgentResponse, ConversationContext, PreprocessedQuery, CollectionReference, DocumentReference, DatabaseOperation


class AgentState(TypedDict):
    """State maintained between agent calls"""

    messages: List[BaseMessage]
    context: ConversationContext
    query: str
    user_id: str
    preprocessed_query: Optional[PreprocessedQuery]
    collection_reference: Optional[CollectionReference]
    document_reference: Optional[DocumentReference]
    operation: Optional[DatabaseOperation]
    response: Optional[AgentResponse]


def create_workflow() -> StateGraph:
    """Create the workflow graph for processing queries"""

    # Initialize agents
    preprocessor = QueryPreprocessorAgent()
    collection_router = CollectionRouterAgent()
    document_router = DocumentRouterAgent()
    action_agent = ActionAgent()

    # Create workflow graph
    workflow = StateGraph(AgentState)

    # Define node functions
    async def preprocess_node(state: AgentState) -> AgentState:
        """Preprocess the query"""
        try:
            state["preprocessed_query"] = await preprocessor.process(query=state["query"], context=state["context"])
            logger.info(f"Preprocessed query: {state['preprocessed_query']}")

            if state["preprocessed_query"].error:
                state["response"] = AgentResponse(
                    success=False,
                    message=f"Failed to preprocess query: {state['preprocessed_query'].error}",
                    error=state["preprocessed_query"].error,
                    data={"preprocessed_query": state["preprocessed_query"].dict()},
                )
            else:
                # No error, continue with normalized query
                logger.info(f"Successfully preprocessed query: {state['preprocessed_query'].normalized_query}")

            return state
        except Exception as e:
            logger.error(f"Error in preprocess_node: {str(e)}", exc_info=True)
            state["response"] = AgentResponse(success=False, message=f"Failed to preprocess query: {str(e)}", error=str(e), data={"exception": str(e)})
            return state

    async def route_collection_node(state: AgentState) -> AgentState:
        """Route to appropriate collection"""
        try:
            if state.get("response") and not state["response"].success:
                return state

            state["collection_reference"] = await collection_router.process(
                preprocessed_query=state["preprocessed_query"], available_collections=[]  # TODO: Get from database
            )
            return state
        except Exception as e:
            state["response"] = AgentResponse(success=False, message=f"Failed to route collection: {str(e)}", error=str(e))
            return state

    async def route_document_node(state: AgentState) -> AgentState:
        """Route to appropriate document(s)"""
        try:
            if state.get("response") and not state["response"].success:
                return state

            state["document_reference"] = await document_router.process(
                preprocessed_query=state["preprocessed_query"],
                collection_reference=state["collection_reference"],
                available_documents=[],  # TODO: Get from database
            )
            return state
        except Exception as e:
            state["response"] = AgentResponse(success=False, message=f"Failed to route document: {str(e)}", error=str(e))
            return state

    async def execute_action_node(state: AgentState) -> AgentState:
        """Execute the database operation"""
        try:
            if state.get("response") and not state["response"].success:
                return state

            if not state.get("collection_reference"):
                state["response"] = AgentResponse(success=False, message="No collection reference available", error="Collection reference missing")
                return state

            state["operation"] = await action_agent.process(
                preprocessed_query=state["preprocessed_query"],
                collection_reference=state["collection_reference"],
                document_reference=state.get("document_reference"),
            )

            state["response"] = AgentResponse(
                success=True,
                message="Operation completed successfully",
                data={
                    "operation": state["operation"].dict(),
                    "collection_name": state["collection_reference"].collection_name,
                    "document_ids": state["document_reference"].document_ids if state.get("document_reference") else None,
                },
            )
            return state
        except Exception as e:
            state["response"] = AgentResponse(success=False, message=f"Failed to execute operation: {str(e)}", error=str(e))
            return state

    # Add nodes
    workflow.add_node("preprocess", preprocess_node)
    workflow.add_node("route_collection", route_collection_node)
    workflow.add_node("route_document", route_document_node)
    workflow.add_node("execute_action", execute_action_node)

    # Define edges
    workflow.add_edge("preprocess", "route_collection")

    # Add conditional routing
    def route_collection(state: AgentState) -> str:
        """Route based on whether a new collection is needed"""
        if state.get("response") and not state["response"].success:
            return "execute_action"  # Route to execute_action for error handling
        if state["collection_reference"].create_new:
            return "execute_action"
        return "route_document"

    workflow.add_conditional_edges("route_collection", route_collection, ["execute_action", "route_document"])

    # Add edge from route_document to execute_action
    workflow.add_edge("route_document", "execute_action")

    # Set entry and end points
    workflow.set_entry_point("preprocess")
    workflow.set_finish_point("execute_action")

    return workflow.compile()


async def process_query(query: str, context: ConversationContext, user_id: str) -> AgentResponse:
    """Process a natural language query through the agent workflow"""

    workflow = create_workflow()

    # Initialize state
    state = AgentState(
        messages=[],
        context=context,
        query=query,
        user_id=user_id,
        preprocessed_query=None,
        collection_reference=None,
        document_reference=None,
        operation=None,
        response=None,
    )

    # Execute workflow
    try:
        final_state = await workflow.ainvoke(state)
        return final_state["response"] or AgentResponse(success=False, message="Failed to process query", error="No response generated")
    except Exception as e:
        return AgentResponse(success=False, message="Failed to process query", error=str(e))
