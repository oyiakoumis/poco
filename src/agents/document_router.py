from typing import Dict, List
from langchain.tools import BaseTool

from agents.base_agents import DocumentRouterAgent
from models.agent_models import PreprocessedQuery, CollectionReference, DocumentReference
from tools.database_tools import ListDocumentsTool


class DocumentRouter(DocumentRouterAgent):
    """Implementation of the document routing agent"""

    def __init__(self, db):
        super().__init__()
        self.tools = [ListDocumentsTool(db=db)]

    async def process(
        self, preprocessed_query: PreprocessedQuery, collection_reference: CollectionReference, available_documents: List[Dict]
    ) -> DocumentReference:
        """Identify documents referenced in the query"""

        try:
            # Analyze the query to determine document references
            document_analysis = await self.llm.ainvoke(
                f"""Given this normalized query and available documents, determine:
                1. Which specific documents are referenced (if any)
                2. Whether new documents need to be created
                3. Any filters that should be applied to find relevant documents

                Query: "{preprocessed_query.normalized_query}"

                Collection: {collection_reference.collection_name}
                Schema: {collection_reference.schema}

                Available Documents:
                {available_documents}

                Consider:
                - Explicit references to specific documents
                - Implicit references based on document content
                - Query filters that could identify relevant documents
                - Whether the query implies creating new documents
                - The schema fields and their purposes

                Format your response as a JSON object with:
                - document_ids: list[str] or null
                - create_new: bool
                - filters: dict or null (MongoDB-style query filters)
                - reason: str (explanation of the decision)"""
            )

            if not isinstance(document_analysis, dict):
                raise ValueError("Invalid document analysis response")

            # If specific documents were identified
            if document_analysis.get("document_ids"):
                # Verify the documents exist
                found_docs = [doc["_id"] for doc in available_documents if doc["_id"] in document_analysis["document_ids"]]

                if not found_docs and not document_analysis.get("create_new", False):
                    raise ValueError(f"Referenced documents not found. Reason: {document_analysis.get('reason', 'Unknown')}")

                return DocumentReference(
                    document_ids=found_docs, create_new=document_analysis.get("create_new", False), filters=document_analysis.get("filters")
                )

            # If using filters to identify documents
            if document_analysis.get("filters"):
                return DocumentReference(document_ids=None, create_new=document_analysis.get("create_new", False), filters=document_analysis["filters"])

            # If creating new documents
            if document_analysis.get("create_new", False):
                return DocumentReference(document_ids=None, create_new=True, filters=None)

            # Default case: no specific documents identified
            return DocumentReference(document_ids=None, create_new=False, filters=None)

        except Exception as e:
            # If we can't determine specific documents, return a default reference
            return DocumentReference(document_ids=None, create_new=True, filters=None)  # Default to creating new if we can't identify existing
