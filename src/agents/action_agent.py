from typing import Dict, List, Optional
from langchain.tools import BaseTool

from agents.base_agents import ActionAgent
from models.agent_models import PreprocessedQuery, CollectionReference, DocumentReference, DatabaseOperation
from tools.database_tools import CreateCollectionTool, DatabaseOperationTool


class Action(ActionAgent):
    """Implementation of the action agent"""

    def __init__(self, db):
        super().__init__()
        self.tools = [CreateCollectionTool(db=db), DatabaseOperationTool(db=db)]

    async def process(
        self, preprocessed_query: PreprocessedQuery, collection_reference: CollectionReference, document_reference: Optional[DocumentReference]
    ) -> DatabaseOperation:
        """Determine and prepare the database operation"""

        try:
            # Analyze the query to determine the required operation
            operation_analysis = await self.llm.ainvoke(
                f"""Given this normalized query and collection/document information, determine:
                1. What database operation is needed (create, read, update, delete)
                2. What data needs to be included in the operation
                3. How to format the data according to the collection schema

                Query: "{preprocessed_query.normalized_query}"

                Collection: {collection_reference.collection_name}
                Schema: {collection_reference.schema}
                Creating New Collection: {collection_reference.create_new}

                Document Reference:
                - IDs: {document_reference.document_ids if document_reference else None}
                - Create New: {document_reference.create_new if document_reference else False}
                - Filters: {document_reference.filters if document_reference else None}

                Consider:
                - The intent of the query
                - The schema fields and their types
                - Required vs optional fields
                - Default values
                - Field validations

                Format your response as a JSON object with:
                - operation_type: str (create, read, update, delete)
                - data: dict or null (for create/update operations)
                - filters: dict or null (for read/update/delete operations)
                - reason: str (explanation of the operation)"""
            )

            if not isinstance(operation_analysis, dict):
                raise ValueError("Invalid operation analysis response")

            # If we need to create a new collection first
            if collection_reference.create_new:
                # Create the collection
                await self.executor.arun(
                    tool="create_collection",
                    user_id=collection_reference.user_id,
                    collection_name=collection_reference.collection_name,
                    schema=collection_reference.schema,
                    description=collection_reference.description,
                )

            # Prepare the database operation
            operation = DatabaseOperation(
                operation_type=operation_analysis["operation_type"],
                collection_name=collection_reference.collection_name,
                document_ids=document_reference.document_ids if document_reference else None,
                data=operation_analysis.get("data"),
                filters=operation_analysis.get("filters") or (document_reference.filters if document_reference else None),
            )

            # Validate the operation data against the schema
            if operation.data:
                # Use the LLM to validate and format the data
                validation_result = await self.llm.ainvoke(
                    f"""Validate and format this data according to the collection schema:
                    
                    Data: {operation.data}
                    Schema: {collection_reference.schema}
                    
                    Ensure:
                    1. All required fields are present
                    2. Data types match schema definitions
                    3. Values are properly formatted
                    4. No extra fields are included
                    5. Default values are applied where needed
                    6. SELECT/MULTI_SELECT values are from allowed options
                    
                    Format your response as a JSON object with the validated data."""
                )

                if isinstance(validation_result, dict):
                    operation.data = validation_result
                else:
                    raise ValueError("Invalid data validation result")

            # Execute the operation
            result = await self._execute_operation(operation)
            return operation

        except Exception as e:
            # If we can't determine the operation, raise the error
            raise ValueError(f"Failed to determine database operation: {str(e)}")

    async def _execute_operation(self, operation: DatabaseOperation) -> Dict:
        """Execute a database operation using the DatabaseOperationTool"""
        return await self.executor.arun(
            tool="execute_database_operation",
            operation_type=operation.operation_type,
            collection_name=operation.collection_name,
            document_ids=operation.document_ids,
            data=operation.data,
            filters=operation.filters,
        )
