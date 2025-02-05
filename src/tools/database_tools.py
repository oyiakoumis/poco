from typing import Dict, List, Optional
from langchain.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

from database_manager.document_db import DocumentDB
from models.schema import CollectionSchema, FieldDefinition


class ListCollectionsInput(BaseModel):
    """Input for listing collections"""

    user_id: str = Field(description="ID of the user to list collections for")


class ListCollectionsOutput(BaseModel):
    """Output from listing collections"""

    collections: List[Dict] = Field(description="List of collections with their schemas and descriptions")


class ListCollectionsTool(BaseTool):
    """Tool for listing available collections for a user"""

    name: str = "list_collections"
    description: str = "Lists all collections available for a user, including their schemas and descriptions"
    args_schema: type[BaseModel] = ListCollectionsInput
    return_direct: bool = True
    _db: DocumentDB = PrivateAttr()

    def __init__(self, db: DocumentDB):
        super().__init__()
        self._db = db

    async def _arun(self, user_id: str) -> ListCollectionsOutput:
        collections = await self._db.list_collections(user_id)
        return ListCollectionsOutput(collections=collections)

    def _run(self, user_id: str) -> ListCollectionsOutput:
        raise NotImplementedError("This tool only supports async operations")


class ListDocumentsInput(BaseModel):
    """Input for listing documents"""

    user_id: str = Field(description="ID of the user to list documents for")
    collection_name: str = Field(description="Name of the collection to list documents from")


class ListDocumentsOutput(BaseModel):
    """Output from listing documents"""

    documents: List[Dict] = Field(description="List of documents in the collection")


class ListDocumentsTool(BaseTool):
    """Tool for listing documents in a collection"""

    name: str = "list_documents"
    description: str = "Lists all documents in a specified collection"
    args_schema: type[BaseModel] = ListDocumentsInput
    return_direct: bool = True
    _db: DocumentDB = PrivateAttr()

    def __init__(self, db: DocumentDB):
        super().__init__()
        self._db = db

    async def _arun(self, user_id: str, collection_name: str) -> ListDocumentsOutput:
        documents = await self._db.list_documents(user_id, collection_name)
        return ListDocumentsOutput(documents=documents)

    def _run(self, user_id: str, collection_name: str) -> ListDocumentsOutput:
        raise NotImplementedError("This tool only supports async operations")


class CreateCollectionInput(BaseModel):
    """Input for creating a collection"""

    user_id: str = Field(description="ID of the user to create the collection for")
    collection_name: str = Field(description="Name of the collection to create")
    schema: Dict = Field(description="Schema definition for the collection")
    description: str = Field(description="Description of the collection's purpose")


class CreateCollectionTool(BaseTool):
    """Tool for creating a new collection"""

    name: str = "create_collection"
    description: str = "Creates a new collection with the specified schema"
    args_schema: type[BaseModel] = CreateCollectionInput
    return_direct: bool = True
    _db: DocumentDB = PrivateAttr()

    def __init__(self, db: DocumentDB):
        super().__init__()
        self._db = db

    async def _arun(self, user_id: str, collection_name: str, schema: Dict, description: str) -> bool:
        # Convert schema dict to CollectionSchema
        field_definitions = [FieldDefinition(**field_def) for field_def in schema["fields"]]
        collection_schema = CollectionSchema(name=collection_name, fields=field_definitions, description=description)

        await self._db.create_collection(user_id, collection_schema)
        return True

    def _run(self, user_id: str, collection_name: str, schema: Dict, description: str) -> bool:
        raise NotImplementedError("This tool only supports async operations")


class DatabaseOperationInput(BaseModel):
    """Input for database operations"""

    user_id: str = Field(description="ID of the user performing the operation")
    collection_name: str = Field(description="Name of the collection to operate on")
    operation: str = Field(description="Operation to perform (create, read, update, delete)")
    document_ids: Optional[List[str]] = Field(default=None, description="IDs of documents to operate on")
    data: Optional[Dict] = Field(default=None, description="Data for create/update operations")
    filters: Optional[Dict] = Field(default=None, description="Query filters for read/update/delete operations")


class DatabaseOperationTool(BaseTool):
    """Tool for performing database operations"""

    name: str = "execute_database_operation"
    description: str = "Executes a database operation (create, read, update, delete)"
    args_schema: type[BaseModel] = DatabaseOperationInput
    return_direct: bool = True
    _db: DocumentDB = PrivateAttr()

    def __init__(self, db: DocumentDB):
        super().__init__()
        self._db = db

    async def _arun(
        self,
        user_id: str,
        collection_name: str,
        operation: str,
        document_ids: Optional[List[str]] = None,
        data: Optional[Dict] = None,
        filters: Optional[Dict] = None,
    ) -> Dict:
        if operation == "create":
            result = await self._db.create_document(user_id, collection_name, data)
            return {"created": result}

        elif operation == "read":
            if document_ids:
                documents = []
                for doc_id in document_ids:
                    doc = await self._db.get_document(user_id, collection_name, doc_id)
                    if doc:
                        documents.append(doc)
                return {"documents": documents}
            else:
                documents = await self._db.query_documents(user_id, collection_name, filters or {})
                return {"documents": documents}

        elif operation == "update":
            if document_ids:
                for doc_id in document_ids:
                    await self._db.update_document(user_id, collection_name, doc_id, data)
            else:
                await self._db.update_documents(user_id, collection_name, filters or {}, data)
            return {"updated": True}

        elif operation == "delete":
            if document_ids:
                for doc_id in document_ids:
                    await self._db.delete_document(user_id, collection_name, doc_id)
            else:
                await self._db.delete_documents(user_id, collection_name, filters or {})
            return {"deleted": True}

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def _run(
        self,
        user_id: str,
        collection_name: str,
        operation: str,
        document_ids: Optional[List[str]] = None,
        data: Optional[Dict] = None,
        filters: Optional[Dict] = None,
    ) -> Dict:
        raise NotImplementedError("This tool only supports async operations")
