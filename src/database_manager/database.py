import logging
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from database_manager.collection import Collection
from database_manager.collection_definition import CollectionDefinition
from database_manager.collection_registry import CollectionRegistry
from database_manager.connection import Connection
from database_manager.document import Document
from database_manager.embedding_wrapper import EmbeddingWrapper
from database_manager.operations.collection_operations import (
    AddFieldsOperation,
    CreateCollectionOperation,
    DeleteFieldsOperation,
    DropCollectionOperation,
    RenameCollectionOperation,
)
from database_manager.operations.document_operations import (
    BulkDeleteOperation,
    BulkInsertOperation,
    BulkUpdateOperation,
    DocumentDeleteOperation,
    DocumentInsertOperation,
    DocumentUpdateOperation,
)
from database_manager.operations.operation_history import OperationHistory
from database_manager.schema_field import SchemaField

logger = logging.getLogger(__name__)


class Database:
    """
    Represents a database with collections and registry management.
    Includes support for undo/redo operations.
    """

    def __init__(self, name: str, connection: Connection, embeddings: EmbeddingWrapper) -> None:
        self.name = name
        self.connection = connection
        self.embeddings = embeddings
        self._mongo_db: Optional[AsyncIOMotorDatabase] = None
        self.registry: Optional[CollectionRegistry] = None
        self.operation_history = OperationHistory()

    async def connect(self, refresh: bool = False) -> None:
        """
        Connect to the database. If restart is True, drop existing collections and registry.
        """
        await self.connection.connect()

        if refresh:
            db = self.connection.client[self.name]
            collection_names = await db.list_collection_names()
            for collection_name in collection_names:
                collection = db[collection_name]
                try:
                    await collection.drop_search_index("*")
                except Exception as e:
                    logger.warning("Failed to drop search index on collection '%s': %s", collection_name, e)
            await self.connection.client.drop_database(self.name)
            logger.info("Dropped existing database '%s'", self.name)

        self._mongo_db = self.connection.client[self.name]

        self.registry = CollectionRegistry(self, self.embeddings)
        await self.registry.init_registry()
        logger.info("Connected to database '%s'", self.name)

    def get_collection(self, collection_definition: CollectionDefinition) -> Optional[Collection]:
        """
        Find a collection by its definition.
        """
        collection = Collection(collection_definition.name, self, self.embeddings, collection_definition.schema)
        return collection

    async def create_collection(self, name: str, schema: Dict[str, SchemaField], description: str) -> Collection:
        """
        Create a new collection with undo support.
        """
        operation = CreateCollectionOperation(self, name, schema, description)
        collection = await operation.execute()
        self.operation_history.push(operation)
        return collection

    async def drop_collection(self, name: str) -> None:
        """
        Drop a collection with undo support.
        """
        operation = DropCollectionOperation(self, name)
        await operation.execute()
        self.operation_history.push(operation)

    async def insert_document(self, collection: Collection, content: Dict[str, Any]) -> Document:
        """
        Insert a document with undo support.
        """
        operation = DocumentInsertOperation(self, collection, content)
        document = await operation.execute()
        self.operation_history.push(operation)
        return document

    async def insert_many_documents(self, collection: Collection, contents: List[Dict[str, Any]]) -> List[Document]:
        """
        Insert multiple documents with undo support.
        """
        operation = BulkInsertOperation(self, collection, contents)
        documents = await operation.execute()
        self.operation_history.push(operation)
        return documents

    async def update_document(self, document: Document, new_content: Dict[str, Any]) -> bool:
        """
        Update a document with undo support.
        """
        operation = DocumentUpdateOperation(self, document, new_content)
        success = await operation.execute()
        if success:
            self.operation_history.push(operation)
        return success

    async def update_many_documents(self, collection: Collection, documents: List[Document], update_dict: Dict[str, Any]) -> int:
        """
        Update multiple documents with undo support.
        """
        if not documents:
            return 0

        # Store original states for undo
        original_states = [(doc, doc.content.copy()) for doc in documents]

        try:
            # Create and execute bulk operation
            operation = BulkUpdateOperation(self, collection, original_states, update_dict)
            await operation.execute()
            self.operation_history.push(operation)
            return len(documents)

        except Exception as e:
            # Rollback changes if something goes wrong
            for doc, original_content in original_states:
                doc.content = original_content
                dict_data = await doc.to_dict()
                await collection._mongo_collection.replace_one({"_id": doc.id}, {**dict_data, "content": original_content})
            raise e

    async def delete_document(self, document: Document) -> bool:
        """
        Delete a document with undo support.
        """
        operation = DocumentDeleteOperation(self, document)
        success = await operation.execute()
        if success:
            self.operation_history.push(operation)
        return success

    async def delete_many_documents(self, collection: Collection, documents: List[Document]) -> int:
        """
        Delete multiple documents with undo support.
        """
        if not documents:
            return 0

        # Store documents for undo
        deleted_docs = [(doc, doc.content.copy()) for doc in documents]

        try:
            # Create and execute bulk operation
            operation = BulkDeleteOperation(self, collection, deleted_docs)
            await operation.execute()
            self.operation_history.push(operation)
            return len(documents)

        except Exception as e:
            # Rollback changes if something goes wrong
            for doc, content in deleted_docs:
                dict_data = await doc.to_dict()
                await collection._mongo_collection.insert_one({**dict_data, "content": content})
            raise e

    async def rename_collection(self, old_name: str, new_name: str) -> None:
        """
        Rename a collection with undo support.
        """
        operation = RenameCollectionOperation(self, old_name, new_name)
        await operation.execute()
        self.operation_history.push(operation)
        logger.info("Renamed collection '%s' to '%s'", old_name, new_name)

    async def add_fields(self, collection_name: str, new_fields: Dict[str, SchemaField]) -> None:
        """
        Add new fields to a collection's schema with undo support.
        """
        operation = AddFieldsOperation(self, collection_name, new_fields)
        await operation.execute()
        self.operation_history.push(operation)
        logger.info("Added fields %s to collection '%s'", list(new_fields.keys()), collection_name)

    async def delete_fields(self, collection_name: str, field_names: List[str]) -> None:
        """
        Delete fields from a collection's schema with undo support.
        """
        operation = DeleteFieldsOperation(self, collection_name, field_names)
        await operation.execute()
        self.operation_history.push(operation)
        logger.info("Deleted fields %s from collection '%s'", field_names, collection_name)

    async def undo(self) -> bool:
        """
        Undo the last operation.
        Returns True if the operation was successfully undone.
        """
        if not self.operation_history.can_undo():
            return False

        operation = self.operation_history.get_undo_operation()
        if not operation:
            return False

        try:
            await operation.undo()
            return True
        except Exception as e:
            logger.error(f"Failed to undo operation: {str(e)}")
            self.operation_history.current_index += 1  # Restore history state
            return False

    async def redo(self) -> bool:
        """
        Redo the last undone operation.
        Returns True if the operation was successfully redone.
        """
        if not self.operation_history.can_redo():
            return False

        operation = self.operation_history.get_redo_operation()
        if not operation:
            return False

        try:
            await operation.execute()
            return True
        except Exception as e:
            logger.error(f"Failed to redo operation: {str(e)}")
            self.operation_history.current_index -= 1  # Restore history state
            return False

    def clear_history(self) -> None:
        """
        Clear the operation history.
        """
        self.operation_history = OperationHistory()
        logger.info("Operation history cleared")
