from typing import Dict, List, Optional, Any
import logging
from pymongo.database import Database as MongoDatabase

from database_manager.collection import Collection
from database_manager.collection_definition import CollectionDefinition
from database_manager.embedding_wrapper import EmbeddingWrapper
from database_manager.operations.collection_operations import (
    AddFieldsOperation,
    CreateCollectionOperation,
    DeleteFieldsOperation,
    DropCollectionOperation,
    RenameCollectionOperation,
)
from database_manager.collection_registry import CollectionRegistry
from database_manager.connection import Connection
from database_manager.document import Document
from database_manager.operations.operation_history import OperationHistory
from database_manager.schema_field import SchemaField
from database_manager.operations.document_operations import (
    BulkDeleteOperation,
    BulkUpdateOperation,
    DocumentInsertOperation,
    DocumentUpdateOperation,
    DocumentDeleteOperation,
    BulkInsertOperation,
)

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
        self._mongo_db: Optional[MongoDatabase] = None
        self.registry: Optional[CollectionRegistry] = None
        self.operation_history = OperationHistory()

    def connect(self, restart: bool = False) -> None:
        """
        Connect to the database. If restart is True, drop existing collections and registry.
        """
        self.connection.connect()

        if restart:
            db = self.connection.client[self.name]
            for collection_name in db.list_collection_names():
                collection = db[collection_name]
                try:
                    collection.drop_search_index("*")
                except Exception as e:
                    logger.warning("Failed to drop search index on collection '%s': %s", collection_name, e)
            self.connection.client.drop_database(self.name)
            logger.info("Dropped existing database '%s'", self.name)

        self._mongo_db = self.connection.client[self.name]

        self.registry = CollectionRegistry(self, self.embeddings)
        self.registry.init_registry()
        logger.info("Connected to database '%s'", self.name)

    def get_collection(self, collection_definition: CollectionDefinition) -> Optional[Collection]:
        """
        Find a collection by its definition.
        """
        collection = Collection(collection_definition.name, self, self.embeddings, collection_definition.schema)

        return collection

    def create_collection(self, name: str, schema: Dict[str, SchemaField], description: str) -> Collection:
        """
        Create a new collection with undo support.
        """
        operation = CreateCollectionOperation(self, name, schema, description)
        collection = operation.execute()
        self.operation_history.push(operation)
        return collection

    def drop_collection(self, name: str) -> None:
        """
        Drop a collection with undo support.
        """
        operation = DropCollectionOperation(self, name)
        operation.execute()
        self.operation_history.push(operation)

    def insert_document(self, collection: Collection, content: Dict[str, Any]) -> Document:
        """
        Insert a document with undo support.
        """
        operation = DocumentInsertOperation(self, collection, content)
        document = operation.execute()
        self.operation_history.push(operation)
        return document

    def insert_many_documents(self, collection: Collection, contents: List[Dict[str, Any]]) -> List[Document]:
        """
        Insert multiple documents with undo support.

        Args:
            collection: Collection to insert into
            contents: List of document contents that match the collection's schema

        Returns:
            List[Document]: The newly created documents

        Raises:
            ValidationError: If any document doesn't match the schema
        """
        operation = BulkInsertOperation(self, collection, contents)
        documents = operation.execute()
        self.operation_history.push(operation)
        return documents

    def update_document(self, document: Document, new_content: Dict[str, Any]) -> bool:
        """
        Update a document with undo support.

        Args:
            document: The document to update
            new_content: New content that matches the collection's schema

        Returns:
            bool: True if update was successful

        Raises:
            ValidationError: If the new content doesn't match the schema
        """
        operation = DocumentUpdateOperation(self, document, new_content)
        success = operation.execute()
        if success:
            self.operation_history.push(operation)
        return success

    def update_many_documents(self, collection: Collection, documents: List[Document], update_dict: Dict[str, Any]) -> int:
        """
        Update multiple documents with undo support.

        Args:
            collection: Collection containing the documents
            documents: List of documents to update
            update_dict: Update operations to apply

        Returns:
            int: Number of documents updated

        Raises:
            ValidationError: If the updates don't match the schema
        """
        if not documents:
            return 0

        # Store original states for undo
        original_states = [(doc, doc.content.copy()) for doc in documents]

        try:
            # Create and execute bulk operation
            operation = BulkUpdateOperation(self, collection, original_states, update_dict)
            operation.execute()
            self.operation_history.push(operation)
            return len(documents)

        except Exception as e:
            # Rollback changes if something goes wrong
            for doc, original_content in original_states:
                doc.content = original_content
                collection._mongo_collection.replace_one({"_id": doc.id}, {**doc.to_dict(), "content": original_content})
            raise e

    def delete_document(self, document: Document) -> bool:
        """
        Delete a document with undo support.

        Args:
            document: The document to delete

        Returns:
            bool: True if deletion was successful
        """
        operation = DocumentDeleteOperation(self, document)
        success = operation.execute()
        if success:
            self.operation_history.push(operation)
        return success

    def delete_many_documents(self, collection: Collection, documents: List[Document]) -> int:
        """
        Delete multiple documents with undo support.

        Args:
            collection: Collection containing the documents
            documents: List of documents to delete

        Returns:
            int: Number of documents deleted
        """
        if not documents:
            return 0

        # Store documents for undo
        deleted_docs = [(doc, doc.content.copy()) for doc in documents]

        try:
            # Create and execute bulk operation
            operation = BulkDeleteOperation(self, collection, deleted_docs)
            operation.execute()
            self.operation_history.push(operation)
            return len(documents)

        except Exception as e:
            # Rollback changes if something goes wrong
            for doc, content in deleted_docs:
                collection._mongo_collection.insert_one({**doc.to_dict(), "content": content})
            raise e

    def rename_collection(self, old_name: str, new_name: str) -> None:
        """
        Rename a collection with undo support.
        """
        operation = RenameCollectionOperation(self, old_name, new_name)
        operation.execute()
        self.operation_history.push(operation)
        logger.info("Renamed collection '%s' to '%s'", old_name, new_name)

    def add_fields(self, collection_name: str, new_fields: Dict[str, SchemaField]) -> None:
        """
        Add new fields to a collection's schema with undo support.

        Args:
            collection_name: Name of the collection to modify
            new_fields: Dictionary of field names to SchemaField objects
        """
        operation = AddFieldsOperation(self, collection_name, new_fields)
        operation.execute()
        self.operation_history.push(operation)
        logger.info("Added fields %s to collection '%s'", list(new_fields.keys()), collection_name)

    def delete_fields(self, collection_name: str, field_names: List[str]) -> None:
        """
        Delete fields from a collection's schema with undo support.

        Args:
            collection_name: Name of the collection to modify
            field_names: List of field names to delete
        """
        operation = DeleteFieldsOperation(self, collection_name, field_names)
        operation.execute()
        self.operation_history.push(operation)
        logger.info("Deleted fields %s from collection '%s'", field_names, collection_name)

    def undo(self) -> bool:
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
            operation.undo()
            return True
        except Exception as e:
            logger.error(f"Failed to undo operation: {str(e)}")
            self.operation_history.current_index += 1  # Restore history state
            return False

    def redo(self) -> bool:
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
            operation.execute()
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
