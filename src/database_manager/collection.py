from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING
from langchain_core.embeddings import Embeddings
from pymongo.collection import Collection as MongoCollection
from pymongo.operations import SearchIndexModel

from database_manager.document import Document
from database_manager.operations.document_operations import BulkDeleteOperation, BulkUpdateOperation
from database_manager.exceptions import ValidationError
from database_manager.query import Query
from database_manager.schema_field import SchemaField

if TYPE_CHECKING:
    from database_manager.database import Database


class Collection:
    """
    Represents a MongoDB collection with schema validation and vector search capabilities.
    Supports document operations with undo/redo functionality.
    """

    EMBEDDING_DIMENSION = 1536
    EMBEDDING_INDEX_NAME = "embedding_index"

    embeddings: Embeddings

    def __init__(self, name: str, database: "Database", embeddings: Embeddings, schema: Dict[str, SchemaField]):
        self.name = name
        self.database = database
        self.embeddings = embeddings
        self.schema = schema
        self._mongo_collection: MongoCollection = self.database._mongo_db[self.name]

    def create_collection(self) -> None:
        """Initialize the collection if it doesn't exist with required indexes."""
        if self.name not in self.database._mongo_db.list_collection_names():
            # Create unique index on name field
            self._mongo_collection.create_index("name", unique=True)

            # Create vector search index for embeddings
            self._create_vector_search_index()

    def _create_vector_search_index(self) -> None:
        """Create the vector search index for embeddings."""
        search_index_definition = {
            "mappings": {
                "dynamic": True,
                "fields": {Document.EMBEDDING_FIELD_NAME: {"type": "knnVector", "dimensions": self.EMBEDDING_DIMENSION, "similarity": "cosine"}},
            }
        }
        search_index_model = SearchIndexModel(definition=search_index_definition, name=self.EMBEDDING_INDEX_NAME)
        self._mongo_collection.create_search_index(search_index_model)

    def insert_one(self, content: Dict[str, Any]) -> Document:
        """
        Insert a document with undo support.

        Args:
            content: Document content that matches the collection's schema

        Returns:
            Document: The newly created document

        Raises:
            ValidationError: If the document doesn't match the schema
        """
        return self.database.insert_document(self, content)

    def insert_many(self, contents: List[Dict[str, Any]]) -> List[Document]:
        """
        Insert multiple documents with undo support.

        Args:
            contents: List of document contents that match the collection's schema

        Returns:
            List[Document]: The newly created documents

        Raises:
            ValidationError: If any document doesn't match the schema
        """
        return [self.insert_one(content) for content in contents]

    def update_one(self, document: Document, new_content: Dict[str, Any]) -> bool:
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
        return self.database.update_document(document, new_content)

    def update_many(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> int:
        """
        Update multiple documents matching the filter with undo support.

        Args:
            filter_dict: Filter to select documents
            update_dict: Update operations to apply

        Returns:
            int: Number of documents updated

        Raises:
            ValidationError: If the updates don't match the schema
        """
        # First get all documents that will be affected
        query = self.find(filter_dict)
        documents = query.execute()

        if not documents:
            return 0

        # Validate the update_dict against schema for all affected fields
        for field_name in update_dict:
            if field_name in self.schema:
                self.schema[field_name].validate(update_dict[field_name])

        # Store original states for undo
        original_states = [(doc, doc.content.copy()) for doc in documents]

        try:
            # Create bulk operation
            operation = BulkUpdateOperation(self.database, self, original_states, update_dict)

            # Execute the update
            operation.execute()

            # Add to operation history if successful
            self.database.operation_history.push(operation.get_state())

            return len(documents)

        except Exception as e:
            # Rollback changes if something goes wrong
            for doc, original_content in original_states:
                doc.content = original_content
                self._mongo_collection.replace_one({"_id": doc.id}, {**doc.to_dict(), "content": original_content})
            raise e

    def delete_one(self, document: Document) -> bool:
        """
        Delete a document with undo support.

        Args:
            document: The document to delete

        Returns:
            bool: True if deletion was successful
        """
        return self.database.delete_document(document)

    def delete_many(self, filter_dict: Dict[str, Any]) -> int:
        """
        Delete multiple documents matching the filter with undo support.

        Args:
            filter_dict: Filter to select documents to delete

        Returns:
            int: Number of documents deleted
        """
        # First get all documents that will be affected
        query = self.find(filter_dict)
        documents = query.execute()

        if not documents:
            return 0

        # Store documents and their content for undo
        deleted_docs = [(doc, doc.content.copy()) for doc in documents]

        try:
            # Create bulk operation
            operation = BulkDeleteOperation(self.database, self, deleted_docs)

            # Execute the deletion
            operation.execute()

            # Add to operation history if successful
            self.database.operation_history.push(operation.get_state())

            return len(documents)

        except Exception as e:
            # Rollback changes if something goes wrong
            for doc, content in deleted_docs:
                self._mongo_collection.insert_one({**doc.to_dict(), "content": content})
            raise e

    def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Document]:
        """
        Find a single document matching the filter.

        Args:
            filter_dict: Filter criteria

        Returns:
            Optional[Document]: The matching document or None
        """
        result = self._mongo_collection.find_one(filter_dict)
        if result:
            return Document.from_dict(result, self)
        return None

    def find(self, filter_dict: Dict[str, Any] = None) -> Query:
        """
        Create a query object for finding documents.

        Args:
            filter_dict: Optional filter criteria

        Returns:
            Query: Query object for building and executing the query
        """
        query = Query(self)
        if filter_dict:
            query.filter(filter_dict)
        return query

    def search_similar(self, document: Document, num_results: int = 5, min_score: float = 0.0, filter_dict: Optional[Dict[str, Any]] = None) -> List[Document]:
        """
        Search for documents with similar embeddings.

        Args:
            document: Document to find similar documents to
            num_results: Maximum number of results to return
            min_score: Minimum similarity score (0-1)
            filter_dict: Optional additional filter criteria

        Returns:
            List[Document]: List of similar documents
        """
        pipeline = [
            {
                "$vectorSearch": {
                    "index": self.EMBEDDING_INDEX_NAME,
                    "path": Document.EMBEDDING_FIELD_NAME,
                    "queryVector": document.embedding,
                    "numCandidates": num_results * 10,
                    "limit": num_results,
                    "exact": False,
                }
            },
            {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
            {"$match": {"score": {"$gte": min_score}}},
        ]

        # Add additional filter if provided
        if filter_dict:
            pipeline.append({"$match": filter_dict})

        pipeline.append({"$project": {"score": 0}})

        results = list(self._mongo_collection.aggregate(pipeline))
        return [Document.from_dict(data, self) for data in results]

    def validate_document(self, document: Dict[str, Any]) -> None:
        """
        Validate a document against the collection's schema.

        Args:
            document: Document content to validate

        Raises:
            ValidationError: If the document doesn't match the schema
        """
        for field_name, field_schema in self.schema.items():
            if field_name in document:
                field_schema.validate(document[field_name])
            elif field_schema.required:
                raise ValidationError(f"Required field {field_name} is missing")

    def add_fields(self, new_fields: Dict[str, SchemaField]) -> None:
        """
        Add new fields to the collection's schema.

        Args:
            new_fields: Dictionary of field name to SchemaField
        """
        self.database.add_fields(self.name, new_fields)

    def delete_fields(self, field_names: List[str]) -> None:
        """
        Delete fields from the collection's schema.

        Args:
            field_names: List of field names to delete
        """
        self.database.delete_fields(self.name, field_names)

    def rename(self, new_name: str) -> None:
        """
        Rename this collection.

        Args:
            new_name: New name for the collection
        """
        self.database.rename_collection(self.name, new_name)
        self.name = new_name
        self._mongo_collection = self.database._mongo_db[self.name]
