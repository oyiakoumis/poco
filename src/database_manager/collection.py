from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING
from langchain_core.embeddings import Embeddings
from pymongo.collection import Collection as MongoCollection
from pymongo.operations import SearchIndexModel

from database_manager.document import Document
from database_manager.exceptions import ValidationError
from database_manager.query import Query
from database_manager.schema_field import SchemaField
from database_manager.embedding_wrapper import EmbeddingWrapper

if TYPE_CHECKING:
    from database_manager.database import Database


class Collection:
    """
    Represents a MongoDB collection with schema validation and vector search capabilities.
    Supports document operations with undo/redo functionality.
    """

    def __init__(self, name: str, database: "Database", embeddings: EmbeddingWrapper, schema: Dict[str, SchemaField]):
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
        search_index_model = SearchIndexModel(definition=self.embeddings.get_index_definition(), name=self.embeddings.config.index_name)
        self._mongo_collection.create_search_index(search_index_model)

    def get_all_documents(self) -> List[Document]:
        query = self.find()
        documents = query.execute()

        return documents

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
        pipeline = self.embeddings.get_search_pipeline(query_vector=document.embedding, num_results=num_results, min_score=min_score, filter_dict=filter_dict)

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
