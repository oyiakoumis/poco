from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo.operations import SearchIndexModel

from database_manager.document import Document
from database_manager.embedding_wrapper import EmbeddingWrapper
from database_manager.exceptions import ValidationError
from database_manager.query import Query
from database_manager.schema_field import SchemaField

if TYPE_CHECKING:
    from database_manager.database import Database


from dataclasses import dataclass
from typing import Dict


@dataclass
class Collection:
    """
    Represents a MongoDB collection with schema validation and vector search capabilities.
    Supports document operations with undo/redo functionality.
    """

    name: str
    database: "Database"
    embeddings: EmbeddingWrapper
    schema: Dict[str, SchemaField]

    def __post_init__(self):
        self._mongo_collection: AsyncIOMotorCollection = self.database._mongo_db[self.name]

    async def create_collection(self) -> None:
        """Initialize the collection if it doesn't exist with required indexes."""
        collection_names = await self.database._mongo_db.list_collection_names()
        if self.name not in collection_names:
            # Create unique index on name field
            await self._mongo_collection.create_index("name", unique=True)

            # Create vector search index for embeddings
            await self._create_vector_search_index()

    async def _create_vector_search_index(self) -> None:
        """Create the vector search index for embeddings."""
        search_index_model = SearchIndexModel(definition=self.embeddings.get_index_definition(), name=self.embeddings.config.index_name)
        await self._mongo_collection.create_search_index(search_index_model)

    async def get_all_documents(self) -> List[Document]:
        query = self.find()
        documents = await query.execute()
        return documents

    async def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Document]:
        """
        Find a single document matching the filter.

        Args:
            filter_dict: Filter criteria

        Returns:
            Optional[Document]: The matching document or None
        """
        result = await self._mongo_collection.find_one(filter_dict)
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

    async def search_similar(
        self, document: Document, num_results: int = 5, min_score: float = 0.0, filter_dict: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
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
        embedding = await document.embedding
        pipeline = self.embeddings.get_search_pipeline(query_vector=embedding, num_results=num_results, min_score=min_score, filter_dict=filter_dict)

        results = await self._mongo_collection.aggregate(pipeline).to_list(None)
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
