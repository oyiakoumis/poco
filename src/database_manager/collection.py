from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from langchain_core.embeddings import Embeddings
from pymongo.collection import Collection as MongoCollection
from pymongo.operations import SearchIndexModel

from database_manager.document import Document
from database_manager.exceptions import ValidationError
from database_manager.query import Query
from database_manager.schema_field import SchemaField

if TYPE_CHECKING:
    from database_manager.database import Database


class Collection:
    # TODO: Create an Embedding class to store that
    EMBEDDING_DIMENSION = 1536
    EMBEDDING_INDEX_NAME = "embedding_index"

    def __init__(self, name: str, database: "Database", embeddings: Embeddings, schema: Dict[str, SchemaField]):
        self.name = name
        self.database = database
        self.embeddings = embeddings
        self.schema = schema
        self._mongo_collection: MongoCollection = self.database._mongo_db[self.name]

    def create_collection(self):
        """Initialize the collection if it doesn't exist"""
        if self.name not in self.database._mongo_db.list_collection_names():
            self._mongo_collection.create_index("name", unique=True)

            search_index_definition = {
                "mappings": {
                    "dynamic": True,
                    "fields": {Document.EMBEDDING_FIELD_NAME: {"type": "knnVector", "dimensions": self.EMBEDDING_DIMENSION, "similarity": "cosine"}},
                }
            }
            search_index_model = SearchIndexModel(definition=search_index_definition, name=self.EMBEDDING_INDEX_NAME)
            self._mongo_collection.create_search_index(search_index_model)

    def insert_one(self, content: Dict[str, Any]) -> Document:
        self.validate_document(content)
        document = Document(content, self)
        result = self._mongo_collection.insert_one(document.to_dict())
        document.id = result.inserted_id
        return document

    def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Document]:
        result = self._mongo_collection.find_one(filter_dict)
        if result:
            return Document.from_dict(result, self)
        return None

    def find(self, filter_dict: Dict[str, Any] = None) -> Query:
        query = Query(self)
        if filter_dict:
            query.filter(filter_dict)
        return query

    def search_similar(self, document: Document, num_results: int = 5, min_score: float = 0.0) -> List[Document]:
        pipeline = [
            {
                "$vectorSearch": {
                    "index": self.EMBEDDING_INDEX_NAME,
                    "path": Document.EMBEDDING_FIELD_NAME,
                    "queryVector": document.embedding,
                    "numCandidates": num_results * 10,  # Adjust as needed
                    "limit": num_results,
                    "exact": False,  # Set to True for exact nearest neighbor search
                }
            },
            {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
            {"$match": {"score": {"$gte": min_score}}},
            {"$project": {"score": 0}},  # Exclude fields from output
        ]

        results = list(self._mongo_collection.aggregate(pipeline))

        return [Document.from_dict(data, self) for data in results]

    def validate_document(self, document: Dict[str, Any]) -> None:
        for field_name, field_schema in self.schema.items():
            if field_name in document:
                field_schema.validate(document[field_name])
            elif field_schema.required:
                raise ValidationError(f"Required field {field_name} is missing")

    def _execute_query(self, query: Query) -> List[Document]:
        cursor = self._mongo_collection.find(query.filters)

        if query.sort_fields:
            cursor = cursor.sort(query.sort_fields)

        if query.limit_val:
            cursor = cursor.limit(query.limit_val)

        return [Document.from_dict(doc, self) for doc in cursor]
