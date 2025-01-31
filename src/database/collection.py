from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection as MongoCollection

from database.document import Document
from database.exceptions import ValidationError
from database.query import Query
from database.schema_field import SchemaField

from typing import TYPE_CHECKING

from __future__ import annotations

if TYPE_CHECKING:
    from database.database import Database


class Collection:
    def __init__(self, name: str, database: "Database", schema: Dict[str, SchemaField] = None):
        self.name = name
        self.database = database
        self.schema = schema or {}
        self._mongo_collection: MongoCollection = database._mongo_db[name]

    def insert_one(self, document: Dict[str, Any]) -> Document:
        self.validate_document(document)
        document["created_at"] = datetime.now()
        document["updated_at"] = datetime.now()

        result = self._mongo_collection.insert_one(document)
        document["_id"] = result.inserted_id
        return Document(document, self)

    def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Document]:
        result = self._mongo_collection.find_one(filter_dict)
        if result:
            return Document(result, self)
        return None

    def find(self, filter_dict: Dict[str, Any] = None) -> Query:
        query = Query(self)
        if filter_dict:
            query.filter(filter_dict)
        return query

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

        return [Document(doc, self) for doc in cursor]
