from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING

from models.schema import (
    AggregateFunction,
    AggregateMetric,
    AggregationQuery,
    CollectionSchema,
    DocumentQuery,
    FieldDefinition,
    FieldType,
)

class DocumentDB:
    """MongoDB-based document database with schema validation"""

    def __init__(self, database: AsyncIOMotorDatabase):
        self.database = database
        self._schemas_collection = database.get_collection("_schemas")

    def _get_collection_name(self, user_id: str, collection_name: str) -> str:
        """Get the full collection name including user prefix"""
        return f"{user_id}_{collection_name}"

    async def create_collection(
        self,
        user_id: str,
        schema: CollectionSchema
    ) -> None:
        """Create a new collection with schema validation"""
        full_name = self._get_collection_name(user_id, schema.name)
        
        # Check if collection already exists
        existing = await self._schemas_collection.find_one({
            "user_id": user_id,
            "name": schema.name
        })
        if existing:
            raise ValueError(f"Collection {schema.name} already exists")

        # Store schema
        await self._schemas_collection.insert_one({
            "user_id": user_id,
            "name": schema.name,
            "fields": [field.dict() for field in schema.fields],
            "description": schema.description
        })

        # Create collection
        await self.database.create_collection(full_name)

    async def update_collection(
        self,
        user_id: str,
        schema: CollectionSchema
    ) -> None:
        """Update an existing collection's schema"""
        full_name = self._get_collection_name(user_id, schema.name)
        
        # Check if collection exists
        existing = await self._schemas_collection.find_one({
            "user_id": user_id,
            "name": schema.name
        })
        if not existing:
            raise ValueError(f"Collection {schema.name} does not exist")

        # Update schema
        await self._schemas_collection.update_one(
            {
                "user_id": user_id,
                "name": schema.name
            },
            {
                "$set": {
                    "fields": [field.dict() for field in schema.fields],
                    "description": schema.description
                }
            }
        )

    async def delete_collection(
        self,
        user_id: str,
        collection_name: str
    ) -> None:
        """Delete a collection and its schema"""
        full_name = self._get_collection_name(user_id, collection_name)
        
        # Delete schema
        result = await self._schemas_collection.delete_one({
            "user_id": user_id,
            "name": collection_name
        })
        if result.deleted_count == 0:
            raise ValueError(f"Collection {collection_name} does not exist")

        # Delete collection
        await self.database.drop_collection(full_name)

    async def list_collections(
        self,
        user_id: str
    ) -> List[Dict]:
        """List all collections for a user"""
        cursor = self._schemas_collection.find({"user_id": user_id})
        collections = []
        async for doc in cursor:
            collections.append({
                "name": doc["name"],
                "fields": doc["fields"],
                "description": doc.get("description", "")
            })
        return collections

    async def get_schema(
        self,
        user_id: str,
        collection_name: str
    ) -> CollectionSchema:
        """Get the schema for a collection"""
        schema_doc = await self._schemas_collection.find_one({
            "user_id": user_id,
            "name": collection_name
        })
        if not schema_doc:
            raise ValueError(f"Collection {collection_name} does not exist")

        return CollectionSchema(
            name=schema_doc["name"],
            fields=[FieldDefinition(**field) for field in schema_doc["fields"]],
            description=schema_doc.get("description", "")
        )

    def _validate_document(
        self,
        document: Dict,
        schema: CollectionSchema,
        partial: bool = False
    ) -> Dict:
        """Validate and convert document fields according to schema"""
        validated = {}
        
        # Track required fields
        required_fields = {
            field.name for field in schema.fields 
            if field.required and not field.default
        }

        for field in schema.fields:
            value = document.get(field.name)
            
            # Handle missing fields
            if value is None:
                if field.default is not None:
                    # Use default value
                    if callable(field.default):
                        value = field.default()
                    else:
                        value = field.default
                elif field.required and not partial:
                    raise ValueError(f"Required field {field.name} is missing")
                else:
                    continue

            # Validate field value
            if field.field_type == FieldType.STRING:
                if not isinstance(value, str):
                    raise ValueError(f"Field {field.name} must be a string")
                
            elif field.field_type == FieldType.INTEGER:
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    raise ValueError(f"Field {field.name} must be an integer")
                value = int(value)
                
            elif field.field_type == FieldType.FLOAT:
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    raise ValueError(f"Field {field.name} must be a number")
                value = float(value)
                
            elif field.field_type == FieldType.BOOLEAN:
                if not isinstance(value, bool):
                    raise ValueError(f"Field {field.name} must be a boolean")
                
            elif field.field_type == FieldType.DATETIME:
                if isinstance(value, str):
                    try:
                        value = datetime.fromisoformat(value)
                    except ValueError:
                        raise ValueError(f"Field {field.name} must be a valid ISO datetime string")
                elif not isinstance(value, datetime):
                    raise ValueError(f"Field {field.name} must be a datetime")
                
            elif field.field_type == FieldType.SELECT:
                if not isinstance(value, str):
                    raise ValueError(f"Field {field.name} must be a string")
                if field.options and value not in field.options:
                    raise ValueError(f"Field {field.name} must be one of: {field.options}")
                    
            elif field.field_type == FieldType.MULTI_SELECT:
                if not isinstance(value, list):
                    raise ValueError(f"Field {field.name} must be a list")
                if not all(isinstance(v, str) for v in value):
                    raise ValueError(f"Field {field.name} must be a list of strings")
                if field.options and not all(v in field.options for v in value):
                    raise ValueError(f"Field {field.name} values must be from: {field.options}")

            validated[field.name] = value
            if field.name in required_fields:
                required_fields.remove(field.name)

        # Check if any required fields are missing
        if required_fields and not partial:
            raise ValueError(f"Required fields missing: {required_fields}")

        return validated

    async def create_document(
        self,
        user_id: str,
        collection_name: str,
        document: Dict
    ) -> str:
        """Create a new document in a collection"""
        schema = await self.get_schema(user_id, collection_name)
        validated = self._validate_document(document, schema)
        
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        result = await collection.insert_one(validated)
        return str(result.inserted_id)

    async def get_document(
        self,
        user_id: str,
        collection_name: str,
        document_id: str
    ) -> Optional[Dict]:
        """Get a document by ID"""
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        document = await collection.find_one({"_id": document_id})
        return document

    async def update_document(
        self,
        user_id: str,
        collection_name: str,
        document_id: str,
        updates: Dict
    ) -> None:
        """Update a document by ID"""
        schema = await self.get_schema(user_id, collection_name)
        validated = self._validate_document(updates, schema, partial=True)
        
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        result = await collection.update_one(
            {"_id": document_id},
            {"$set": validated}
        )
        if result.matched_count == 0:
            raise ValueError(f"Document {document_id} not found")

    async def delete_document(
        self,
        user_id: str,
        collection_name: str,
        document_id: str
    ) -> None:
        """Delete a document by ID"""
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        result = await collection.delete_one({"_id": document_id})
        if result.deleted_count == 0:
            raise ValueError(f"Document {document_id} not found")

    async def query_documents(
        self,
        user_id: str,
        collection_name: str,
        query: DocumentQuery
    ) -> List[Dict]:
        """Query documents with filtering and sorting"""
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        
        # Build query
        find_query = {}
        if query.filter:
            find_query.update(query.filter)
            
        # Build sort
        sort_list = []
        if query.sort:
            for field, direction in query.sort.items():
                sort_list.append(
                    (field, ASCENDING if direction > 0 else DESCENDING)
                )
                
        # Execute query
        cursor = collection.find(find_query)
        if sort_list:
            cursor = cursor.sort(sort_list)
        if query.skip:
            cursor = cursor.skip(query.skip)
        if query.limit:
            cursor = cursor.limit(query.limit)
            
        # Collect results
        documents = []
        async for doc in cursor:
            documents.append(doc)
        return documents

    async def update_documents(
        self,
        user_id: str,
        collection_name: str,
        query: DocumentQuery,
        updates: Dict
    ) -> int:
        """Update multiple documents matching a query"""
        schema = await self.get_schema(user_id, collection_name)
        validated = self._validate_document(updates, schema, partial=True)
        
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        
        # Build query
        update_query = {}
        if query.filter:
            update_query.update(query.filter)
            
        # Execute update
        result = await collection.update_many(
            update_query,
            {"$set": validated}
        )
        return result.modified_count

    async def delete_documents(
        self,
        user_id: str,
        collection_name: str,
        query: DocumentQuery
    ) -> int:
        """Delete multiple documents matching a query"""
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        
        # Build query
        delete_query = {}
        if query.filter:
            delete_query.update(query.filter)
            
        # Execute delete
        result = await collection.delete_many(delete_query)
        return result.deleted_count

    async def aggregate_documents(
        self,
        user_id: str,
        collection_name: str,
        query: AggregationQuery
    ) -> List[Dict]:
        """Aggregate documents with grouping and metrics"""
        collection = self.database.get_collection(
            self._get_collection_name(user_id, collection_name)
        )
        
        # Build pipeline
        pipeline = []
        
        # Add match stage if filters specified
        if query.filter:
            pipeline.append({"$match": query.filter})
            
        # Build group stage
        group_stage = {
            "_id": {},
            "count": {"$sum": 1}
        }
        
        # Add grouping fields
        for field in query.group_by:
            group_stage["_id"][field] = f"${field}"
            
        # Add metrics
        for metric in query.metrics:
            if metric.function == AggregateFunction.SUM:
                group_stage[f"{metric.field}_sum"] = {"$sum": f"${metric.field}"}
            elif metric.function == AggregateFunction.AVERAGE:
                group_stage[f"{metric.field}_avg"] = {"$avg": f"${metric.field}"}
            elif metric.function == AggregateFunction.MAX:
                group_stage[f"{metric.field}_max"] = {"$max": f"${metric.field}"}
            elif metric.function == AggregateFunction.MIN:
                group_stage[f"{metric.field}_min"] = {"$min": f"${metric.field}"}
                
        pipeline.append({"$group": group_stage})
        
        # Execute pipeline
        results = []
        async for result in collection.aggregate(pipeline):
            results.append(result)
        return results
