"""Dataset manager for MongoDB operations."""

from __future__ import annotations

from asyncio import sleep
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

import pymongo
from langchain_openai import OpenAIEmbeddings
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo.errors import BulkWriteError
from pymongo.operations import SearchIndexModel

from database.document_store.exceptions import (
    DatabaseError,
    DatasetNameExistsError,
    DatasetNotFoundError,
    InvalidDatasetSchemaError,
    InvalidRecordDataError,
    RecordNotFoundError,
)
from database.document_store.models import Dataset, Record
from database.document_store.models.field import SchemaField
from database.document_store.models.query import RecordQuery
from database.document_store.models.record import RecordData
from database.document_store.models.schema import DatasetSchema
from database.document_store.models.types import FieldType, TypeRegistry
from database.document_store.pipeline import build_aggregation_pipeline
from utils.logging import logger

if TYPE_CHECKING:
    from agents.tools.database_operator import RecordUpdate


class IndexStatus(Enum):
    """MongoDB Atlas Search index status."""

    BUILDING = "BUILDING"  # Building or re-building index
    DOES_NOT_EXIST = "DOES_NOT_EXIST"  # Index does not exist
    DELETING = "DELETING"  # Index is being deleted
    FAILED = "FAILED"  # Index build failed
    PENDING = "PENDING"  # Not yet started building
    READY = "READY"  # Index is ready and queryable
    STALE = "STALE"  # Queryable but out of date


class DatasetManager:
    """Manager for dataset and record operations."""

    DATABASE: str = "document_store"
    COLLECTION_DATASETS: str = "datasets"
    COLLECTION_RECORDS: str = "records"

    # Vector search configuration
    VECTOR_SEARCH_CONFIG = {
        "MODEL": "text-embedding-3-small",
        "INDEX_NAME": "vector_search_datasets",
        "FIELD_NAME": "embedding",
        "DIMENSION": 1536,  # 1536 for text-embedding-3-small and 3072 for text-embedding-3-large
        "NUM_CANDIDATES_MULTIPLIER": 10,
        "MIN_SCORE": 0.7,
    }

    def __init__(self, mongodb_client: AsyncIOMotorClient) -> None:
        """Initialize manager with MongoDB client.
        Note: Use DatasetManager.setup() to create a properly initialized instance."""
        self.client = mongodb_client
        self._db: AsyncIOMotorDatabase = self.client.get_database(self.DATABASE)
        self._datasets: AsyncIOMotorCollection = self._db.get_collection(self.COLLECTION_DATASETS)
        self._records: AsyncIOMotorCollection = self._db.get_collection(self.COLLECTION_RECORDS)
        self.embeddings_model = OpenAIEmbeddings(model=self.VECTOR_SEARCH_CONFIG["MODEL"], dimensions=self.VECTOR_SEARCH_CONFIG["DIMENSION"])

    async def _create_vector_search_index_generic(self, collection: AsyncIOMotorCollection, index_name: str, entity_type: str, dimension: int) -> None:
        """Create vector search index if it doesn't exist and ensure it's ready."""
        try:
            logger.info(f"Checking {entity_type} vector search index status")
            # Check current index status
            status = await self._get_index_status_generic(collection, index_name, entity_type)

            if status == IndexStatus.READY:
                logger.info(f"{entity_type.capitalize()} vector search index is ready")
                return  # Index exists and is ready
            elif status == IndexStatus.FAILED:
                # Drop failed index to recreate it
                logger.warning(f"{entity_type.capitalize()} vector search index failed, dropping to recreate")
                await self._delete_vector_search_index_generic(collection, index_name, entity_type)
            elif status == IndexStatus.STALE:
                logger.warning(f"{entity_type.capitalize()} vector search index is stale, dropping to recreate")
                await self._delete_vector_search_index_generic(collection, index_name, entity_type)
            elif status in (IndexStatus.BUILDING, IndexStatus.PENDING):
                # Wait for existing index to be ready
                logger.info(f"Waiting for existing {entity_type} vector search index to be ready")
                await self._wait_for_index_ready_generic(collection, index_name, entity_type)
                return
            elif status == IndexStatus.DELETING:
                logger.info(f"Waiting for {entity_type} vector search index deletion to complete")
                await sleep(5)  # Wait a bit before creating new index

            # Create new index if it doesn't exist or was dropped
            logger.info(f"Creating new {entity_type} vector search index")
            index_definition = {
                "mappings": {
                    "dynamic": True,
                    "fields": {
                        self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: {
                            "type": "knnVector",
                            "dimensions": dimension,
                            "similarity": "cosine",
                        }
                    },
                }
            }

            # Create index
            search_index = SearchIndexModel(definition=index_definition, name=index_name)
            await collection.create_search_index(search_index)

            # Wait for index to be ready
            logger.info(f"Waiting for new {entity_type} vector search index to be ready")
            await self._wait_for_index_ready_generic(collection, index_name, entity_type)
            logger.info(f"{entity_type.capitalize()} vector search index is ready")

        except Exception as e:
            raise DatabaseError(f"Failed to create {entity_type} vector search index: {str(e)}")

    async def _delete_vector_search_index_generic(self, collection: AsyncIOMotorCollection, index_name: str, entity_type: str) -> None:
        """Delete vector search index and wait until it's confirmed to be deleted."""
        try:
            # Start deletion
            await collection.drop_search_index(index_name)

            # Wait for deletion to complete
            MAX_POLL_ATTEMPTS = 30  # 1 minute total
            POLL_INTERVAL_SECONDS = 2

            attempts = 0
            while attempts < MAX_POLL_ATTEMPTS:
                status = await self._get_index_status_generic(collection, index_name, entity_type)
                if status == IndexStatus.DOES_NOT_EXIST:
                    return
                elif status == IndexStatus.DELETING:
                    await sleep(POLL_INTERVAL_SECONDS)
                else:
                    raise DatabaseError(f"Unexpected {entity_type} index status during deletion: {status}")
                attempts += 1

            raise DatabaseError(f"{entity_type.capitalize()} index deletion not complete after {MAX_POLL_ATTEMPTS} attempts")

        except Exception as e:
            raise DatabaseError(f"Failed to delete {entity_type} vector search index: {str(e)}")

    async def _get_index_status_generic(self, collection: AsyncIOMotorCollection, index_name: str, entity_type: str) -> IndexStatus:
        """Get current status of the vector search index."""
        try:
            indexes = await collection.list_search_indexes().to_list(None)
            for index in indexes:
                if index["name"] == index_name:
                    status = index.get("status", "")
                    try:
                        return IndexStatus(status)
                    except ValueError:
                        print(f"Warning: Unknown {entity_type} index status: {status}")
                        return IndexStatus.FAILED
            return IndexStatus.DOES_NOT_EXIST
        except Exception as e:
            raise DatabaseError(f"Failed to get {entity_type} index status: {str(e)}")

    async def _wait_for_index_ready_generic(self, collection: AsyncIOMotorCollection, index_name: str, entity_type: str) -> None:
        """Poll index status until ready or max attempts reached."""
        MAX_POLL_ATTEMPTS = 30  # 1 minute total
        POLL_INTERVAL_SECONDS = 2

        attempts = 0
        while attempts < MAX_POLL_ATTEMPTS:
            status = await self._get_index_status_generic(collection, index_name, entity_type)

            if status == IndexStatus.READY:
                return
            elif status == IndexStatus.FAILED:
                raise DatabaseError(f"{entity_type.capitalize()} vector search index creation failed")
            elif status == IndexStatus.STALE:
                print(f"Warning: {entity_type.capitalize()} index is stale, may return out-of-date results")
                return
            elif status == IndexStatus.DELETING:
                await sleep(POLL_INTERVAL_SECONDS * 2)  # Wait longer for deletion
            elif status == IndexStatus.BUILDING or status == IndexStatus.PENDING:
                await sleep(POLL_INTERVAL_SECONDS)
            else:
                raise DatabaseError(f"Unexpected {entity_type} index status: {status}")

            attempts += 1

        raise DatabaseError(f"{entity_type.capitalize()} index not ready after {MAX_POLL_ATTEMPTS} attempts")

    # Wrapper methods for backward compatibility and specific entity types

    async def _create_dataset_vector_search_index(self) -> None:
        """Create vector search index for datasets if it doesn't exist and ensure it's ready."""
        await self._create_vector_search_index_generic(
            collection=self._datasets,
            index_name=self.VECTOR_SEARCH_CONFIG["INDEX_NAME"],
            entity_type="dataset",
            dimension=self.VECTOR_SEARCH_CONFIG["DIMENSION"],
        )

    async def _create_record_vector_search_index(self) -> None:
        """Create vector search index for records if it doesn't exist and ensure it's ready."""
        await self._create_vector_search_index_generic(
            collection=self._records, index_name="vector_search_records", entity_type="record", dimension=self.VECTOR_SEARCH_CONFIG["DIMENSION"]
        )

    async def _generate_dataset_embedding(self, dataset: Dataset) -> List[float]:
        """Generate embedding from dataset metadata and schema."""
        logger.debug("Generating dataset embedding")
        # Build schema description including field descriptions
        schema_desc = []
        for field in dataset.dataset_schema.fields:
            desc = f"{field.field_name}"
            if field.description:
                desc += f" ({field.description})"
            schema_desc.append(desc)

        text_to_embed = f"""
        Name: {dataset.name}
        Description: {dataset.description}
        Schema Fields:
        {chr(10).join(f'- {desc}' for desc in schema_desc)}
        """
        return await self.embeddings_model.aembed_query(text_to_embed)

    async def _generate_record_embedding(self, record: Record, dataset_schema: DatasetSchema) -> List[float]:
        """Generate embedding from record data using dataset schema for context."""
        logger.debug("Generating record embedding")
        # Build a clean representation of the record data
        content_parts = []

        for field in dataset_schema.fields:
            field_name = field.field_name
            if field_name in record.data:
                value = record.data[field_name]
                # Format based on field type if needed
                if field.description:
                    content_parts.append(f"{field_name} ({field.description}): {value}")
                else:
                    content_parts.append(f"{field_name}: {value}")

        # Create a clean text representation focused on the content
        text_to_embed = "\n".join(content_parts)

        return await self.embeddings_model.aembed_query(text_to_embed)

    @classmethod
    async def setup(cls, mongodb_client: AsyncIOMotorClient) -> "DatasetManager":
        """Factory method to create and setup a DatasetManager instance."""
        try:
            # Create manager instance
            manager = cls(mongodb_client)

            # Setup datasets collection indexes
            await manager._datasets.create_indexes(
                [
                    # Compound index for unique dataset names per user
                    pymongo.IndexModel([("user_id", 1), ("name", 1)], unique=True, background=True),
                    # Index for listing user's datasets
                    pymongo.IndexModel([("user_id", 1)], background=True),
                ]
            )

            # Setup records collection indexes
            await manager._records.create_indexes(
                [
                    # Index for querying records by dataset
                    pymongo.IndexModel([("user_id", 1), ("dataset_id", 1)], background=True),
                    # Index for record lookups
                    pymongo.IndexModel([("user_id", 1), ("dataset_id", 1), ("_id", 1)], background=True),
                ]
            )

            # Setup vector search indexes
            await manager._create_dataset_vector_search_index()
            await manager._create_record_vector_search_index()

            return manager

        except Exception as e:
            raise DatabaseError(f"Failed to setup indexes: {str(e)}")

    async def create_dataset(self, user_id: str, name: str, description: str, schema: DatasetSchema) -> UUID:
        """Creates a new dataset with the given schema and generates its embedding."""
        try:
            logger.info(f"Creating dataset '{name}' for user {user_id}")
            # Create dataset model
            dataset = Dataset(
                user_id=user_id,
                name=name,
                description=description,
                dataset_schema=schema,
            )

            # Generate embedding
            embedding = await self._generate_dataset_embedding(dataset)

            # Add embedding to dataset dict
            dataset_dict = dataset.model_dump(by_alias=True)
            dataset_dict[self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]] = embedding

            # Insert into database
            result = await self._datasets.insert_one(dataset_dict)
            logger.info(f"Dataset created with ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            if "duplicate key error" in str(e).lower():
                raise DatasetNameExistsError(f"Dataset with name '{name}' already exists for user {user_id}")
            raise DatabaseError(f"Failed to create dataset: {str(e)}")

    async def update_dataset(self, user_id: str, dataset_id: UUID, name: str, description: str) -> None:
        """Updates dataset metadata (name and description) and regenerates its embedding."""
        try:
            logger.info(f"Updating dataset {dataset_id} for user {user_id}")
            # Validate dataset exists and belongs to user
            dataset = await self.get_dataset(user_id, dataset_id)

            # Create updated dataset
            updated = Dataset(
                id=dataset_id,
                user_id=user_id,
                name=name,
                description=description,
                dataset_schema=dataset.dataset_schema,
                created_at=dataset.created_at,
                updated_at=datetime.now(timezone.utc),
            )

            # Generate new embedding
            logger.debug("Regenerating dataset embedding")
            embedding = await self._generate_dataset_embedding(updated)

            # Add embedding to dataset dict
            dataset_dict = updated.model_dump(by_alias=True)
            dataset_dict[self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]] = embedding

            # Update in database
            result = await self._datasets.replace_one(
                {"_id": str(dataset_id), "user_id": user_id},
                dataset_dict,
            )
            logger.info("Dataset updated successfully")

            if result.modified_count == 0:
                raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

        except DatasetNotFoundError:
            raise
        except Exception as e:
            if "duplicate key error" in str(e).lower():
                raise DatasetNameExistsError(f"Dataset with name '{name}' already exists for user {user_id}")
            raise DatabaseError(f"Failed to update dataset: {str(e)}")

    async def delete_dataset(self, user_id: str, dataset_id: UUID) -> None:
        """Deletes a dataset and all its records."""
        try:
            logger.info(f"Deleting dataset {dataset_id} and its records for user {user_id}")
            # Verify dataset exists and belongs to user
            await self.dataset_exists(user_id, dataset_id)

            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Delete dataset and its records
                    await self._records.delete_many(
                        {
                            "user_id": user_id,
                            "dataset_id": str(dataset_id),
                        },
                        session=session,
                    )

                    result = await self._datasets.delete_one(
                        {
                            "_id": str(dataset_id),
                            "user_id": user_id,
                        },
                        session=session,
                    )

                    if result.deleted_count == 0:
                        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
                    logger.info("Dataset and records deleted successfully")

        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to delete dataset: {str(e)}")

    async def list_datasets(self, user_id: str) -> List[Dataset]:
        """Lists all datasets belonging to the user."""
        try:
            logger.info(f"Listing datasets for user {user_id}")
            datasets = []
            cursor = self._datasets.find(
                {"user_id": user_id},
                {self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: 0}
            )
            async for doc in cursor:
                datasets.append(Dataset.model_validate(doc))
            return datasets
        except Exception as e:
            raise DatabaseError(f"Failed to list datasets: {str(e)}")

    async def get_dataset(self, user_id: str, dataset_id: UUID) -> Dataset:
        """Retrieves a specific dataset."""
        try:
            logger.debug(f"Getting dataset {dataset_id} for user {user_id}")
            doc = await self._datasets.find_one(
                {"_id": str(dataset_id), "user_id": user_id},
                {self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: 0}
            )
            if not doc:
                raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
            return Dataset.model_validate(doc)
        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get dataset: {str(e)}")

    async def dataset_exists(self, user_id: str, dataset_id: UUID) -> bool:
        """Efficiently checks if a dataset exists without retrieving the full document."""
        try:
            logger.debug(f"Checking if dataset {dataset_id} exists for user {user_id}")
            # Use count_documents with limit=1 for efficiency
            count = await self._datasets.count_documents({"_id": str(dataset_id), "user_id": user_id}, limit=1)
            if count == 0:
                raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
            return True
        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to check dataset existence: {str(e)}")

    async def _prepare_record_updates(
        self, user_id: str, dataset_id: UUID, field_name: str, old_field: SchemaField, field_update: SchemaField, session
    ) -> List[pymongo.UpdateOne]:
        """Prepares bulk update operations for records."""
        if old_field.type == field_update.type:
            return []

        # Get type implementation for new type
        type_impl = TypeRegistry.get_type(field_update.type)
        if field_update.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
            type_impl.set_options(field_update.options)

        # Get records with this field using session
        mongo_query = {"user_id": user_id, "dataset_id": str(dataset_id), f"data.{field_name}": {"$exists": True}}  # Only get records that have this field

        records = []
        cursor = self._records.find(mongo_query, session=session)
        async for doc in cursor:
            records.append(Record.model_validate(doc))

        updates = []
        for record in records:
            try:
                # Convert and validate value
                converted_value = type_impl.validate(record.data[field_name])

                # Create update operation
                updates.append(
                    pymongo.UpdateOne(
                        {
                            "_id": record.id,
                            "user_id": user_id,
                            "dataset_id": str(dataset_id),
                        },
                        {
                            "$set": {
                                f"data.{field_name}": converted_value,
                                "updated_at": datetime.now(timezone.utc),
                            }
                        },
                    )
                )
            except ValueError as e:
                raise InvalidRecordDataError(f"Failed to convert field '{field_name}' in record {record.id}: {str(e)}")

        return updates

    async def delete_field(self, user_id: str, dataset_id: UUID, field_name: str) -> None:
        """Deletes a field from the dataset schema and removes it from all records."""
        try:
            # Validate dataset exists and belongs to user
            dataset = await self.get_dataset(user_id, dataset_id)

            # Create new schema without the field
            new_schema = DatasetSchema(fields=[field for field in dataset.dataset_schema if field.field_name != field_name])

            # Validate schema - will raise InvalidDatasetSchemaError if field doesn't exist
            if len(new_schema) == len(dataset.dataset_schema):
                raise InvalidDatasetSchemaError(f"Field '{field_name}' not found in schema")

            # Start transaction
            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Update dataset schema and regenerate embedding
                    updated = Dataset(
                        id=dataset_id,
                        user_id=user_id,
                        name=dataset.name,
                        description=dataset.description,
                        dataset_schema=new_schema,
                        created_at=dataset.created_at,
                        updated_at=datetime.now(timezone.utc),
                    )

                    # Generate new embedding
                    embedding = await self._generate_dataset_embedding(updated)

                    # Add embedding to dataset dict
                    dataset_dict = updated.model_dump(by_alias=True)
                    dataset_dict[self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]] = embedding

                    result = await self._datasets.replace_one(
                        {"_id": str(dataset_id), "user_id": user_id},
                        dataset_dict,
                        session=session,
                    )

                    if result.modified_count == 0:
                        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

                    # Remove field from all records
                    await self._records.update_many(
                        {"user_id": user_id, "dataset_id": str(dataset_id)},
                        {"$unset": {f"data.{field_name}": ""}},
                        session=session,
                    )

        except (DatasetNotFoundError, InvalidDatasetSchemaError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to delete field: {str(e)}")

    async def add_field(
        self,
        user_id: str,
        dataset_id: UUID,
        field: SchemaField,
    ) -> None:
        """Adds a new field to the dataset schema and initializes it in existing records."""
        try:
            # Validate dataset exists and belongs to user
            dataset = await self.get_dataset(user_id, dataset_id)

            # Create new schema with the added field
            new_schema = DatasetSchema(fields=[*dataset.dataset_schema.fields, field])

            # Start transaction
            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Update dataset schema and regenerate embedding
                    updated = Dataset(
                        id=dataset_id,
                        user_id=user_id,
                        name=dataset.name,
                        description=dataset.description,
                        dataset_schema=new_schema,
                        created_at=dataset.created_at,
                        updated_at=datetime.now(timezone.utc),
                    )

                    # Generate new embedding
                    embedding = await self._generate_dataset_embedding(updated)

                    # Add embedding to dataset dict
                    dataset_dict = updated.model_dump(by_alias=True)
                    dataset_dict[self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]] = embedding

                    result = await self._datasets.replace_one(
                        {"_id": str(dataset_id), "user_id": user_id},
                        dataset_dict,
                        session=session,
                    )

                    if result.modified_count == 0:
                        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

                    # Initialize field in existing records if default value provided
                    if field.default is not None:
                        await self._records.update_many(
                            {"user_id": user_id, "dataset_id": str(dataset_id)},
                            {"$set": {f"data.{field.field_name}": field.default}},
                            session=session,
                        )

        except (DatasetNotFoundError, InvalidDatasetSchemaError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to add field: {str(e)}")

    async def _validate_required_field_update(
        self, user_id: str, dataset_id: UUID, field_name: str, old_field: SchemaField, field_update: SchemaField, session
    ) -> None:
        """Validate that changing a field to required doesn't violate existing records."""
        # Only check if changing from not required to required
        if not old_field.required and field_update.required:
            # If there's a default value, we can use that for null values
            if field_update.default is not None:
                return

            # Check if any records have null values for this field
            query = {"user_id": user_id, "dataset_id": str(dataset_id), f"data.{field_name}": {"$exists": False}}  # Field doesn't exist or is null

            # Count records with null values
            count = await self._records.count_documents(query, session=session)
            if count > 0:
                raise InvalidRecordDataError(f"Cannot change field '{field_name}' to required: {count} records have null values for this field")

    async def _validate_unique_field_update(
        self, user_id: str, dataset_id: UUID, field_name: str, old_field: SchemaField, field_update: SchemaField, session
    ) -> None:
        """Validate that changing a field to unique doesn't violate existing records."""
        # Only check if changing from not unique to unique
        if not old_field.unique and field_update.unique:
            # Check for duplicate values in existing records
            pipeline = [
                # Match records in this dataset
                {"$match": {"user_id": user_id, "dataset_id": str(dataset_id)}},
                # Only include records that have this field
                {"$match": {f"data.{field_name}": {"$exists": True}}},
                # Group by field value and count occurrences
                {"$group": {"_id": f"$data.{field_name}", "count": {"$sum": 1}}},
                # Only include groups with more than one record (duplicates)
                {"$match": {"count": {"$gt": 1}}},
                # Limit to just one result for efficiency
                {"$limit": 1},
            ]

            # Execute pipeline
            cursor = self._records.aggregate(pipeline, session=session)
            duplicates = await cursor.to_list(length=1)

            if duplicates:
                duplicate_value = duplicates[0]["_id"]
                duplicate_count = duplicates[0]["count"]
                raise InvalidRecordDataError(f"Cannot change field '{field_name}' to unique: {duplicate_count} records have the value '{duplicate_value}'")

    async def update_field(
        self,
        user_id: str,
        dataset_id: UUID,
        field_name: str,
        field_update: SchemaField,
    ) -> None:
        """Updates a single field in the dataset schema and converts existing records."""
        try:
            # Validate dataset exists and belongs to user
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate field update
            old_field, new_schema = dataset.dataset_schema.validate_field_update(field_name, field_update)
            if not old_field:
                # No changes needed
                return

            # Start transaction
            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Validate required and unique constraints
                    await self._validate_required_field_update(user_id, dataset_id, field_name, old_field, field_update, session)
                    await self._validate_unique_field_update(user_id, dataset_id, field_name, old_field, field_update, session)

                    # Update dataset schema and regenerate embedding
                    updated = Dataset(
                        id=dataset_id,
                        user_id=user_id,
                        name=dataset.name,
                        description=dataset.description,
                        dataset_schema=new_schema,
                        created_at=dataset.created_at,
                        updated_at=datetime.now(timezone.utc),
                    )

                    # Generate new embedding
                    embedding = await self._generate_dataset_embedding(updated)

                    # Add embedding to dataset dict
                    dataset_dict = updated.model_dump(by_alias=True)
                    dataset_dict[self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]] = embedding

                    result = await self._datasets.replace_one(
                        {"_id": str(dataset_id), "user_id": user_id},
                        dataset_dict,
                        session=session,
                    )

                    if result.modified_count == 0:
                        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

                    # Prepare record updates if needed
                    updates = await self._prepare_record_updates(user_id, dataset_id, field_name, old_field, field_update, session)

                    # Execute bulk updates if any
                    if updates:
                        try:
                            result = await self._records.bulk_write(updates, session=session)
                            if result.modified_count != len(updates):
                                raise DatabaseError(f"Failed to update all records: {result.modified_count}/{len(updates)} updated")
                        except BulkWriteError as e:
                            raise DatabaseError(f"Failed to update records: {str(e)}")

        except (DatasetNotFoundError, InvalidDatasetSchemaError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to update field: {str(e)}")

    async def _check_value_exists(self, user_id: str, dataset_id: UUID, field_name: str, value: Any, exclude_record_id: Optional[UUID] = None) -> bool:
        """Check if a value already exists for a unique field in any record.
        Returns True if the value exists, False otherwise."""
        try:
            query = {"user_id": user_id, "dataset_id": str(dataset_id), f"data.{field_name}": value}

            # Exclude the current record if updating
            if exclude_record_id:
                query["_id"] = {"$ne": str(exclude_record_id)}

            # Use count_documents with limit=1 for efficiency
            count = await self._records.count_documents(query, limit=1)
            return count > 0

        except Exception as e:
            raise DatabaseError(f"Failed to check value existence: {str(e)}")

    async def _validate_uniqueness(
        self, user_id: str, dataset_id: UUID, data: RecordData, dataset_schema: DatasetSchema, exclude_record_id: Optional[UUID] = None
    ) -> None:
        """Validate that data doesn't violate uniqueness constraints."""
        # Get unique fields from schema
        unique_fields = [field for field in dataset_schema.fields if field.unique]

        # Check each unique field
        for field in unique_fields:
            field_name = field.field_name
            if field_name in data:
                # Check if value already exists in another record
                exists = await self._check_value_exists(user_id, dataset_id, field_name, data[field_name], exclude_record_id)
                if exists:
                    raise InvalidRecordDataError(f"Value '{data[field_name]}' for field '{field_name}' already exists in another record")

    async def create_record(self, user_id: str, dataset_id: UUID, data: RecordData) -> UUID:
        """Creates a new record in the specified dataset."""
        try:
            logger.info(f"Creating record in dataset {dataset_id} for user {user_id}")
            # Get dataset to validate against schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert data
            validated_data = Record.validate_data(data, dataset.dataset_schema)

            # Check uniqueness constraints
            await self._validate_uniqueness(user_id, dataset_id, validated_data, dataset.dataset_schema)

            # Create record
            record = Record(
                user_id=user_id,
                dataset_id=str(dataset_id),
                data=validated_data,
            )

            # Generate embedding
            embedding = await self._generate_record_embedding(record, dataset.dataset_schema)

            # Add embedding to record dict
            record_dict = record.model_dump(by_alias=True)
            record_dict[self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]] = embedding

            # Insert into database
            result = await self._records.insert_one(record_dict)
            logger.info(f"Record created with ID: {result.inserted_id}")
            return result.inserted_id

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to create record: {str(e)}")

    async def update_record(self, user_id: str, dataset_id: UUID, record_id: UUID, data: RecordData) -> None:
        """Updates an existing record."""
        try:
            logger.info(f"Updating record {record_id} in dataset {dataset_id}")
            # Get dataset to validate against schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert data
            validated_data = Record.validate_data(data, dataset.dataset_schema)

            # Check uniqueness constraints
            await self._validate_uniqueness(user_id, dataset_id, validated_data, dataset.dataset_schema, record_id)

            # Create record object for embedding generation
            record = Record(
                id=record_id,
                user_id=user_id,
                dataset_id=str(dataset_id),
                data=validated_data,
            )

            # Generate new embedding
            embedding = await self._generate_record_embedding(record, dataset.dataset_schema)

            # Update record with embedding
            result = await self._records.update_one(
                {
                    "_id": str(record_id),
                    "user_id": user_id,
                    "dataset_id": str(dataset_id),
                },
                {
                    "$set": {
                        "data": validated_data,
                        "updated_at": datetime.now(timezone.utc),
                        self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: embedding
                    }
                },
            )

            if result.modified_count == 0:
                # Check if record exists
                record = await self._records.find_one(
                    {
                        "_id": str(record_id),
                        "user_id": user_id,
                        "dataset_id": str(dataset_id),
                    },
                    {self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: 0}
                )
                if not record:
                    raise RecordNotFoundError(f"Record {record_id} not found")
                logger.debug("Record exists but no changes were made")
            else:
                logger.info("Record updated successfully")

        except (DatasetNotFoundError, RecordNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to update record: {str(e)}")

    async def delete_record(self, user_id: str, dataset_id: UUID, record_id: UUID) -> None:
        """Deletes a record."""
        try:
            logger.info(f"Deleting record {record_id} from dataset {dataset_id}")
            # Verify dataset exists
            await self.dataset_exists(user_id, dataset_id)

            # Delete record
            result = await self._records.delete_one(
                {
                    "_id": str(record_id),
                    "user_id": user_id,
                    "dataset_id": str(dataset_id),
                }
            )

            if result.deleted_count == 0:
                raise RecordNotFoundError(f"Record {record_id} not found")
            logger.info("Record deleted successfully")

        except (DatasetNotFoundError, RecordNotFoundError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to delete record: {str(e)}")

    async def get_record(self, user_id: str, dataset_id: UUID, record_id: UUID) -> Record:
        """Retrieves a specific record."""
        try:
            logger.debug(f"Getting record {record_id} from dataset {dataset_id}")
            # Verify dataset exists
            await self.dataset_exists(user_id, dataset_id)

            # Get record
            doc = await self._records.find_one(
                {
                    "_id": str(record_id),
                    "user_id": user_id,
                    "dataset_id": str(dataset_id),
                },
                {self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: 0}
            )

            if not doc:
                raise RecordNotFoundError(f"Record {record_id} not found")

            return Record.model_validate(doc)

        except (DatasetNotFoundError, RecordNotFoundError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get record: {str(e)}")

    async def _search_similar_entities_generic(
        self,
        collection: AsyncIOMotorCollection,
        index_name: str,
        entity_type: str,
        user_id: str,
        query_embedding: List[float],
        limit: int = 10,
        min_score: Optional[float] = None,
        filter_dict: Optional[Dict] = None,
        additional_filters: Optional[Dict] = None,
        model_class: Any = None,
    ) -> List[Any]:
        """Generic method to find similar entities using vector search."""
        try:
            logger.info(f"Searching similar {entity_type}s for user {user_id}")

            # Build search pipeline
            pipeline = [
                {
                    "$vectorSearch": {
                        "index": index_name,
                        "path": self.VECTOR_SEARCH_CONFIG["FIELD_NAME"],
                        "queryVector": query_embedding,
                        "numCandidates": limit * self.VECTOR_SEARCH_CONFIG["NUM_CANDIDATES_MULTIPLIER"],
                        "limit": limit,
                        "exact": False,
                    }
                },
                {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
            ]

            # Add score filter if provided
            if min_score is not None:
                pipeline.append({"$match": {"score": {"$gte": min_score}}})
            elif self.VECTOR_SEARCH_CONFIG["MIN_SCORE"] > 0:
                pipeline.append({"$match": {"score": {"$gte": self.VECTOR_SEARCH_CONFIG["MIN_SCORE"]}}})

            # Add any additional specific filters
            if additional_filters:
                pipeline.append({"$match": additional_filters})

            # Add any general filters
            if filter_dict:
                pipeline.append({"$match": filter_dict})

            # Add user_id filter in a separate stage to ensure it cannot be overridden
            pipeline.append({"$match": {"user_id": user_id}})

            # Remove score from final results
            pipeline.append({"$project": {"score": 0}})

            # Execute search
            results = []
            async for doc in collection.aggregate(pipeline):
                if model_class:
                    entity = model_class.model_validate(doc)
                    results.append(entity)
                else:
                    results.append(doc)

            logger.info(f"Found {len(results)} similar {entity_type}s")
            return results

        except Exception as e:
            raise DatabaseError(f"Failed to perform {entity_type} vector search: {str(e)}")

    async def search_similar_datasets(
        self,
        user_id: str,
        dataset: Dataset,
        limit: int = 10,
        min_score: Optional[float] = None,
        filter_dict: Optional[Dict] = None,
    ) -> List[Dataset]:
        """Find similar datasets using vector search."""
        try:
            # Generate embedding from dataset
            query_embedding = await self._generate_dataset_embedding(dataset)

            # Use generic search method
            return await self._search_similar_entities_generic(
                collection=self._datasets,
                index_name=self.VECTOR_SEARCH_CONFIG["INDEX_NAME"],
                entity_type="dataset",
                user_id=user_id,
                query_embedding=query_embedding,
                limit=limit,
                min_score=min_score,
                filter_dict=filter_dict,
                model_class=Dataset,
            )

        except Exception as e:
            raise DatabaseError(f"Failed to perform dataset vector search: {str(e)}")

    async def search_similar_records(
        self,
        user_id: str,
        dataset_id: UUID,
        record: Record,
        limit: int = 10,
        min_score: Optional[float] = None,
        filter_dict: Optional[Dict] = None,
    ) -> List[Record]:
        """Find similar records using vector search."""
        try:
            # Get dataset to access schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Generate embedding from record
            query_embedding = await self._generate_record_embedding(record, dataset.dataset_schema)

            # Additional filter to ensure we only search within the specified dataset
            dataset_filter = {"dataset_id": str(dataset_id)}

            # Use generic search method
            return await self._search_similar_entities_generic(
                collection=self._records,
                index_name="vector_search_records",
                entity_type="record",
                user_id=user_id,
                query_embedding=query_embedding,
                limit=limit,
                min_score=min_score,
                filter_dict=filter_dict,
                additional_filters=dataset_filter,
                model_class=Record,
            )

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to perform record vector search: {str(e)}")

    async def get_all_records(self, user_id: str, dataset_id: UUID) -> List[Record]:
        """Retrieves all records in the specified dataset."""
        try:
            logger.info(f"Getting all records from dataset {dataset_id} for user {user_id}")
            # Verify dataset exists
            await self.dataset_exists(user_id, dataset_id)

            # Get all records
            records = []
            cursor = self._records.find(
                {"user_id": user_id, "dataset_id": str(dataset_id)},
                {self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: 0}
            )
            async for doc in cursor:
                records.append(Record.model_validate(doc))

            logger.info(f"Retrieved {len(records)} records")
            return records

        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get all records: {str(e)}")

    async def _validate_batch_uniqueness(self, user_id: str, dataset_id: UUID, records_data: List[RecordData], dataset_schema: DatasetSchema) -> None:
        """Validate uniqueness constraints for a batch of records."""
        # Get unique fields from schema
        unique_fields = [field for field in dataset_schema.fields if field.unique]
        if not unique_fields:
            return  # No unique fields to check

        # For each unique field
        for field in unique_fields:
            field_name = field.field_name

            # Collect all values for this field from the batch
            batch_values = {}
            for i, data in enumerate(records_data):
                if field_name in data:
                    value = data[field_name]
                    if value in batch_values:
                        # Duplicate within the batch itself
                        raise InvalidRecordDataError(f"Duplicate value '{value}' for unique field '{field_name}' within the batch")
                    batch_values[value] = i

            if not batch_values:
                continue

            # Check if any values already exist in the database
            query = {"user_id": user_id, "dataset_id": str(dataset_id), f"data.{field_name}": {"$in": list(batch_values.keys())}}

            # Find existing records with these values
            existing_values = set()
            async for doc in self._records.find(query, {f"data.{field_name}": 1}):
                if field_name in doc.get("data", {}):
                    existing_values.add(doc["data"][field_name])

            # Check for conflicts
            for value in existing_values:
                if value in batch_values:
                    raise InvalidRecordDataError(f"Value '{value}' for field '{field_name}' already exists in another record")

    async def batch_create_records(self, user_id: str, dataset_id: UUID, records_data: List[RecordData]) -> List[UUID]:
        """Creates multiple records in the specified dataset."""
        try:
            logger.info(f"Batch creating {len(records_data)} records in dataset {dataset_id} for user {user_id}")
            # Get dataset to validate against schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert all data first
            validated_records_data = []
            validated_records = []
            for data in records_data:
                validated_data = Record.validate_data(data, dataset.dataset_schema)
                validated_records_data.append(validated_data)
                record = Record(
                    user_id=user_id,
                    dataset_id=str(dataset_id),
                    data=validated_data,
                )
                
                # Generate embedding for each record
                embedding = await self._generate_record_embedding(record, dataset.dataset_schema)
                
                # Add embedding to record dict
                record_dict = record.model_dump(by_alias=True)
                record_dict[self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]] = embedding
                validated_records.append(record_dict)

            # Check uniqueness constraints for the batch
            await self._validate_batch_uniqueness(user_id, dataset_id, validated_records_data, dataset.dataset_schema)

            # Insert all records
            result = await self._records.insert_many(validated_records)
            logger.info(f"Batch created {len(result.inserted_ids)} records")
            return result.inserted_ids

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to batch create records: {str(e)}")

    async def _validate_batch_updates_uniqueness(
        self, user_id: str, dataset_id: UUID, records_updates: List["RecordUpdate"], dataset_schema: DatasetSchema
    ) -> None:
        """Validate uniqueness constraints for a batch of record updates."""
        # Get unique fields from schema
        unique_fields = [field for field in dataset_schema.fields if field.unique]
        if not unique_fields:
            return  # No unique fields to check

        # For each unique field
        for field in unique_fields:
            field_name = field.field_name

            # Collect all values and record IDs for this field from the batch
            batch_values = {}  # Maps values to record IDs
            for update in records_updates:
                record_id = update.get("record_id")
                data = update.get("data")

                if field_name in data:
                    value = data[field_name]
                    # Convert record_id to string for consistent comparison
                    str_record_id = str(record_id)

                    if value in batch_values:
                        # Another record in the batch is being updated to the same value
                        # Convert the stored record_id to string for comparison
                        stored_record_id = str(batch_values[value])

                        if stored_record_id != str_record_id:
                            # Duplicate within the batch itself (different records)
                            raise InvalidRecordDataError(
                                f"Duplicate value '{value}' for unique field '{field_name}' within the batch. "
                                f"Records {stored_record_id} and {str_record_id} would have the same value."
                            )

                    batch_values[value] = record_id

            if not batch_values:
                continue

            # Check if any values already exist in the database (excluding the records being updated)
            str_record_ids = [str(record_id) for record_id in batch_values.values()]
            query = {
                "user_id": user_id,
                "dataset_id": str(dataset_id),
                f"data.{field_name}": {"$in": list(batch_values.keys())},
                "_id": {"$nin": str_record_ids},  # Exclude records being updated
            }

            # Find existing records with these values
            existing_values = {}  # Maps values to record IDs
            async for doc in self._records.find(query, {f"data.{field_name}": 1, "_id": 1}):
                if field_name in doc.get("data", {}):
                    existing_values[doc["data"][field_name]] = doc["_id"]

            # Check for conflicts
            for value, existing_id in existing_values.items():
                if value in batch_values:
                    raise InvalidRecordDataError(f"Value '{value}' for field '{field_name}' already exists in record {existing_id}")

    async def batch_update_records(self, user_id: str, dataset_id: UUID, records_updates: List["RecordUpdate"]) -> List[UUID]:
        """Updates multiple existing records."""
        try:
            logger.info(f"Batch updating {len(records_updates)} records in dataset {dataset_id}")
            # Get dataset to validate against schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert all data first
            validated_updates = []
            record_ids = []

            for update in records_updates:
                record_id = update.get("record_id")
                data = update.get("data")

                if not record_id or not data:
                    raise InvalidRecordDataError("Record update missing record_id or data")

                # Validate and convert data
                validated_data = Record.validate_data(data, dataset.dataset_schema)
                validated_updates.append({"record_id": record_id, "data": validated_data})
                record_ids.append(record_id)

            # Check uniqueness constraints for the batch
            await self._validate_batch_updates_uniqueness(user_id, dataset_id, validated_updates, dataset.dataset_schema)

            # Prepare bulk operations
            operations = []
            for update in validated_updates:
                record_id = update["record_id"]
                validated_data = update["data"]

                # Create record object for embedding generation
                record = Record(
                    id=record_id,
                    user_id=user_id,
                    dataset_id=str(dataset_id),
                    data=validated_data,
                )
                
                # Generate new embedding
                embedding = await self._generate_record_embedding(record, dataset.dataset_schema)

                # Add to operations with embedding
                operations.append(
                    pymongo.UpdateOne(
                        {
                            "_id": str(record_id),
                            "user_id": user_id,
                            "dataset_id": str(dataset_id),
                        },
                        {
                            "$set": {
                                "data": validated_data,
                                "updated_at": datetime.now(timezone.utc),
                                self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: embedding
                            }
                        },
                    )
                )

            # Execute bulk update
            if operations:
                result = await self._records.bulk_write(operations)
                logger.info(f"Batch updated {result.modified_count}/{len(operations)} records")

                # Check if all records were updated
                if result.modified_count != len(operations):
                    # Find all missing records in a single query
                    str_record_ids = [str(record_id) for record_id in record_ids]
                    existing_records = await self._records.find(
                        {
                            "_id": {"$in": str_record_ids},
                            "user_id": user_id,
                            "dataset_id": str(dataset_id),
                        },
                        {self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: 0}
                    ).to_list(None)

                    existing_ids = {str(record["_id"]) for record in existing_records}
                    missing_ids = [record_id for record_id in record_ids if str(record_id) not in existing_ids]

                    if missing_ids:
                        if len(missing_ids) == 1:
                            raise RecordNotFoundError(f"Record {missing_ids[0]} not found")
                        else:
                            raise RecordNotFoundError(f"Multiple records not found: {', '.join(str(id) for id in missing_ids)}")
                    else:
                        # Records exist but weren't modified (likely because data is identical)
                        logger.debug("Some records were not modified, but all exist")

            return record_ids

        except (DatasetNotFoundError, RecordNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to batch update records: {str(e)}")

    async def batch_delete_records(self, user_id: str, dataset_id: UUID, record_ids: List[UUID]) -> List[UUID]:
        """Deletes multiple records."""
        try:
            logger.info(f"Batch deleting {len(record_ids)} records from dataset {dataset_id}")
            # Verify dataset exists
            await self.dataset_exists(user_id, dataset_id)

            # Convert record IDs to strings
            str_record_ids = [str(record_id) for record_id in record_ids]

            # Delete records
            result = await self._records.delete_many(
                {
                    "_id": {"$in": str_record_ids},
                    "user_id": user_id,
                    "dataset_id": str(dataset_id),
                }
            )

            logger.info(f"Batch deleted {result.deleted_count}/{len(record_ids)} records")
            return record_ids

        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to batch delete records: {str(e)}")

    async def query_records(
        self, user_id: str, dataset_id: UUID, query: Optional[RecordQuery] = None, ids_only: bool = False
    ) -> Union[List[Record], List[Dict], List[str]]:
        """Query records in the specified dataset."""
        try:
            logger.info(f"Querying records in dataset {dataset_id} for user {user_id}")
            # Verify dataset exists and get schema for validation
            dataset = await self.get_dataset(user_id, dataset_id)

            # Create default query if none provided
            if query is None:
                query = RecordQuery()

            # Validate query against schema
            query.validate_with_schema(dataset.dataset_schema)

            # Build pipeline
            pipeline = build_aggregation_pipeline(user_id, str(dataset_id), query)
            
            # Add projection to exclude embedding field
            pipeline.append({"$project": {self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: 0}})

            logger.debug("Executing aggregation pipeline")
            # Execute pipeline
            cursor = self._records.aggregate(pipeline)

            # Handle results based on query type
            if query.aggregations:
                # Aggregation query - return Dict results
                results = []
                async for doc in cursor:
                    # If group by was used, move _id contents to top level
                    if doc["_id"] and isinstance(doc["_id"], dict):
                        doc.update(doc["_id"])
                    doc.pop("_id")
                    results.append(doc)
                logger.info(f"Query returned {len(results)} aggregated results")
                return results
            else:
                # Simple query - return Record objects or just IDs
                if ids_only:
                    # Return only record IDs
                    record_ids = []
                    async for doc in cursor:
                        record_ids.append(doc["_id"])
                    logger.info(f"Query returned {len(record_ids)} record IDs")
                    return record_ids
                else:
                    # Return full Record objects
                    records = []
                    async for doc in cursor:
                        records.append(Record.model_validate(doc))
                    logger.info(f"Query returned {len(records)} records")
                    return records

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to query records: {str(e)}")
