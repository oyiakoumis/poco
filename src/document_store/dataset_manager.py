"""Dataset manager for MongoDB operations."""

from asyncio import sleep
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Union
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

from document_store.exceptions import (
    DatabaseError,
    DatasetNameExistsError,
    DatasetNotFoundError,
    InvalidDatasetSchemaError,
    InvalidRecordDataError,
    RecordNotFoundError,
)
from document_store.models import Dataset, Record
from document_store.models.field import SchemaField
from document_store.models.query import RecordQuery
from document_store.models.record import RecordData
from document_store.models.schema import DatasetSchema
from document_store.models.types import FieldType, TypeRegistry
from document_store.pipeline import build_aggregation_pipeline
from utils.logging import logger


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
        "INDEX_NAME": "vector_search_datasets",
        "FIELD_NAME": "embedding",
        "DIMENSION": 1536,  # text-embedding-3-small dimension
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
        self.embeddings_model = OpenAIEmbeddings(model="text-embedding-3-small")

    async def _create_vector_search_index(self) -> None:
        """Create vector search index if it doesn't exist and ensure it's ready."""
        try:
            logger.info("Checking vector search index status")
            # Check current index status
            status = await self._get_index_status()

            if status == IndexStatus.READY:
                logger.info("Vector search index is ready")
                return  # Index exists and is ready
            elif status == IndexStatus.FAILED:
                # Drop failed index to recreate it
                logger.warning("Vector search index failed, dropping to recreate")
                await self._delete_vector_search_index()
            elif status == IndexStatus.STALE:
                logger.warning("Vector search index is stale, dropping to recreate")
                await self._delete_vector_search_index()
            elif status in (IndexStatus.BUILDING, IndexStatus.PENDING):
                # Wait for existing index to be ready
                logger.info("Waiting for existing vector search index to be ready")
                await self._wait_for_index_ready()
                return
            elif status == IndexStatus.DELETING:
                logger.info("Waiting for vector search index deletion to complete")
                await sleep(5)  # Wait a bit before creating new index

            # Create new index if it doesn't exist or was dropped
            logger.info("Creating new vector search index")
            index_definition = {
                "mappings": {
                    "dynamic": True,
                    "fields": {
                        self.VECTOR_SEARCH_CONFIG["FIELD_NAME"]: {
                            "type": "knnVector",
                            "dimensions": self.VECTOR_SEARCH_CONFIG["DIMENSION"],
                            "similarity": "cosine",
                        }
                    },
                }
            }

            # Create index
            search_index = SearchIndexModel(definition=index_definition, name=self.VECTOR_SEARCH_CONFIG["INDEX_NAME"])
            await self._datasets.create_search_index(search_index)

            # Wait for index to be ready
            logger.info("Waiting for new vector search index to be ready")
            await self._wait_for_index_ready()
            logger.info("Vector search index is ready")

        except Exception as e:
            raise DatabaseError(f"Failed to create vector search index: {str(e)}")

    async def _delete_vector_search_index(self) -> None:
        """Delete vector search index and wait until it's confirmed to be deleted."""
        try:
            # Start deletion
            await self._datasets.drop_search_index(self.VECTOR_SEARCH_CONFIG["INDEX_NAME"])

            # Wait for deletion to complete
            MAX_POLL_ATTEMPTS = 30  # 1 minute total
            POLL_INTERVAL_SECONDS = 2

            attempts = 0
            while attempts < MAX_POLL_ATTEMPTS:
                status = await self._get_index_status()
                if status == IndexStatus.DOES_NOT_EXIST:
                    return
                elif status == IndexStatus.DELETING:
                    await sleep(POLL_INTERVAL_SECONDS)
                else:
                    raise DatabaseError(f"Unexpected index status during deletion: {status}")
                attempts += 1

            raise DatabaseError(f"Index deletion not complete after {MAX_POLL_ATTEMPTS} attempts")

        except Exception as e:
            raise DatabaseError(f"Failed to delete vector search index: {str(e)}")

    async def _get_index_status(self) -> IndexStatus:
        """Get current status of the vector search index."""
        try:
            indexes = await self._datasets.list_search_indexes().to_list(None)
            for index in indexes:
                if index["name"] == self.VECTOR_SEARCH_CONFIG["INDEX_NAME"]:
                    status = index.get("status", "")
                    try:
                        return IndexStatus(status)
                    except ValueError:
                        print(f"Warning: Unknown index status: {status}")
                        return IndexStatus.FAILED
            return IndexStatus.DOES_NOT_EXIST
        except Exception as e:
            raise DatabaseError(f"Failed to get index status: {str(e)}")

    async def _wait_for_index_ready(self) -> None:
        """Poll index status until ready or max attempts reached."""
        MAX_POLL_ATTEMPTS = 30  # 1 minute total
        POLL_INTERVAL_SECONDS = 2

        attempts = 0
        while attempts < MAX_POLL_ATTEMPTS:
            status = await self._get_index_status()

            if status == IndexStatus.READY:
                return
            elif status == IndexStatus.FAILED:
                raise DatabaseError("Vector search index creation failed")
            elif status == IndexStatus.STALE:
                print("Warning: Index is stale, may return out-of-date results")
                return
            elif status == IndexStatus.DELETING:
                await sleep(POLL_INTERVAL_SECONDS * 2)  # Wait longer for deletion
            elif status == IndexStatus.BUILDING or status == IndexStatus.PENDING:
                await sleep(POLL_INTERVAL_SECONDS)
            else:
                raise DatabaseError(f"Unexpected index status: {status}")

            attempts += 1

        raise DatabaseError(f"Index not ready after {MAX_POLL_ATTEMPTS} attempts")

    async def _generate_dataset_embedding(self, dataset: Dataset) -> List[float]:
        """Generate embedding from dataset metadata and schema."""
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

            # Setup vector search index
            await manager._create_vector_search_index()

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
            logger.debug("Generating dataset embedding")
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
            await self.get_dataset(user_id, dataset_id)

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
            cursor = self._datasets.find({"user_id": user_id})
            async for doc in cursor:
                datasets.append(Dataset.model_validate(doc))
            return datasets
        except Exception as e:
            raise DatabaseError(f"Failed to list datasets: {str(e)}")

    async def get_dataset(self, user_id: str, dataset_id: UUID) -> Dataset:
        """Retrieves a specific dataset."""
        try:
            logger.debug(f"Getting dataset {dataset_id} for user {user_id}")
            doc = await self._datasets.find_one({"_id": str(dataset_id), "user_id": user_id})
            if not doc:
                raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
            return Dataset.model_validate(doc)
        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get dataset: {str(e)}")

    async def _prepare_record_updates(
        self, user_id: str, dataset_id: UUID, field_name: str, old_field: SchemaField, field_update: SchemaField, session
    ) -> List[pymongo.UpdateOne]:
        """Prepares bulk update operations for records.

        Args:
            user_id: User ID
            dataset_id: Dataset ID
            field_name: Field being updated
            old_field: Original field definition
            field_update: New field definition
            session: MongoDB session for transaction

        Returns:
            List of UpdateOne operations

        Raises:
            InvalidRecordDataError: If conversion fails
        """
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
        """Deletes a field from the dataset schema and removes it from all records.

        All changes are performed in a transaction to ensure consistency.

        Args:
            user_id: ID of the user who owns the dataset
            dataset_id: ID of the dataset to update
            field_name: Name of the field to delete

        Raises:
            DatasetNotFoundError: If dataset not found
            InvalidDatasetSchemaError: If field does not exist
            DatabaseError: For other database errors
        """
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
        """Adds a new field to the dataset schema and initializes it in existing records.

        All changes are performed in a transaction to ensure consistency. If the field
        has a default value defined, it will be used to initialize the field in existing records.

        Args:
            user_id: ID of the user who owns the dataset
            dataset_id: ID of the dataset to update
            field: New field definition with optional default value

        Raises:
            DatasetNotFoundError: If dataset not found
            InvalidDatasetSchemaError: If field already exists or is invalid
            DatabaseError: For other database errors
        """
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

    async def update_field(
        self,
        user_id: str,
        dataset_id: UUID,
        field_name: str,
        field_update: SchemaField,
    ) -> None:
        """Updates a single field in the dataset schema and converts existing records.

        All changes are performed in a transaction to ensure consistency. If the field
        type changes, existing record values will be converted if the conversion is safe.
        Only updates records if the field type has changed.

        Args:
            user_id: ID of the user who owns the dataset
            dataset_id: ID of the dataset to update
            field_name: Name of the field to update
            field_update: New field definition

        Raises:
            DatasetNotFoundError: If dataset not found
            InvalidDatasetSchemaError: If field update is invalid
            InvalidRecordDataError: If records cannot be converted to new field type
            DatabaseError: For other database errors
        """
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

    async def create_record(self, user_id: str, dataset_id: UUID, data: RecordData) -> UUID:
        """Creates a new record in the specified dataset."""
        try:
            logger.info(f"Creating record in dataset {dataset_id} for user {user_id}")
            # Get dataset to validate against schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert data
            validated_data = Record.validate_data(data, dataset.dataset_schema)

            # Create record
            record = Record(
                user_id=user_id,
                dataset_id=str(dataset_id),
                data=validated_data,
            )

            result = await self._records.insert_one(record.model_dump(by_alias=True))
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

            # Update record
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
                    }
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
            await self.get_dataset(user_id, dataset_id)

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
            await self.get_dataset(user_id, dataset_id)

            # Get record
            doc = await self._records.find_one(
                {
                    "_id": str(record_id),
                    "user_id": user_id,
                    "dataset_id": str(dataset_id),
                }
            )

            if not doc:
                raise RecordNotFoundError(f"Record {record_id} not found")

            return Record.model_validate(doc)

        except (DatasetNotFoundError, RecordNotFoundError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get record: {str(e)}")

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
            logger.info(f"Searching similar datasets for user {user_id}")
            logger.debug("Generating embedding for similarity search")
            # Generate embedding from dataset
            query_embedding = await self._generate_dataset_embedding(dataset)

            # Build search pipeline
            pipeline = [
                {
                    "$vectorSearch": {
                        "index": self.VECTOR_SEARCH_CONFIG["INDEX_NAME"],
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

            # Add any additional filters first
            if filter_dict:
                pipeline.append({"$match": filter_dict})

            # Add user_id filter in a separate stage to ensure it cannot be overridden
            pipeline.append({"$match": {"user_id": user_id}})

            # Remove score from final results
            pipeline.append({"$project": {"score": 0}})

            # Execute search
            results = []
            async for doc in self._datasets.aggregate(pipeline):
                dataset = Dataset.model_validate(doc)
                results.append(dataset)

            logger.info(f"Found {len(results)} similar datasets")
            return results

        except Exception as e:
            raise DatabaseError(f"Failed to perform vector search: {str(e)}")

    async def get_all_records(self, user_id: str, dataset_id: UUID) -> List[Record]:
        """Retrieves all records in the specified dataset."""
        try:
            logger.info(f"Getting all records from dataset {dataset_id} for user {user_id}")
            # Verify dataset exists
            await self.get_dataset(user_id, dataset_id)

            # Get all records
            records = []
            cursor = self._records.find({"user_id": user_id, "dataset_id": str(dataset_id)})
            async for doc in cursor:
                records.append(Record.model_validate(doc))

            logger.info(f"Retrieved {len(records)} records")
            return records

        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get all records: {str(e)}")

    async def query_records(self, user_id: str, dataset_id: UUID, query: Optional[RecordQuery] = None) -> Union[List[Record], List[Dict]]:
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
                # Simple query - return Record objects
                records = []
                async for doc in cursor:
                    records.append(Record.model_validate(doc))
                logger.info(f"Query returned {len(records)} records")
                return records

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to query records: {str(e)}")
