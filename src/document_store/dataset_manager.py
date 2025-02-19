"""Dataset manager for MongoDB operations."""

from datetime import datetime, timezone
from typing import Dict, List, Optional

import pymongo
from bson import ObjectId
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo.errors import BulkWriteError

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
from document_store.models.query import AggregationQuery
from document_store.models.record import RecordData
from document_store.models.schema import DatasetSchema
from document_store.models.types import FieldType
from document_store.pipeline import build_aggregation_pipeline
from document_store.type_validators.factory import get_validator
from document_store.type_validators.record import validate_query_fields


class DatasetManager:
    """Manager for dataset and record operations."""

    DATABASE: str = "document_store"
    COLLECTION_DATASETS: str = "datasets"
    COLLECTION_RECORDS: str = "records"

    def __init__(self, mongodb_client: AsyncIOMotorClient) -> None:
        """Initialize manager with MongoDB client.
        Note: Use DatasetManager.setup() to create a properly initialized instance."""
        self.client = mongodb_client
        self._db: AsyncIOMotorDatabase = self.client.get_database(self.DATABASE)
        self._datasets: AsyncIOMotorCollection = self._db.get_collection(self.COLLECTION_DATASETS)
        self._records: AsyncIOMotorCollection = self._db.get_collection(self.COLLECTION_RECORDS)

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

            return manager

        except Exception as e:
            raise DatabaseError(f"Failed to setup indexes: {str(e)}")

    async def create_dataset(self, user_id: str, name: str, description: str, schema: DatasetSchema) -> ObjectId:
        """Creates a new dataset with the given schema."""
        try:
            dataset = Dataset(
                user_id=user_id,
                name=name,
                description=description,
                dataset_schema=schema,
            )
            result = await self._datasets.insert_one(dataset.model_dump(by_alias=True))
            return result.inserted_id
        except Exception as e:
            if "duplicate key error" in str(e).lower():
                raise DatasetNameExistsError(f"Dataset with name '{name}' already exists for user {user_id}")
            raise DatabaseError(f"Failed to create dataset: {str(e)}")

    async def update_dataset(self, user_id: str, dataset_id: ObjectId, name: str, description: str) -> None:
        """Updates dataset metadata (name and description)."""
        try:
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

            # Update in database
            result = await self._datasets.replace_one(
                {"_id": dataset_id, "user_id": user_id},
                updated.model_dump(by_alias=True),
            )

            if result.modified_count == 0:
                raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

        except DatasetNotFoundError:
            raise
        except Exception as e:
            if "duplicate key error" in str(e).lower():
                raise DatasetNameExistsError(f"Dataset with name '{name}' already exists for user {user_id}")
            raise DatabaseError(f"Failed to update dataset: {str(e)}")

    async def delete_dataset(self, user_id: str, dataset_id: ObjectId) -> None:
        """Deletes a dataset and all its records."""
        try:
            # Verify dataset exists and belongs to user
            await self.get_dataset(user_id, dataset_id)

            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Delete dataset and its records
                    await self._records.delete_many(
                        {
                            "user_id": user_id,
                            "dataset_id": dataset_id,
                        },
                        session=session,
                    )

                    result = await self._datasets.delete_one(
                        {
                            "_id": dataset_id,
                            "user_id": user_id,
                        },
                        session=session,
                    )

                    if result.deleted_count == 0:
                        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to delete dataset: {str(e)}")

    async def list_datasets(self, user_id: str) -> List[Dataset]:
        """Lists all datasets belonging to the user."""
        try:
            datasets = []
            cursor = self._datasets.find({"user_id": user_id})
            async for doc in cursor:
                datasets.append(Dataset.model_validate(doc))
            return datasets
        except Exception as e:
            raise DatabaseError(f"Failed to list datasets: {str(e)}")

    async def get_dataset(self, user_id: str, dataset_id: ObjectId) -> Dataset:
        """Retrieves a specific dataset."""
        try:
            doc = await self._datasets.find_one({"_id": dataset_id, "user_id": user_id})
            if not doc:
                raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
            return Dataset.model_validate(doc)
        except DatasetNotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get dataset: {str(e)}")

    async def _prepare_record_updates(
        self, user_id: str, dataset_id: ObjectId, field_name: str, old_field: SchemaField, field_update: SchemaField, session
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

        # Get validator for new type
        validator = get_validator(field_update.type)
        if field_update.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
            validator.set_options(field_update.options)

        # Get records with this field using session
        mongo_query = {"user_id": user_id, "dataset_id": dataset_id, f"data.{field_name}": {"$exists": True}}  # Only get records that have this field

        records = []
        cursor = self._records.find(mongo_query, session=session)
        async for doc in cursor:
            records.append(Record.model_validate(doc))

        updates = []
        for record in records:
            try:
                # Convert and validate value
                converted_value = validator.validate(record.data[field_name])

                # Create update operation
                updates.append(
                    pymongo.UpdateOne(
                        {
                            "_id": record.id,
                            "user_id": user_id,
                            "dataset_id": dataset_id,
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

    async def delete_field(self, user_id: str, dataset_id: ObjectId, field_name: str) -> None:
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
            new_schema = [field for field in dataset.dataset_schema if field.field_name != field_name]

            # Validate schema - will raise InvalidDatasetSchemaError if field doesn't exist
            if len(new_schema) == len(dataset.dataset_schema):
                raise InvalidDatasetSchemaError(f"Field '{field_name}' not found in schema")

            # Start transaction
            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Update dataset schema
                    updated = Dataset(
                        id=dataset_id,
                        user_id=user_id,
                        name=dataset.name,
                        description=dataset.description,
                        dataset_schema=new_schema,
                        created_at=dataset.created_at,
                        updated_at=datetime.now(timezone.utc),
                    )

                    result = await self._datasets.replace_one(
                        {"_id": dataset_id, "user_id": user_id},
                        updated.model_dump(by_alias=True),
                        session=session,
                    )

                    if result.modified_count == 0:
                        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

                    # Remove field from all records
                    await self._records.update_many(
                        {"user_id": user_id, "dataset_id": dataset_id},
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
        dataset_id: ObjectId,
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
            new_schema = [*dataset.dataset_schema, field]

            # Start transaction
            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    # Update dataset schema
                    updated = Dataset(
                        id=dataset_id,
                        user_id=user_id,
                        name=dataset.name,
                        description=dataset.description,
                        dataset_schema=new_schema,
                        created_at=dataset.created_at,
                        updated_at=datetime.now(timezone.utc),
                    )

                    result = await self._datasets.replace_one(
                        {"_id": dataset_id, "user_id": user_id},
                        updated.model_dump(by_alias=True),
                        session=session,
                    )

                    if result.modified_count == 0:
                        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")

                    # Initialize field in existing records if default value provided
                    if field.default is not None:
                        await self._records.update_many(
                            {"user_id": user_id, "dataset_id": dataset_id},
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
        dataset_id: ObjectId,
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
                    # Update dataset schema
                    updated = Dataset(
                        id=dataset_id,
                        user_id=user_id,
                        name=dataset.name,
                        description=dataset.description,
                        dataset_schema=new_schema,
                        created_at=dataset.created_at,
                        updated_at=datetime.now(timezone.utc),
                    )

                    result = await self._datasets.replace_one(
                        {"_id": dataset_id, "user_id": user_id},
                        updated.model_dump(by_alias=True),
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

    async def create_record(self, user_id: str, dataset_id: ObjectId, data: RecordData) -> ObjectId:
        """Creates a new record in the specified dataset."""
        try:
            # Get dataset to validate against schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert data
            validated_data = Record.validate_data(data, dataset.dataset_schema)

            # Create record
            record = Record(
                user_id=user_id,
                dataset_id=dataset_id,
                data=validated_data,
            )

            result = await self._records.insert_one(record.model_dump(by_alias=True))
            return result.inserted_id

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to create record: {str(e)}")

    async def update_record(self, user_id: str, dataset_id: ObjectId, record_id: ObjectId, data: RecordData) -> None:
        """Updates an existing record."""
        try:
            # Get dataset to validate against schema
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert data
            validated_data = Record.validate_data(data, dataset.dataset_schema)

            # Update record
            result = await self._records.update_one(
                {
                    "_id": record_id,
                    "user_id": user_id,
                    "dataset_id": dataset_id,
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
                        "_id": record_id,
                        "user_id": user_id,
                        "dataset_id": dataset_id,
                    }
                )
                if not record:
                    raise RecordNotFoundError(f"Record {record_id} not found")

        except (DatasetNotFoundError, RecordNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to update record: {str(e)}")

    async def delete_record(self, user_id: str, dataset_id: ObjectId, record_id: ObjectId) -> None:
        """Deletes a record."""
        try:
            # Verify dataset exists
            await self.get_dataset(user_id, dataset_id)

            # Delete record
            result = await self._records.delete_one(
                {
                    "_id": record_id,
                    "user_id": user_id,
                    "dataset_id": dataset_id,
                }
            )

            if result.deleted_count == 0:
                raise RecordNotFoundError(f"Record {record_id} not found")

        except (DatasetNotFoundError, RecordNotFoundError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to delete record: {str(e)}")

    async def get_record(self, user_id: str, dataset_id: ObjectId, record_id: ObjectId) -> Record:
        """Retrieves a specific record."""
        try:
            # Verify dataset exists
            await self.get_dataset(user_id, dataset_id)

            # Get record
            doc = await self._records.find_one(
                {
                    "_id": record_id,
                    "user_id": user_id,
                    "dataset_id": dataset_id,
                }
            )

            if not doc:
                raise RecordNotFoundError(f"Record {record_id} not found")

            return Record.model_validate(doc)

        except (DatasetNotFoundError, RecordNotFoundError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to get record: {str(e)}")

    async def find_records(self, user_id: str, dataset_id: ObjectId, query: Optional[Dict] = None) -> List[Record]:
        """Finds records in the specified dataset."""
        try:
            # Verify dataset exists and get schema for query validation
            dataset = await self.get_dataset(user_id, dataset_id)

            # Build query
            mongo_query = {
                "user_id": user_id,
                "dataset_id": dataset_id,
            }

            if query:
                # Validate query fields exist in schema
                validate_query_fields(query, dataset.dataset_schema)
                # Add data field conditions
                for field, value in query.items():
                    mongo_query[f"data.{field}"] = value

            # Execute query
            records = []
            cursor = self._records.find(mongo_query)
            async for doc in cursor:
                records.append(Record.model_validate(doc))
            return records

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to find records: {str(e)}")

    async def aggregate_records(self, user_id: str, dataset_id: ObjectId, query: AggregationQuery) -> List[Dict]:
        """Perform aggregation operations on records.

        Args:
            user_id: User ID
            dataset_id: Dataset ID
            query: Aggregation query

        Returns:
            List of aggregation results

        Raises:
            DatasetNotFoundError: If dataset not found
            InvalidRecordDataError: If query is invalid
            DatabaseError: For other database errors
        """
        try:
            # Verify dataset exists and get schema for validation
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate query against schema
            query.validate_with_schema(dataset.dataset_schema)

            # Build aggregation pipeline
            pipeline = build_aggregation_pipeline(user_id, dataset_id, query)

            # Execute aggregation
            results = []
            cursor = self._records.aggregate(pipeline)
            async for doc in cursor:
                # If group by was used, move _id contents to top level
                if doc["_id"] and isinstance(doc["_id"], dict):
                    doc.update(doc["_id"])
                doc.pop("_id")
                results.append(doc)

            return results

        except (DatasetNotFoundError, InvalidRecordDataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to aggregate records: {str(e)}")
