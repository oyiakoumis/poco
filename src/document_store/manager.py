"""Dataset manager for MongoDB operations."""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from bson import ObjectId
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

from .exceptions import (
    DatasetNameExistsError,
    DatasetNotFoundError,
    DatabaseError,
    InvalidRecordDataError,
    RecordNotFoundError,
)
from .models import Dataset, Record
from .types import Field, RecordData
from .validators import validate_record_data, validate_query_fields


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

    async def create_dataset(self, user_id: str, name: str, description: str, structure: List[Field]) -> ObjectId:
        """Creates a new dataset with the given structure."""
        try:
            dataset = Dataset(
                user_id=user_id,
                name=name,
                description=description,
                structure=structure,
            )
            result = await self._datasets.insert_one(dataset.model_dump(by_alias=True))
            return result.inserted_id
        except Exception as e:
            if "duplicate key error" in str(e).lower():
                raise DatasetNameExistsError(f"Dataset with name '{name}' already exists for user {user_id}")
            raise DatabaseError(f"Failed to create dataset: {str(e)}")

    async def update_dataset(self, user_id: str, dataset_id: ObjectId, name: str, description: str, structure: List[Field]) -> None:
        """Updates an existing dataset."""
        try:
            # Validate dataset exists and belongs to user
            dataset = await self.get_dataset(user_id, dataset_id)

            # Create updated dataset
            updated = Dataset(
                id=dataset_id,
                user_id=user_id,
                name=name,
                description=description,
                structure=structure,
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

            # Delete dataset and its records
            await self._records.delete_many(
                {
                    "user_id": user_id,
                    "dataset_id": dataset_id,
                }
            )

            result = await self._datasets.delete_one(
                {
                    "_id": dataset_id,
                    "user_id": user_id,
                }
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

    async def create_record(self, user_id: str, dataset_id: ObjectId, data: RecordData) -> ObjectId:
        """Creates a new record in the specified dataset."""
        try:
            # Get dataset to validate against structure
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert data
            validated_data = validate_record_data(data, dataset.structure)

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
            # Get dataset to validate against structure
            dataset = await self.get_dataset(user_id, dataset_id)

            # Validate and convert data
            validated_data = validate_record_data(data, dataset.structure)

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
            # Verify dataset exists and get structure for query validation
            dataset = await self.get_dataset(user_id, dataset_id)

            # Build query
            mongo_query = {
                "user_id": user_id,
                "dataset_id": dataset_id,
            }

            if query:
                # Validate query fields exist in structure
                validate_query_fields(query, dataset.structure)
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
