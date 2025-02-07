"""Tests for the dataset manager."""

from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pymongo
import pytest
import pytest_asyncio
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from document_store.dataset_manager import DatasetManager
from document_store.exceptions import (
    DatabaseError,
    DatasetNameExistsError,
    DatasetNotFoundError,
    InvalidRecordDataError,
    RecordNotFoundError,
)
from document_store.models import Dataset, Record
from document_store.types import DatasetSchema, SchemaField, FieldType


@pytest.fixture
def user_id() -> str:
    """Test user ID."""
    return "test_user"


@pytest.fixture
def dataset_id() -> str:
    """Test dataset ID."""
    return str(ObjectId())


@pytest.fixture
def record_id() -> str:
    """Test record ID."""
    return str(ObjectId())


@pytest.fixture
def sample_schema() -> DatasetSchema:
    """Sample dataset schema for testing."""
    return [
        SchemaField(field_name="age", description="User age", type=FieldType.INTEGER, required=True),
        SchemaField(field_name="name", description="User name", type=FieldType.STRING, required=True),
    ]


@pytest.fixture
def sample_dataset(user_id: str, dataset_id: str, sample_schema: DatasetSchema) -> Dataset:
    """Sample dataset for testing."""
    return Dataset(
        id=dataset_id,
        user_id=user_id,
        name="test_dataset",
        description="Test dataset",
        schema=sample_schema,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_record(user_id: str, dataset_id: str, record_id: str) -> Record:
    """Sample record for testing."""
    return Record(
        id=record_id,
        user_id=user_id,
        dataset_id=dataset_id,
        data={"age": 25, "name": "John"},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


def create_mock_collection(return_doc: dict = None) -> AsyncMock:
    """Create a mock MongoDB collection with configurable return values."""
    collection = AsyncMock(spec=AsyncIOMotorCollection)

    # Configure find_one to return the specified document
    collection.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=return_doc)()

    # Configure other async methods
    result = AsyncMock()
    result.inserted_id = ObjectId()
    collection.insert_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()

    result_update = AsyncMock()
    result_update.modified_count = 1
    collection.update_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_update)()
    collection.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_update)()

    result_delete = AsyncMock()
    result_delete.deleted_count = 1
    collection.delete_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_delete)()
    collection.delete_many.side_effect = lambda *args, **kwargs: AsyncMock(return_value=AsyncMock())()

    # Configure find() to return a cursor that supports async iteration
    cursor = AsyncMock()
    cursor.__aiter__.return_value = []
    collection.find.side_effect = lambda *args, **kwargs: cursor

    return collection


@pytest.fixture
def mock_collection() -> AsyncMock:
    """Mock MongoDB collection."""
    return create_mock_collection()


@pytest.fixture
def mock_db(mock_collection: AsyncMock) -> AsyncMock:
    """Mock MongoDB database."""
    db = AsyncMock(spec=AsyncIOMotorDatabase)
    db.get_collection.return_value = mock_collection
    return db


@pytest.fixture
def mock_client(mock_db: AsyncMock) -> AsyncMock:
    """Mock MongoDB client."""
    client = AsyncMock(spec=AsyncIOMotorClient)
    client.get_database.return_value = mock_db
    return client


@pytest.fixture
def mock_datasets_collection() -> AsyncMock:
    """Mock datasets collection with configurable behavior."""
    collection = AsyncMock(spec=AsyncIOMotorCollection)

    # Configure create_indexes
    collection.create_indexes.side_effect = lambda x: AsyncMock(return_value=[])()

    # Configure find_one with default return value
    collection.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=None)()

    # Configure insert_one with ObjectId result
    result = AsyncMock()
    result.inserted_id = ObjectId()
    collection.insert_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()

    # Configure update operations
    result_update = AsyncMock()
    result_update.modified_count = 1
    collection.update_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_update)()
    collection.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_update)()

    # Configure delete operations
    result_delete = AsyncMock()
    result_delete.deleted_count = 1
    collection.delete_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_delete)()
    collection.delete_many.side_effect = lambda *args, **kwargs: AsyncMock(return_value=AsyncMock())()

    # Configure find with cursor
    cursor = AsyncMock()
    cursor.__aiter__.return_value = []
    collection.find.side_effect = lambda *args, **kwargs: cursor

    return collection


@pytest.fixture
def mock_records_collection() -> AsyncMock:
    """Mock records collection with configurable behavior."""
    collection = AsyncMock(spec=AsyncIOMotorCollection)

    # Configure create_indexes
    collection.create_indexes.side_effect = lambda x: AsyncMock(return_value=[])()

    # Configure find_one with default return value
    collection.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=None)()

    # Configure insert_one with ObjectId result
    result = AsyncMock()
    result.inserted_id = ObjectId()
    collection.insert_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()

    # Configure update operations
    result_update = AsyncMock()
    result_update.modified_count = 1
    collection.update_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_update)()

    # Configure delete operations
    result_delete = AsyncMock()
    result_delete.deleted_count = 1
    collection.delete_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_delete)()
    collection.delete_many.side_effect = lambda *args, **kwargs: AsyncMock(return_value=AsyncMock())()

    # Configure find with cursor
    cursor = AsyncMock()
    cursor.__aiter__.return_value = []
    collection.find.side_effect = lambda *args, **kwargs: cursor

    return collection


@pytest_asyncio.fixture
async def manager(mock_client: AsyncMock, sample_dataset: Dataset, sample_record: Record) -> DatasetManager:
    """Dataset manager instance for testing."""
    # Create fresh collections for each test
    datasets_collection = AsyncMock(spec=AsyncIOMotorCollection)
    records_collection = AsyncMock(spec=AsyncIOMotorCollection)

    # Configure create_indexes
    datasets_collection.create_indexes.side_effect = lambda x: AsyncMock(return_value=[])()
    records_collection.create_indexes.side_effect = lambda x: AsyncMock(return_value=[])()

    # Configure datasets collection with sample dataset
    datasets_collection.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()
    result = AsyncMock()
    result.inserted_id = ObjectId(sample_dataset.id)
    datasets_collection.insert_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
    result_update = AsyncMock()
    result_update.modified_count = 1
    datasets_collection.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_update)()
    result_delete = AsyncMock()
    result_delete.deleted_count = 1
    datasets_collection.delete_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_delete)()
    cursor = AsyncMock()
    cursor.__aiter__.return_value = [sample_dataset.model_dump(by_alias=True)]
    datasets_collection.find.side_effect = lambda *args, **kwargs: cursor

    # Configure records collection with sample record
    records_collection.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_record.model_dump(by_alias=True))()
    result = AsyncMock()
    result.inserted_id = ObjectId(sample_record.id)
    records_collection.insert_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
    result_update = AsyncMock()
    result_update.modified_count = 1
    records_collection.update_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_update)()
    result_delete = AsyncMock()
    result_delete.deleted_count = 1
    records_collection.delete_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result_delete)()
    records_collection.delete_many.side_effect = lambda *args, **kwargs: AsyncMock(return_value=AsyncMock())()
    cursor = AsyncMock()
    cursor.__aiter__.return_value = [sample_record.model_dump(by_alias=True)]
    records_collection.find.side_effect = lambda *args, **kwargs: cursor

    # Configure get_collection to return appropriate collection
    mock_client.get_database().get_collection.side_effect = lambda name: {
        DatasetManager.COLLECTION_DATASETS: datasets_collection,
        DatasetManager.COLLECTION_RECORDS: records_collection,
    }[name]

    return await DatasetManager.setup(mock_client)


@pytest.mark.asyncio
class TestManagerSetup:
    """Tests for manager setup."""

    async def test_setup_creates_indexes(self, mock_client: AsyncMock, mock_db: AsyncMock) -> None:
        """Test setup creates required indexes."""
        # Setup
        datasets_collection = AsyncMock()
        records_collection = AsyncMock()
        # Configure create_indexes to return completed coroutines
        datasets_collection.create_indexes.return_value = AsyncMock(return_value=[])()
        records_collection.create_indexes.return_value = AsyncMock(return_value=[])()

        mock_db.get_collection.side_effect = lambda name: {
            DatasetManager.COLLECTION_DATASETS: datasets_collection,
            DatasetManager.COLLECTION_RECORDS: records_collection,
        }[name]

        # Execute
        manager = await DatasetManager.setup(mock_client)

        # Verify
        assert isinstance(manager, DatasetManager)

        # Verify datasets indexes were created
        datasets_collection.create_indexes.assert_called_once()
        datasets_indexes = datasets_collection.create_indexes.call_args[0][0]
        assert len(datasets_indexes) == 2

        # Verify records indexes were created
        records_collection.create_indexes.assert_called_once()
        records_indexes = records_collection.create_indexes.call_args[0][0]
        assert len(records_indexes) == 2

    async def test_setup_handles_errors(self, mock_client: AsyncMock, mock_db: AsyncMock) -> None:
        """Test setup handles index creation errors."""
        # Setup
        datasets_collection = AsyncMock()
        datasets_collection.create_indexes.side_effect = Exception("Failed to create indexes")
        mock_db.get_collection.return_value = datasets_collection

        # Execute and verify
        with pytest.raises(DatabaseError) as exc_info:
            await DatasetManager.setup(mock_client)
        assert "Failed to setup indexes" in str(exc_info.value)


@pytest.mark.asyncio
class TestDatasetOperations:
    """Tests for dataset operations."""

    async def test_create_dataset(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_schema: DatasetSchema) -> None:
        """Test creating a dataset."""
        # Setup - override manager's mock behavior
        result = AsyncMock()
        result.inserted_id = ObjectId(dataset_id)
        manager._datasets.insert_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()

        # Execute
        result = await manager.create_dataset(user_id=user_id, name="test_dataset", description="Test dataset", schema=sample_schema)

        # Verify
        assert result == ObjectId(dataset_id)

    async def test_create_dataset_duplicate_name(self, manager: DatasetManager, user_id: str, sample_schema: DatasetSchema) -> None:
        """Test creating a dataset with duplicate name fails."""

        # Setup - override manager's mock behavior
        async def mock_insert(*args, **kwargs):
            raise pymongo.errors.DuplicateKeyError("duplicate key error")

        manager._datasets.insert_one = AsyncMock(side_effect=mock_insert)

        # Execute and verify
        with pytest.raises(DatasetNameExistsError):
            await manager.create_dataset(user_id=user_id, name="test_dataset", description="Test dataset", schema=sample_schema)

    async def test_update_dataset(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset, sample_schema: DatasetSchema) -> None:
        """Test updating a dataset."""
        # Execute
        await manager.update_dataset(user_id=user_id, dataset_id=dataset_id, name="updated_dataset", description="Updated dataset", schema=sample_schema)

        # Verify
        filter_doc, update_doc = manager._datasets.replace_one.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["_id"] == dataset_id
        assert update_doc["name"] == "updated_dataset"
        assert update_doc["description"] == "Updated dataset"

    async def test_update_dataset_not_found(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_schema: DatasetSchema) -> None:
        """Test updating a non-existent dataset."""
        # Setup - override manager's mock behavior
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=None)()

        # Execute and verify
        with pytest.raises(DatasetNotFoundError):
            await manager.update_dataset(user_id=user_id, dataset_id=dataset_id, name="updated_dataset", description="Updated dataset", schema=sample_schema)

    async def test_delete_dataset(self, manager: DatasetManager, user_id: str, dataset_id: str) -> None:
        """Test deleting a dataset."""
        # Setup - override manager's mock behavior
        manager._records.delete_many.side_effect = lambda *args, **kwargs: AsyncMock(return_value=AsyncMock())()
        manager._datasets.delete_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=AsyncMock(deleted_count=1))()

        # Execute
        await manager.delete_dataset(user_id, dataset_id)

    async def test_get_dataset(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test retrieving a dataset."""
        # Execute
        result = await manager.get_dataset(user_id, dataset_id)

        # Verify
        assert isinstance(result, Dataset)
        assert str(result.id) == dataset_id
        assert result.user_id == user_id

    async def test_get_dataset_not_found(self, manager: DatasetManager, user_id: str, dataset_id: str) -> None:
        """Test retrieving a non-existent dataset."""
        # Setup - override manager's mock behavior
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=None)()

        # Execute and verify
        with pytest.raises(DatasetNotFoundError):
            await manager.get_dataset(user_id, dataset_id)

    async def test_list_datasets(self, manager: DatasetManager, user_id: str, sample_dataset: Dataset) -> None:
        """Test listing datasets."""
        # Setup - override manager's mock behavior
        cursor = AsyncMock()
        cursor.__aiter__.return_value = [sample_dataset.model_dump(by_alias=True)]
        manager._datasets.find.side_effect = lambda *args, **kwargs: cursor

        # Execute
        results = await manager.list_datasets(user_id)

        # Verify
        assert len(results) == 1
        assert isinstance(results[0], Dataset)
        assert results[0].user_id == user_id


@pytest.mark.asyncio
class TestRecordOperations:
    """Tests for record operations."""

    async def test_create_record(self, manager: DatasetManager, user_id: str, dataset_id: str, record_id: str) -> None:
        """Test creating a record."""
        # Execute
        result = await manager.create_record(user_id=user_id, dataset_id=dataset_id, data={"age": 25, "name": "John"})

        # Verify
        assert result == ObjectId(record_id)

    async def test_create_record_invalid_data(self, manager: DatasetManager, user_id: str, dataset_id: str) -> None:
        """Test creating a record with invalid data fails."""
        # Execute and verify
        with pytest.raises(InvalidRecordDataError):
            await manager.create_record(user_id=user_id, dataset_id=dataset_id, data={"invalid_field": "value"})

    async def test_update_record(self, manager: DatasetManager, user_id: str, dataset_id: str, record_id: str) -> None:
        """Test updating a record."""
        # Execute
        await manager.update_record(user_id=user_id, dataset_id=dataset_id, record_id=record_id, data={"age": 30, "name": "John Doe"})

    async def test_update_record_not_found(self, manager: DatasetManager, user_id: str, dataset_id: str, record_id: str) -> None:
        """Test updating a non-existent record."""
        # Setup - override the default mock behavior for this test
        manager._records.update_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=AsyncMock(modified_count=0))()
        manager._records.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=None)()

        # Execute and verify
        with pytest.raises(RecordNotFoundError):
            await manager.update_record(user_id=user_id, dataset_id=dataset_id, record_id=record_id, data={"age": 30, "name": "John Doe"})

    async def test_delete_record(self, manager: DatasetManager, user_id: str, dataset_id: str, record_id: str) -> None:
        """Test deleting a record."""
        # Execute
        await manager.delete_record(user_id, dataset_id, record_id)

    async def test_get_record(self, manager: DatasetManager, user_id: str, dataset_id: str, record_id: str, sample_record: Record) -> None:
        """Test retrieving a record."""
        # Execute
        result = await manager.get_record(user_id, dataset_id, record_id)

        # Verify
        assert isinstance(result, Record)
        assert str(result.id) == record_id
        assert result.user_id == user_id
        assert result.dataset_id == ObjectId(dataset_id)

    async def test_find_records(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_record: Record) -> None:
        """Test finding records with query."""
        # Execute
        results = await manager.find_records(user_id=user_id, dataset_id=dataset_id, query={"age": 25})

        # Verify
        assert len(results) == 1
        assert isinstance(results[0], Record)
        assert results[0].user_id == user_id
        assert str(results[0].dataset_id) == dataset_id
