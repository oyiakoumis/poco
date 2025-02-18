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
    InvalidDatasetSchemaError,
    InvalidRecordDataError,
    RecordNotFoundError,
)
from document_store.models import Dataset, Record
from document_store.types import DatasetSchema, FieldType, SchemaField


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
        dataset_schema=sample_schema,
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

    # Configure session
    class MockTransaction:
        def __init__(self, should_raise=False):
            self.should_raise = should_raise

        async def __aenter__(self):
            if self.should_raise:
                raise ValueError("Transaction error")
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            # Propagate any errors
            return False

    class MockSession:
        def __init__(self, should_raise=False):
            self.should_raise = should_raise

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            # Propagate any errors
            return False

        def start_transaction(self):
            return MockTransaction(self.should_raise)

    # Create session context
    mock_session = MockSession()
    mock_client.start_session = AsyncMock(return_value=mock_session)

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

    async def test_update_dataset(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test updating a dataset."""
        # Execute
        await manager.update_dataset(user_id=user_id, dataset_id=dataset_id, name="updated_dataset", description="Updated dataset")

        # Verify
        filter_doc, update_doc = manager._datasets.replace_one.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["_id"] == dataset_id
        assert update_doc["name"] == "updated_dataset"
        assert update_doc["description"] == "Updated dataset"
        assert update_doc["dataset_schema"] == [c.model_dump(by_alias=True) for c in sample_dataset.dataset_schema]  # Ensure schema is unchanged

    async def test_update_dataset_not_found(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_schema: DatasetSchema) -> None:
        """Test updating a non-existent dataset."""
        # Setup - override manager's mock behavior
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=None)()

        # Execute and verify
        with pytest.raises(DatasetNotFoundError):
            await manager.update_dataset(user_id=user_id, dataset_id=dataset_id, name="updated_dataset", description="Updated dataset")

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


@pytest.mark.asyncio
class TestFieldOperations:
    """Tests for field operations."""

    async def test_update_field_same_type(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test updating a field with same type."""
        # Setup field update
        field_update = SchemaField(
            field_name="age",
            description="Updated age description",
            type=FieldType.INTEGER,
            required=True,
        )

        # Configure collection with session
        result = AsyncMock()
        result.modified_count = 1
        manager._datasets.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()

        # Execute
        await manager.update_field(user_id, dataset_id, "age", field_update)

        # Verify dataset was updated
        filter_doc, update_doc = manager._datasets.replace_one.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["_id"] == dataset_id
        assert any(f["description"] == "Updated age description" for f in update_doc["dataset_schema"])

    async def test_update_field_type_conversion(
        self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset, sample_record: Record
    ) -> None:
        """Test updating a field with type conversion."""
        # Setup field update
        field_update = SchemaField(
            field_name="age",
            description="Age as float",
            type=FieldType.FLOAT,
            required=True,
        )

        # Configure collection with session
        result = AsyncMock()
        result.modified_count = 1
        manager._datasets.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()

        class AsyncIterator:
            def __init__(self, items):
                self.items = items
                self.index = 0

            async def __anext__(self):
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

            def __aiter__(self):
                return self

        # Configure records collection for type conversion
        mock_cursor = AsyncIterator([sample_record.model_dump(by_alias=True)])
        mock_records = MagicMock()
        mock_records.find.return_value = mock_cursor
        manager._records.bulk_write = AsyncMock(return_value=result)

        # Execute
        await manager.update_field(user_id, dataset_id, "age", field_update)

        # Verify dataset was updated
        filter_doc, update_doc = manager._datasets.replace_one.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["_id"] == dataset_id
        assert any(f["type"] == "float" for f in update_doc["dataset_schema"])

        # Verify records were updated
        assert manager._records.bulk_write.called

    async def test_update_field_not_found(self, manager: DatasetManager, user_id: str, dataset_id: str) -> None:
        """Test updating a non-existent field."""
        # Setup field update
        field_update = SchemaField(
            field_name="invalid_field",
            description="Invalid field",
            type=FieldType.STRING,
        )

        # Execute and verify
        with pytest.raises(InvalidDatasetSchemaError) as exc:
            await manager.update_field(user_id, dataset_id, "invalid_field", field_update)
        assert "not found in schema" in str(exc.value)

    async def test_update_field_invalid_conversion(self, manager: DatasetManager, user_id: str, sample_dataset: Dataset, dataset_id: str) -> None:
        """Test updating a field with invalid type conversion."""
        # Setup field update
        field_update = SchemaField(
            field_name="age",
            description="Age as string",
            type=FieldType.BOOLEAN,
        )

        # Configure collection with session
        result = AsyncMock()
        result.modified_count = 1
        manager._datasets.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()

        # Execute and verify
        with pytest.raises(InvalidRecordDataError) as exc:
            await manager.update_field(user_id, dataset_id, "age", field_update)
        assert "Cannot safely convert field" in str(exc.value)

    async def test_add_field(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test adding a new field to dataset schema."""
        # Setup new field
        new_field = SchemaField(field_name="email", description="User email", type=FieldType.STRING, required=False, default="test@example.com")

        # Configure collection with session
        result = AsyncMock()
        result.modified_count = 1
        manager._datasets.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()
        manager._records.update_many = AsyncMock()

        # Execute
        await manager.add_field(user_id, dataset_id, new_field)

        # Verify dataset was updated with new field
        filter_doc, update_doc = manager._datasets.replace_one.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["_id"] == dataset_id
        assert len(update_doc["dataset_schema"]) == len(sample_dataset.dataset_schema) + 1
        assert any(f["field_name"] == "email" for f in update_doc["dataset_schema"])

        # Verify records were updated with default value
        manager._records.update_many.assert_called_once()
        filter_doc, update_doc = manager._records.update_many.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["dataset_id"] == dataset_id
        assert update_doc["$set"]["data.email"] == "test@example.com"

    async def test_add_field_no_default(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test adding a new field without default value."""
        # Setup new field without default
        new_field = SchemaField(field_name="email", description="User email", type=FieldType.STRING, required=False)

        # Configure collection
        result = AsyncMock()
        result.modified_count = 1
        manager._datasets.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()

        # Execute
        await manager.add_field(user_id, dataset_id, new_field)

        # Verify dataset was updated
        filter_doc, update_doc = manager._datasets.replace_one.call_args[0]
        assert len(update_doc["dataset_schema"]) == len(sample_dataset.dataset_schema) + 1

        # Verify records were not updated (no default value)
        manager._records.update_many.assert_not_called()

    async def test_add_field_duplicate(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test adding a duplicate field fails."""
        # Setup duplicate field
        duplicate_field = SchemaField(field_name="age", description="Duplicate age field", type=FieldType.INTEGER)  # Already exists in sample_schema

        # Configure collection
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()

        # Execute and verify
        with pytest.raises(InvalidDatasetSchemaError) as exc:
            await manager.add_field(user_id, dataset_id, duplicate_field)
        assert "Duplicate field names in schema" in str(exc.value)

    async def test_delete_field(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test deleting a field from dataset schema."""
        # Configure collection with session
        result = AsyncMock()
        result.modified_count = 1
        manager._datasets.replace_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=result)()
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()
        manager._records.update_many = AsyncMock()

        # Execute
        await manager.delete_field(user_id, dataset_id, "age")

        # Verify dataset schema was updated
        filter_doc, update_doc = manager._datasets.replace_one.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["_id"] == dataset_id
        assert len(update_doc["dataset_schema"]) == len(sample_dataset.dataset_schema) - 1
        assert not any(f["field_name"] == "age" for f in update_doc["dataset_schema"])

        # Verify field was removed from records
        manager._records.update_many.assert_called_once()
        filter_doc, update_doc = manager._records.update_many.call_args[0]
        assert filter_doc["user_id"] == user_id
        assert filter_doc["dataset_id"] == dataset_id
        assert update_doc["$unset"]["data.age"] == ""

    async def test_delete_field_not_found(self, manager: DatasetManager, user_id: str, dataset_id: str, sample_dataset: Dataset) -> None:
        """Test deleting a non-existent field fails."""
        # Configure collection
        manager._datasets.find_one.side_effect = lambda *args, **kwargs: AsyncMock(return_value=sample_dataset.model_dump(by_alias=True))()

        # Execute and verify
        with pytest.raises(InvalidDatasetSchemaError) as exc:
            await manager.delete_field(user_id, dataset_id, "invalid_field")
        assert "not found in schema" in str(exc.value)
