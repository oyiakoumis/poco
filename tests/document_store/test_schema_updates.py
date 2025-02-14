"""Integration tests for schema update functionality."""

from datetime import datetime, timezone
import pytest
from bson import ObjectId
from unittest.mock import AsyncMock, MagicMock, patch

import pytest_asyncio
from tests.document_store.test_manager import mock_client, mock_db, mock_collection

from document_store.exceptions import (
    DatasetNotFoundError,
    InvalidSchemaUpdateError,
    TypeConversionError,
)
from document_store.dataset_manager import DatasetManager
from document_store.models import Dataset
from document_store.types import FieldType, SchemaField


@pytest_asyncio.fixture
def schema_update_dataset():
    """Create a sample dataset for schema update testing."""
    return Dataset(
        id=ObjectId(),
        user_id="test_user",
        name="test_dataset",
        description="Test dataset",
        dataset_schema=[
            SchemaField(
                field_name="count",
                description="Item count",
                type=FieldType.INTEGER,
            ),
            SchemaField(
                field_name="price",
                description="Item price",
                type=FieldType.FLOAT,
            ),
            SchemaField(
                field_name="name",
                description="Item name",
                type=FieldType.STRING,
            ),
            SchemaField(
                field_name="status",
                description="Item status",
                type=FieldType.SELECT,
                options=["active", "inactive"],
            ),
        ],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_records():
    """Sample records for testing schema updates."""
    return [
        {
            "_id": ObjectId(),
            "user_id": "test_user",
            "dataset_id": ObjectId(),
            "data": {
                "count": 42,
                "price": 9.99,
                "name": "Test Item",
                "status": "active",
            },
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        },
        {
            "_id": ObjectId(),
            "user_id": "test_user",
            "dataset_id": ObjectId(),
            "data": {
                "count": 10,
                "price": 5.50,
                "name": "Another Item",
                "status": "inactive",
            },
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        },
    ]


@pytest_asyncio.fixture
async def schema_update_manager(mock_client, schema_update_dataset, sample_records):
    """Dataset manager configured for schema update testing."""
    # Create collections with schema update specific behavior
    datasets_collection = AsyncMock()
    records_collection = AsyncMock()

    # Configure datasets collection
    datasets_collection.create_indexes.return_value = []

    # Configure find_one to return a dictionary directly
    async def mock_find_one(*args, **kwargs):
        return schema_update_dataset.model_dump(by_alias=True)

    datasets_collection.find_one.side_effect = mock_find_one

    # Configure records collection
    records_collection.create_indexes.return_value = []

    # Configure aggregate to return proper async iterator
    async def mock_aiter(self):  # Add self parameter
        for record in sample_records:
            yield record

    cursor = AsyncMock()
    cursor.__aiter__ = mock_aiter

    # Make aggregate return the cursor directly, not a coroutine
    records_collection.aggregate = lambda *args, **kwargs: cursor

    # Configure bulk_write
    records_collection.bulk_write.return_value = AsyncMock()

    # Configure session management
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.__aexit__.return_value = None

    # Configure transaction management
    transaction = AsyncMock()
    transaction.__aenter__.return_value = transaction
    transaction.__aexit__.return_value = None

    # Create a proper async context manager for transactions
    class AsyncTransactionContextManager:
        async def __aenter__(self):
            return transaction
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    # Make start_transaction return our async context manager
    session.start_transaction = lambda: AsyncTransactionContextManager()

    # Configure client with session
    mock_client.start_session = AsyncMock()
    mock_client.start_session.return_value = AsyncMock()
    mock_client.start_session.return_value.__aenter__ = AsyncMock(return_value=session)
    mock_client.start_session.return_value.__aexit__ = AsyncMock(return_value=None)

    # Configure database
    mock_db = AsyncMock()
    mock_db.get_collection = lambda name: {
        DatasetManager.COLLECTION_DATASETS: datasets_collection,
        DatasetManager.COLLECTION_RECORDS: records_collection,
    }[name]
    mock_client.get_database.return_value = mock_db

    manager = await DatasetManager.setup(mock_client)

    # Store references for test assertions
    manager._session = session
    return manager


@pytest.mark.asyncio
class TestSchemaUpdates:
    """Tests for schema update operations."""

    async def test_update_schema_type_conversions(self, schema_update_manager, schema_update_dataset):
        """Test schema update with type conversions."""
        # Create new schema with type conversions
        new_schema = [
            SchemaField(
                field_name="count",
                description="Item count",
                type=FieldType.FLOAT,  # INT to FLOAT
            ),
            SchemaField(
                field_name="price",
                description="Item price",
                type=FieldType.STRING,  # FLOAT to STRING
            ),
            SchemaField(
                field_name="name",
                description="Item name",
                type=FieldType.STRING,  # No change
            ),
            SchemaField(
                field_name="status",
                description="Item status",
                type=FieldType.SELECT,
                options=["active", "inactive"],  # No change
            ),
        ]

        # Execute update
        await schema_update_manager.update_schema(
            user_id="test_user",
            dataset_id=schema_update_dataset.id,
            new_schema=new_schema,
        )

        # Verify bulk write was called with correct operations
        bulk_ops = schema_update_manager._records.bulk_write.call_args[0][0]
        assert len(bulk_ops) == 2  # Two records updated

        # Verify schema was updated
        update_call = schema_update_manager._datasets.update_one.call_args
        assert update_call is not None
        filter_doc, update_doc = update_call[0]
        assert filter_doc["_id"] == schema_update_dataset.id
        assert filter_doc["user_id"] == "test_user"
        assert len(update_doc["$set"]["dataset_schema"]) == 4

    async def test_update_schema_new_fields(self, schema_update_manager, schema_update_dataset):
        """Test schema update with new fields."""
        new_schema = [*schema_update_dataset.dataset_schema]
        new_schema.append(
            SchemaField(
                field_name="description",
                description="Item description",
                type=FieldType.STRING,
                required=True,
                default="No description",
            )
        )

        # Execute update
        await schema_update_manager.update_schema(
            user_id="test_user",
            dataset_id=schema_update_dataset.id,
            new_schema=new_schema,
        )

        # Verify bulk write operations
        bulk_ops = schema_update_manager._records.bulk_write.call_args[0][0]
        assert len(bulk_ops) == 2  # Two records updated

        # Verify new field was added with default value
        for op in bulk_ops:
            assert op._doc["$set"]["data"]["description"] == "No description"

    async def test_update_schema_remove_fields(self, schema_update_manager, schema_update_dataset):
        """Test schema update with removed fields."""
        new_schema = [field for field in schema_update_dataset.dataset_schema if field.field_name != "status"]  # Remove status field

        # Execute update
        await schema_update_manager.update_schema(
            user_id="test_user",
            dataset_id=schema_update_dataset.id,
            new_schema=new_schema,
        )

        # Verify bulk write operations
        bulk_ops = schema_update_manager._records.bulk_write.call_args[0][0]
        assert len(bulk_ops) == 2  # Two records updated

        # Verify removed field is not in updated data
        for op in bulk_ops:
            assert "status" not in op._doc["$set"]["data"]

    async def test_update_schema_invalid_conversion(self, schema_update_manager, schema_update_dataset):
        """Test schema update with invalid type conversion."""
        new_schema = [*schema_update_dataset.dataset_schema]
        new_schema[0] = SchemaField(
            field_name="count",
            description="Item count",
            type=FieldType.DATE,  # Invalid conversion from INTEGER
        )

        # Execute and verify
        with pytest.raises(InvalidSchemaUpdateError) as exc:
            await schema_update_manager.update_schema(
                user_id="test_user",
                dataset_id=schema_update_dataset.id,
                new_schema=new_schema,
            )
        assert "Cannot convert field" in str(exc.value)

    async def test_update_schema_dataset_not_found(self, schema_update_manager, schema_update_dataset):
        """Test schema update with non-existent dataset."""

        # Configure find_one to return None
        async def mock_find_one(*args, **kwargs):
            return None

        schema_update_manager._datasets.find_one.side_effect = mock_find_one

        with pytest.raises(DatasetNotFoundError):
            await schema_update_manager.update_schema(
                user_id="test_user",
                dataset_id=schema_update_dataset.id,
                new_schema=schema_update_dataset.dataset_schema,
            )

    async def test_update_schema_conversion_error(self, schema_update_manager, schema_update_dataset):
        """Test schema update with conversion error during record update."""
        # Modify a record to have invalid data for conversion
        bad_records = [
            {
                "_id": ObjectId(),
                "user_id": "test_user",
                "dataset_id": ObjectId(),
                "data": {
                    "count": "not a number",  # This will fail float conversion
                    "price": 9.99,
                    "name": "Test Item",
                    "status": "active",
                },
            }
        ]

        # Configure aggregate to return bad records with proper async iterator
        async def mock_aiter(self):  # Add self parameter
            for record in bad_records:
                yield record

        cursor = AsyncMock()
        cursor.__aiter__ = mock_aiter  # Set directly as method
        schema_update_manager._records.aggregate = lambda *args, **kwargs: cursor  # Return cursor directly

        new_schema = [*schema_update_dataset.dataset_schema]
        new_schema[0] = SchemaField(
            field_name="count",
            description="Item count",
            type=FieldType.FLOAT,  # Try to convert to float
        )

        # Execute and verify
        with pytest.raises(TypeConversionError) as exc:
            await schema_update_manager.update_schema(
                user_id="test_user",
                dataset_id=schema_update_dataset.id,
                new_schema=new_schema,
            )
        assert "Failed to convert" in str(exc.value)

    async def test_update_schema_transaction_rollback(self, schema_update_manager, schema_update_dataset):
        """Test schema update transaction rollback on error."""
        # Configure bulk_write to raise an error
        schema_update_manager._records.bulk_write.side_effect = Exception("Database error")

        new_schema = [*schema_update_dataset.dataset_schema]
        new_schema[0] = SchemaField(
            field_name="count",
            description="Item count",
            type=FieldType.FLOAT,
        )

        # Execute and verify
        with pytest.raises(Exception):
            await schema_update_manager.update_schema(
                user_id="test_user",
                dataset_id=schema_update_dataset.id,
                new_schema=new_schema,
            )

        # Verify schema was not updated (transaction rolled back)
        schema_update_manager._datasets.update_one.assert_not_called()
