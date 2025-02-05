import pytest
import pytest_asyncio
from datetime import datetime
from typing import Dict, Any

from motor.motor_asyncio import AsyncIOMotorClient
from models.schema import (
    AggregateFunction,
    AggregateMetric,
    AggregationQuery,
    CollectionSchema,
    DocumentQuery,
    FieldDefinition,
    FieldType,
)
from database_manager.document_db import DocumentDB
from database_manager.exceptions import (
    CollectionNotFoundError,
    DocumentNotFoundError,
    DuplicateCollectionError,
)


@pytest_asyncio.fixture
async def db():
    """Create a test database."""
    from constants import DATABASE_CONNECTION_STRING

    client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
    db_name = "test_document_db"
    # Clean up any existing test database
    await client.drop_database(db_name)

    try:
        doc_db = DocumentDB(DATABASE_CONNECTION_STRING, db_name)
        await doc_db.initialize()
        return doc_db
    finally:
        await client.drop_database(db_name)
        client.close()


@pytest.fixture
def user_id():
    return "test_user"


@pytest.fixture
def test_schema():
    return CollectionSchema(
        name="test_collection",
        description="Test collection for unit tests",
        fields=[
            FieldDefinition(
                name="title",
                description="Document title",
                field_type=FieldType.STRING,
                required=True,
            ),
            FieldDefinition(
                name="count",
                description="A number field",
                field_type=FieldType.INTEGER,
                default=0,
            ),
            FieldDefinition(
                name="tags",
                description="Document tags",
                field_type=FieldType.MULTI_SELECT,
                options=["tag1", "tag2", "tag3"],
            ),
            FieldDefinition(
                name="status",
                description="Document status",
                field_type=FieldType.SELECT,
                options=["draft", "published", "archived"],
                default="draft",
            ),
            FieldDefinition(
                name="created_at",
                description="Creation timestamp",
                field_type=FieldType.DATETIME,
                default=datetime.utcnow,
            ),
        ],
    )


@pytest.mark.asyncio
async def test_create_collection(db: DocumentDB, user_id: str, test_schema: CollectionSchema):
    """Test creating a new collection."""
    # Create collection
    collection_name = await db.create_collection(user_id, test_schema)
    assert collection_name == test_schema.name

    # Verify collection exists
    collections = await db.list_collections(user_id)
    assert len(collections) == 1
    assert collections[0].name == test_schema.name

    # Try creating duplicate collection
    with pytest.raises(DuplicateCollectionError):
        await db.create_collection(user_id, test_schema)


@pytest.mark.asyncio
async def test_crud_operations(db: DocumentDB, user_id: str, test_schema: CollectionSchema):
    """Test CRUD operations on documents."""
    # Create collection
    await db.create_collection(user_id, test_schema)

    # Create document
    doc = {
        "title": "Test Document",
        "count": 42,
        "tags": ["tag1", "tag2"],
        "status": "draft",
    }
    doc_id = await db.create_document(user_id, test_schema.name, doc)
    assert doc_id is not None

    # Read document
    docs = await db.get_documents(user_id, test_schema.name)
    assert len(docs) == 1
    assert docs[0]["title"] == "Test Document"
    assert docs[0]["count"] == 42
    assert docs[0]["tags"] == ["tag1", "tag2"]
    assert docs[0]["status"] == "draft"

    # Update document
    update_doc = {"title": "Updated Document", "status": "published"}
    await db.update_document(user_id, test_schema.name, doc_id, update_doc)

    # Verify update
    docs = await db.get_documents(user_id, test_schema.name)
    assert len(docs) == 1
    assert docs[0]["title"] == "Updated Document"
    assert docs[0]["status"] == "published"
    assert docs[0]["count"] == 42  # Unchanged fields remain

    # Delete document
    await db.delete_document(user_id, test_schema.name, doc_id)

    # Verify deletion
    docs = await db.get_documents(user_id, test_schema.name)
    assert len(docs) == 0


@pytest.mark.asyncio
async def test_query_operations(db: DocumentDB, user_id: str, test_schema: CollectionSchema):
    """Test query operations."""
    # Create collection
    await db.create_collection(user_id, test_schema)

    # Create test documents
    docs = [
        {
            "title": f"Document {i}",
            "count": i,
            "tags": ["tag1"] if i % 2 == 0 else ["tag2"],
            "status": "published" if i > 5 else "draft",
        }
        for i in range(10)
    ]
    for doc in docs:
        await db.create_document(user_id, test_schema.name, doc)

    # Test filtering
    query = DocumentQuery(filter={"status": "published"})
    results = await db.get_documents(user_id, test_schema.name, query)
    assert len(results) == 4  # Documents with i > 5

    # Test sorting
    query = DocumentQuery(sort={"count": -1}, limit=3)
    results = await db.get_documents(user_id, test_schema.name, query)
    assert len(results) == 3
    assert [doc["count"] for doc in results] == [9, 8, 7]

    # Test aggregation
    agg_query = AggregationQuery(
        group_by=["status"],
        metrics=[
            AggregateMetric(field="count", function=AggregateFunction.AVERAGE),
            AggregateMetric(field="count", function=AggregateFunction.SUM),
        ],
    )
    results = await db.aggregate_documents(user_id, test_schema.name, agg_query)

    # Convert results to dict for easier assertion
    results_dict = {
        doc["_id"]["status"]: {
            "avg": doc["count_avg"],
            "sum": doc["count_sum"],
        }
        for doc in results
    }

    assert "draft" in results_dict
    assert "published" in results_dict
    assert results_dict["published"]["avg"] > results_dict["draft"]["avg"]


@pytest.mark.asyncio
async def test_schema_validation(db: DocumentDB, user_id: str, test_schema: CollectionSchema):
    """Test schema validation."""
    # Create collection
    await db.create_collection(user_id, test_schema)

    # Test required field
    with pytest.raises(ValueError):
        await db.create_document(user_id, test_schema.name, {"count": 42})

    # Test invalid field type
    with pytest.raises(ValueError):
        await db.create_document(user_id, test_schema.name, {"title": "Test", "count": "not a number"})

    # Test invalid select option
    with pytest.raises(ValueError):
        await db.create_document(
            user_id,
            test_schema.name,
            {"title": "Test", "status": "invalid_status"},
        )

    # Test invalid multi-select option
    with pytest.raises(ValueError):
        await db.create_document(
            user_id,
            test_schema.name,
            {"title": "Test", "tags": ["invalid_tag"]},
        )


@pytest.mark.asyncio
async def test_collection_operations(db: DocumentDB, user_id: str, test_schema: CollectionSchema):
    """Test collection operations."""
    # Create collection
    await db.create_collection(user_id, test_schema)

    # Update schema
    updated_schema = CollectionSchema(
        name=test_schema.name,
        description="Updated description",
        fields=[
            *test_schema.fields,
            FieldDefinition(
                name="new_field",
                description="New field",
                field_type=FieldType.STRING,
                default="default value",
            ),
        ],
    )
    await db.update_collection(user_id, test_schema.name, updated_schema)

    # Verify update
    collections = await db.list_collections(user_id)
    assert len(collections) == 1
    assert len(collections[0].fields) == len(test_schema.fields) + 1

    # Delete collection
    await db.delete_collection(user_id, test_schema.name)

    # Verify deletion
    with pytest.raises(CollectionNotFoundError):
        await db.get_documents(user_id, test_schema.name)
