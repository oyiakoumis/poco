import pytest
from unittest.mock import MagicMock, Mock, patch
from pymongo.collection import Collection as MongoCollection
from pymongo.operations import SearchIndexModel
from pymongo.cursor import Cursor
from bson import ObjectId

from database_manager.collection import Collection
from database_manager.document import Document
from database_manager.exceptions import ValidationError
from database_manager.query import Query
from database_manager.schema_field import DataType, SchemaField


@pytest.fixture
def mock_database():
    database = Mock()
    mock_collection = Mock(spec=MongoCollection)
    # Create a dict-like mock for _mongo_db
    mock_mongo_db = Mock()
    mock_mongo_db.list_collection_names.return_value = []
    mock_mongo_db.__getitem__ = Mock(return_value=mock_collection)
    database._mongo_db = mock_mongo_db
    return database


@pytest.fixture
def mock_embeddings():
    embeddings = Mock()
    embeddings.embed_query.return_value = [0.1] * Collection.EMBEDDING_DIMENSION
    return embeddings


@pytest.fixture
def sample_schema():
    return {
        "name": SchemaField("name", "Name field", DataType.STRING, required=True),
        "age": SchemaField("age", "Age field", DataType.INTEGER, required=False),
    }


@pytest.fixture
def collection(mock_database, mock_embeddings, sample_schema):
    return Collection(name="test_collection", database=mock_database, embeddings=mock_embeddings, schema=sample_schema)


def test_init(collection, mock_database, mock_embeddings, sample_schema):
    assert collection.name == "test_collection"
    assert collection.database == mock_database
    assert collection.embeddings == mock_embeddings
    assert collection.schema == sample_schema


def test_create_collection(collection, mock_database):
    collection.create_collection()

    # Verify index creation
    collection._mongo_collection.create_index.assert_called_once_with("name", unique=True)

    # Verify search index creation
    collection._mongo_collection.create_search_index.assert_called_once()
    args = collection._mongo_collection.create_search_index.call_args[0][0]
    assert isinstance(args, SearchIndexModel)
    assert args.document["name"] == Collection.EMBEDDING_INDEX_NAME


def test_insert_one_valid_document(collection):
    content = {"name": "John Doe", "age": 30}
    inserted_id = ObjectId()
    mock_result = Mock()
    mock_result.inserted_id = inserted_id
    collection._mongo_collection.insert_one.return_value = mock_result

    document = collection.insert_one(content)

    assert isinstance(document, Document)
    assert document.content == content
    assert document.id == inserted_id
    collection._mongo_collection.insert_one.assert_called_once()


def test_insert_one_invalid_document(collection):
    content = {"age": 30}  # Missing required 'name' field

    with pytest.raises(ValidationError):
        collection.insert_one(content)


def test_find_one_existing(collection):
    mock_data = {
        "_id": ObjectId(),
        "content": {"name": "John"},
        "_created_at": "2024-01-01",
        "_updated_at": "2024-01-01",
        "_embedding": [0.1] * Collection.EMBEDDING_DIMENSION,
    }
    collection._mongo_collection.find_one.return_value = mock_data

    document = collection.find_one({"name": "John"})

    assert isinstance(document, Document)
    assert document.content == mock_data["content"]
    collection._mongo_collection.find_one.assert_called_once_with({"name": "John"})


def test_find_one_non_existing(collection):
    collection._mongo_collection.find_one.return_value = None

    document = collection.find_one({"name": "NonExistent"})

    assert document is None
    collection._mongo_collection.find_one.assert_called_once_with({"name": "NonExistent"})


def test_find_without_filter(collection):
    query = collection.find()
    assert query.collection == collection
    assert query.filters == {}


def test_find_with_filter(collection):
    filter_dict = {"name": "John"}
    query = collection.find(filter_dict)
    assert query.collection == collection
    assert query.filters == filter_dict


def test_search_similar(collection):
    # Mock document with embedding
    mock_document = Mock(spec=Document)
    mock_document.embedding = [0.1] * Collection.EMBEDDING_DIMENSION

    # Mock aggregate results
    mock_results = [
        {
            "_id": ObjectId(),
            "content": {"name": "Similar1"},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.2] * Collection.EMBEDDING_DIMENSION,
        },
        {
            "_id": ObjectId(),
            "content": {"name": "Similar2"},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.3] * Collection.EMBEDDING_DIMENSION,
        },
    ]
    collection._mongo_collection.aggregate.return_value = mock_results

    results = collection.search_similar(mock_document, num_results=2, min_score=0.5)

    assert len(results) == 2
    assert all(isinstance(doc, Document) for doc in results)
    collection._mongo_collection.aggregate.assert_called_once()


def test_validate_document_valid(collection):
    valid_doc = {"name": "John Doe", "age": 30}
    collection.validate_document(valid_doc)  # Should not raise any exception


def test_validate_document_missing_required(collection):
    invalid_doc = {"age": 30}  # Missing required 'name' field
    with pytest.raises(ValidationError, match="Required field name is missing"):
        collection.validate_document(invalid_doc)


def test_execute_query_basic_filter(collection):
    # Setup test data
    test_docs = [
        {
            "_id": ObjectId(),
            "content": {"name": "Alice", "age": 30},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.1] * Collection.EMBEDDING_DIMENSION,
        },
        {
            "_id": ObjectId(),
            "content": {"name": "Bob", "age": 25},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.1] * Collection.EMBEDDING_DIMENSION,
        },
    ]

    # Create mock cursor
    mock_cursor = MagicMock(spec=Cursor)
    mock_cursor.__iter__.return_value = iter(test_docs)

    # Setup collection mock
    collection._mongo_collection.find.return_value = mock_cursor

    # Create query
    query = Query(collection)
    query.filter({"age": {"$gt": 20}})

    # Execute query
    results = collection._execute_query(query)

    # Assertions
    assert len(results) == 2
    assert isinstance(results[0], Document)
    assert results[0].content["name"] == "Alice"
    assert results[1].content["name"] == "Bob"

    # Verify the find was called with correct parameters
    collection._mongo_collection.find.assert_called_once_with({"age": {"$gt": 20}})


def test_execute_query_with_sort(collection):
    # Setup test data
    test_docs = [
        {
            "_id": ObjectId(),
            "content": {"name": "Alice", "age": 30},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.1] * Collection.EMBEDDING_DIMENSION,
        },
        {
            "_id": ObjectId(),
            "content": {"name": "Bob", "age": 25},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.1] * Collection.EMBEDDING_DIMENSION,
        },
    ]

    # Create mock cursor
    mock_cursor = MagicMock(spec=Cursor)
    mock_cursor.__iter__.return_value = iter(test_docs)
    mock_cursor.sort.return_value = mock_cursor

    # Setup collection mock
    collection._mongo_collection.find.return_value = mock_cursor

    # Create query with sort
    query = Query(collection)
    query.sort("age", ascending=False)

    # Execute query
    results = collection._execute_query(query)

    # Assertions
    assert len(results) == 2
    mock_cursor.sort.assert_called_once_with([("age", -1)])


def test_execute_query_with_limit(collection):
    # Setup test data
    test_docs = [
        {
            "_id": ObjectId(),
            "content": {"name": "Alice", "age": 30},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.1] * Collection.EMBEDDING_DIMENSION,
        }
    ]

    # Create mock cursor
    mock_cursor = MagicMock(spec=Cursor)
    mock_cursor.__iter__.return_value = iter(test_docs)
    mock_cursor.limit.return_value = mock_cursor

    # Setup collection mock
    collection._mongo_collection.find.return_value = mock_cursor

    # Create query with limit
    query = Query(collection)
    query.limit(1)

    # Execute query
    results = collection._execute_query(query)

    # Assertions
    assert len(results) == 1
    mock_cursor.limit.assert_called_once_with(1)


def test_execute_query_empty_result(collection):
    # Create mock cursor with no results
    mock_cursor = MagicMock(spec=Cursor)
    mock_cursor.__iter__.return_value = iter([])

    # Setup collection mock
    collection._mongo_collection.find.return_value = mock_cursor

    # Create query
    query = Query(collection)

    # Execute query
    results = collection._execute_query(query)

    # Assertions
    assert len(results) == 0
    assert isinstance(results, list)


def test_execute_query_combined_operations(collection):
    # Setup test data
    test_docs = [
        {
            "_id": ObjectId(),
            "content": {"name": "Alice", "age": 30},
            "_created_at": "2024-01-01",
            "_updated_at": "2024-01-01",
            "_embedding": [0.1] * Collection.EMBEDDING_DIMENSION,
        }
    ]

    # Create mock cursor
    mock_cursor = MagicMock(spec=Cursor)
    mock_cursor.__iter__.return_value = iter(test_docs)
    mock_cursor.sort.return_value = mock_cursor
    mock_cursor.limit.return_value = mock_cursor

    # Setup collection mock
    collection._mongo_collection.find.return_value = mock_cursor

    # Create query with multiple operations
    query = Query(collection)
    query.filter({"age": {"$gt": 25}})
    query.sort("age", ascending=False)
    query.limit(1)

    # Execute query
    results = collection._execute_query(query)

    # Assertions
    assert len(results) == 1
    collection._mongo_collection.find.assert_called_once_with({"age": {"$gt": 25}})
    mock_cursor.sort.assert_called_once_with([("age", -1)])
    mock_cursor.limit.assert_called_once_with(1)
    assert results[0].content["name"] == "Alice"
