import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock
from pymongo.collection import Collection
from pymongo.database import Database as MongoDatabase
from pymongo.operations import SearchIndexModel

from database_manager.collection_definition import CollectionDefinition
from database_manager.database import Database
from database_manager.collection_registry import CollectionRegistry
from database_manager.schema_field import DataType, SchemaField


@pytest.fixture
def mock_embeddings():
    embeddings = Mock()
    embeddings.embed_query = MagicMock(return_value=[0.1] * 1536)
    return embeddings


@pytest.fixture
def mock_mongo_collection():
    collection = Mock(spec=Collection)
    collection.find_one = MagicMock()
    collection.find = MagicMock()
    collection.create_index = MagicMock()
    collection.create_search_index = MagicMock()
    collection.insert_one = MagicMock()
    collection.update_one = MagicMock()
    collection.delete_one = MagicMock()
    collection.aggregate = MagicMock()
    return collection


@pytest.fixture
def mock_mongo_db(mock_mongo_collection):
    db = MagicMock(spec=MongoDatabase)
    db.list_collection_names.return_value = []
    db.__getitem__.side_effect = lambda x: mock_mongo_collection
    return db


@pytest.fixture
def mock_database(mock_mongo_db):
    database = Mock(spec=Database)
    database._mongo_db = mock_mongo_db
    return database


@pytest.fixture
def collection_registry(mock_database, mock_embeddings):
    return CollectionRegistry(mock_database, mock_embeddings)


@pytest.fixture
def sample_collection_definition(collection_registry):
    return CollectionDefinition(name="test_collection", description="Test collection", schema={"type": SchemaField("type", "desc", DataType.STRING)}, collection_registry=collection_registry)


def test_init_registry_creates_indexes(collection_registry, mock_mongo_collection):
    collection_registry.init_registry()

    mock_mongo_collection.create_index.assert_called_once_with("name", unique=True)
    mock_mongo_collection.create_search_index.assert_called_once()

    # Verify search index creation arguments
    call_args = mock_mongo_collection.create_search_index.call_args
    search_index_model = call_args[0][0]
    assert isinstance(search_index_model, SearchIndexModel)
    assert search_index_model.document["name"] == collection_registry.EMBEDDING_INDEX_NAME


def test_register_collection(collection_registry, sample_collection_definition, mock_mongo_collection):
    collection_registry.register_collection(sample_collection_definition)

    mock_mongo_collection.insert_one.assert_called_once_with(sample_collection_definition.to_dict())


def test_get_collection_definition_found(collection_registry, sample_collection_definition, mock_mongo_collection):
    mock_mongo_collection.find_one.return_value = sample_collection_definition.to_dict()

    result = collection_registry.get_collection_definition("test_collection")

    assert result is not None
    assert result.name == sample_collection_definition.name
    mock_mongo_collection.find_one.assert_called_once_with({"name": "test_collection"})


def test_get_collection_definition_not_found(collection_registry, mock_mongo_collection):
    mock_mongo_collection.find_one.return_value = None

    result = collection_registry.get_collection_definition("nonexistent")

    assert result is None
    mock_mongo_collection.find_one.assert_called_once_with({"name": "nonexistent"})


def test_list_collection_definitions(collection_registry, sample_collection_definition, mock_mongo_collection):
    mock_mongo_collection.find.return_value = [sample_collection_definition.to_dict()]

    results = collection_registry.list_collection_definitions()

    assert len(results) == 1
    assert results[0].name == sample_collection_definition.name
    mock_mongo_collection.find.assert_called_once_with()


def test_search_similar_collections(collection_registry, sample_collection_definition, mock_mongo_collection):
    mock_mongo_collection.aggregate.return_value = [sample_collection_definition.to_dict()]

    results = collection_registry.search_similar_collections(sample_collection_definition, num_results=5, min_score=0.0)

    assert len(results) == 1
    assert results[0].name == sample_collection_definition.name

    # Verify aggregate pipeline
    call_args = mock_mongo_collection.aggregate.call_args
    pipeline = call_args[0][0]
    assert len(pipeline) == 4
    assert "$vectorSearch" in pipeline[0]
    assert pipeline[0]["$vectorSearch"]["limit"] == 5


def test_update_collection_definition(collection_registry, sample_collection_definition, mock_mongo_collection):
    collection_registry.update_collection_definition(sample_collection_definition)

    mock_mongo_collection.update_one.assert_called_once()
    call_args = mock_mongo_collection.update_one.call_args
    assert call_args[0][0] == {"name": sample_collection_definition.name}
    assert isinstance(call_args[0][1]["$set"]["_updated_at"], datetime)


def test_unregister_collection(collection_registry, mock_mongo_collection):
    collection_registry.unregister_collection("test_collection")

    mock_mongo_collection.delete_one.assert_called_once_with({"name": "test_collection"})
