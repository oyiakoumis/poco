import pytest
from unittest.mock import MagicMock, Mock, patch
from typing import Dict

from langchain_core.embeddings import Embeddings

from database_manager.collection import Collection
from database_manager.collection_registry import CollectionDefinition, CollectionRegistry
from database_manager.connection import Connection
from database_manager.schema_field import FieldType, SchemaField
from database_manager.database import Database


@pytest.fixture
def mock_connection():
    connection = Mock(spec=Connection)
    # Configure the mock client to support dictionary-style access
    mock_client = Mock()
    mock_db = Mock()
    # Configure the mock client to act like a dictionary
    mock_client = type("MockClient", (), {"__getitem__": lambda self, x: mock_db, "__class__": Mock})()
    connection.client = mock_client
    return connection


@pytest.fixture
def mock_embeddings():
    return Mock(spec=Embeddings)


@pytest.fixture
def mock_registry():
    return Mock(spec=CollectionRegistry)


@pytest.fixture
def sample_schema():
    return {
        "title": SchemaField("title", "desc", field_type=FieldType.STRING, required=True),
        "content": SchemaField("content", "desc", field_type=FieldType.STRING, required=True),
    }


@pytest.fixture
def database(mock_connection, mock_embeddings):
    db = Database("test_db", mock_connection, mock_embeddings)
    return db


def test_database_initialization(database):
    assert database.name == "test_db"
    assert isinstance(database.collections, dict)
    assert len(database.collections) == 0
    assert database._mongo_db is None
    assert database.registry is None


def test_database_connect(database, mock_connection):
    # Mock the CollectionRegistry
    with patch("database_manager.database.CollectionRegistry") as mock_registry_class:
        mock_registry_instance = Mock()
        # Make list_collection_definitions return an empty list
        mock_registry_instance.list_collection_definitions.return_value = []
        mock_registry_class.return_value = mock_registry_instance

        database.connect()

        # Verify connection was established
        mock_connection.connect.assert_called_once()

        # Verify database was selected
        assert database._mongo_db == mock_connection.client["test_db"]

        # Verify registry was initialized
        mock_registry_class.assert_called_once_with(database, database.embeddings)
        mock_registry_instance.init_registry.assert_called_once()


def test_load_existing_collections(database, mock_registry, sample_schema):
    database._mongo_db = MagicMock()
    database.registry = mock_registry

    # Create a mock collection definition
    mock_definition = Mock(spec=CollectionDefinition)
    mock_definition.name = "test_collection"
    mock_definition.schema = sample_schema

    # Set up registry to return our mock definition
    mock_registry.list_collection_definitions.return_value = [mock_definition]

    # Load collections
    database._load_existing_collections()

    # Verify collection was created and stored
    assert "test_collection" in database.collections
    assert isinstance(database.collections["test_collection"], Collection)


def test_create_collection(database, mock_registry, sample_schema):
    database._mongo_db = MagicMock()
    database.registry = mock_registry

    # Create a new collection
    collection = database.create_collection("new_collection", sample_schema, "Test collection description")

    # Verify collection definition was registered
    mock_registry.register_collection.assert_called_once()
    args = mock_registry.register_collection.call_args[0][0]
    assert isinstance(args, CollectionDefinition)
    assert args.name == "new_collection"

    # Verify collection was created and stored
    assert "new_collection" in database.collections
    assert isinstance(collection, Collection)
    assert collection.name == "new_collection"


def test_drop_collection(database):
    database._mongo_db = Mock()
    database.registry = Mock()

    # Add a mock collection to drop
    mock_collection = Mock(spec=Collection)
    database.collections["test_collection"] = mock_collection

    # Drop the collection
    database.drop_collection("test_collection")

    # Verify collection was dropped from MongoDB
    database._mongo_db.drop_collection.assert_called_once_with("test_collection")

    # Verify collection was unregistered
    database.registry.unregister_collection.assert_called_once_with("test_collection")

    # Verify collection was removed from local cache
    assert "test_collection" not in database.collections


def test_drop_nonexistent_collection(database):
    database._mongo_db = Mock()
    database.registry = Mock()

    # Try to drop a collection that doesn't exist
    database.drop_collection("nonexistent")

    # Verify no operations were performed
    database._mongo_db.drop_collection.assert_not_called()
    database.registry.unregister_collection.assert_not_called()
