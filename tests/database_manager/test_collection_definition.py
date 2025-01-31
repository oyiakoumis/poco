import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from database_manager.schema_field import DataType, SchemaField
from database_manager.collection_definition import CollectionDefinition


@pytest.fixture
def mock_collection_registry():
    registry = Mock()
    # Mock the embeddings functionality
    registry.embeddings.embed_query.return_value = [0.1, 0.2, 0.3]
    return registry


@pytest.fixture
def sample_schema():
    return {
        "field1": SchemaField(name="field1", description="Test field 1", field_type=DataType.STRING, required=True, default=None),
        "field2": SchemaField(name="field2", description="Test field 2", field_type=DataType.INTEGER, required=False, default=0),
    }


@pytest.fixture
def collection_definition(mock_collection_registry, sample_schema):
    return CollectionDefinition(name="test_collection", collection_registry=mock_collection_registry, description="Test collection", schema=sample_schema)


def test_init(collection_definition):
    assert collection_definition.name == "test_collection"
    assert collection_definition.description == "Test collection"
    assert isinstance(collection_definition.created_at, datetime)
    assert isinstance(collection_definition.updated_at, datetime)
    assert len(collection_definition.schema) == 2


def test_embedding_property(collection_definition, mock_collection_registry):
    embedding = collection_definition.embedding
    assert isinstance(embedding, list)
    assert len(embedding) == 3
    mock_collection_registry.embeddings.embed_query.assert_called_once()


def test_get_content_for_embedding(collection_definition):
    content = collection_definition.get_content_for_embedding()
    content_dict = json.loads(content)

    assert content_dict["name"] == "test_collection"
    assert content_dict["description"] == "Test collection"
    assert "schema" in content_dict
    assert len(content_dict["schema"]) == 2
    assert content_dict["schema"]["field1"]["field_type"] == DataType.STRING.value
    assert content_dict["schema"]["field2"]["field_type"] == DataType.INTEGER.value


def test_generate_embedding(collection_definition, mock_collection_registry):
    embedding = collection_definition.generate_embedding()
    assert embedding == [0.1, 0.2, 0.3]
    mock_collection_registry.embeddings.embed_query.assert_called_once()


def test_to_dict(collection_definition):
    result = collection_definition.to_dict()

    assert result["name"] == "test_collection"
    assert result["description"] == "Test collection"
    assert isinstance(result["schema"], dict)
    assert isinstance(result["_created_at"], datetime)
    assert isinstance(result["_updated_at"], datetime)
    assert result[CollectionDefinition.EMBEDDING_FIELD_NAME] == [0.1, 0.2, 0.3]


def test_from_dict(mock_collection_registry):
    test_data = {
        "name": "test_collection",
        "description": "Test collection",
        "schema": {"field1": {"name": "field1", "description": "Test field 1", "field_type": "string", "required": True, "default": None}},
        "_created_at": datetime.now(),
        "_updated_at": datetime.now(),
        "_embedding": [0.1, 0.2, 0.3],
    }

    collection = CollectionDefinition.from_dict(test_data, mock_collection_registry)

    assert isinstance(collection, CollectionDefinition)
    assert collection.name == test_data["name"]
    assert collection.description == test_data["description"]
    assert len(collection.schema) == 1
    assert isinstance(collection.schema["field1"], SchemaField)
    assert collection.schema["field1"].field_type == DataType.STRING
    assert collection.created_at == test_data["_created_at"]
    assert collection.updated_at == test_data["_updated_at"]


def test_from_dict_with_invalid_data(mock_collection_registry):
    invalid_data = {
        "name": "test_collection",
        # Missing required fields
        "schema": {},
    }

    with pytest.raises(KeyError):
        CollectionDefinition.from_dict(invalid_data, mock_collection_registry)


@pytest.mark.parametrize(
    "field_type,expected",
    [
        (DataType.STRING, "string"),
        (DataType.INTEGER, "integer"),
        (DataType.FLOAT, "float"),
        (DataType.BOOLEAN, "boolean"),
    ],
)
def test_schema_field_types(mock_collection_registry, field_type, expected):
    schema = {"test_field": SchemaField(name="test_field", description="Test field", field_type=field_type, required=True, default=None)}

    collection = CollectionDefinition(name="test_collection", collection_registry=mock_collection_registry, description="Test collection", schema=schema)

    content = collection.get_content_for_embedding()
    content_dict = json.loads(content)
    assert content_dict["schema"]["test_field"]["field_type"] == expected
