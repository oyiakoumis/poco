import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from bson import ObjectId

from src.database_manager.document import Document  # Update with your actual import path


@pytest.fixture
def mock_collection():
    collection = Mock()
    collection.embeddings = Mock()
    collection.embeddings.embed_query.return_value = [0.1, 0.2, 0.3]
    collection.validate_document = Mock()
    collection._mongo_collection = Mock()
    return collection


@pytest.fixture
def sample_content():
    return {"field1": "value1", "field2": "value2"}


@pytest.fixture
def document(mock_collection, sample_content):
    return Document(content=sample_content, collection=mock_collection)


def test_document_initialization(document, sample_content):
    assert document.content == sample_content
    assert isinstance(document.id, ObjectId)
    assert isinstance(document.created_at, datetime)
    assert isinstance(document.updated_at, datetime)


def test_embedding_property(document, mock_collection):
    embedding = document.embedding
    assert embedding == [0.1, 0.2, 0.3]
    mock_collection.embeddings.embed_query.assert_called_once()


def test_to_dict(document):
    result = document.to_dict()
    assert isinstance(result, dict)
    assert result["content"] == document.content
    assert result["_id"] == document.id
    assert result["_created_at"] == document.created_at
    assert result["_updated_at"] == document.updated_at
    assert result["_embedding"] == document.embedding


def test_from_dict(mock_collection):
    content = {"field1": "value1"}
    object_id = ObjectId()
    created_at = datetime.now()
    updated_at = datetime.now()
    embedding = [0.1, 0.2, 0.3]

    data = {"content": content, "_id": object_id, "_created_at": created_at, "_updated_at": updated_at, "_embedding": embedding}

    document = Document.from_dict(data, mock_collection)

    assert document.content == content
    assert document.id == object_id
    assert document.created_at == created_at
    assert document.updated_at == updated_at
    assert document.embedding == embedding
    assert document.collection == mock_collection


def test_update_success(document, mock_collection):
    mock_collection._mongo_collection.update_one.return_value = Mock(modified_count=1)
    original_updated_at = document.updated_at

    result = document.update()

    assert result is True
    assert document.updated_at > original_updated_at
    mock_collection.validate_document.assert_called_once_with(document.content)
    mock_collection._mongo_collection.update_one.assert_called_once()


def test_update_failure(document, mock_collection):
    mock_collection._mongo_collection.update_one.return_value = Mock(modified_count=0)
    original_updated_at = document.updated_at

    result = document.update()

    assert result is False
    assert document.updated_at == original_updated_at
    mock_collection.validate_document.assert_called_once_with(document.content)
    mock_collection._mongo_collection.update_one.assert_called_once()


def test_delete_success(document, mock_collection):
    mock_collection._mongo_collection.delete_one.return_value = Mock(deleted_count=1)

    result = document.delete()

    assert result is True
    mock_collection._mongo_collection.delete_one.assert_called_once_with({"_id": document.id})


def test_delete_failure(document, mock_collection):
    mock_collection._mongo_collection.delete_one.return_value = Mock(deleted_count=0)

    result = document.delete()

    assert result is False
    mock_collection._mongo_collection.delete_one.assert_called_once_with({"_id": document.id})


def test_get_content_for_embedding(document, sample_content):
    result = document.get_content_for_embedding()
    assert isinstance(result, str)
    assert '"field1": "value1"' in result
    assert '"field2": "value2"' in result


def test_generate_embedding(document, mock_collection):
    content = document.get_content_for_embedding()
    embedding = document.generate_embedding()

    assert embedding == [0.1, 0.2, 0.3]
    mock_collection.embeddings.embed_query.assert_called_with(content)


# Edge cases and error handling tests
def test_update_with_invalid_content(document, mock_collection):
    mock_collection.validate_document.side_effect = ValueError("Invalid content")

    with pytest.raises(ValueError):
        document.update()


@pytest.mark.parametrize(
    "invalid_data",
    [
        {},
        {"content": {}},
        {"content": {}, "_id": ObjectId()},
        {"content": {}, "_id": ObjectId(), "_created_at": datetime.now()},
    ],
)
def test_from_dict_with_invalid_data(invalid_data, mock_collection):
    with pytest.raises(KeyError):
        Document.from_dict(invalid_data, mock_collection)
