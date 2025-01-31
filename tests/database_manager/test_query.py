import pytest
from typing import List, Any
from unittest.mock import Mock, call

from database_manager.query import Query


# Mock classes for testing
class MockDocument:
    def __init__(self, data: dict):
        self.data = data


class MockCollection:
    def __init__(self):
        self.documents: List[MockDocument] = []
        self._execute_query = Mock()

    def add_document(self, doc: MockDocument):
        self.documents.append(doc)


@pytest.fixture
def collection():
    return MockCollection()


@pytest.fixture
def query(collection):
    return Query(collection)


def test_query_initialization(collection):
    """Test that Query is properly initialized with empty filters and sort fields"""
    query = Query(collection)

    assert query.collection == collection
    assert query.filters == {}
    assert query.sort_fields == []
    assert query.limit_val is None


def test_filter_single_condition(query):
    """Test adding a single filter condition"""
    result = query.filter({"name": "John"})

    assert result == query  # Test method chaining
    assert query.filters == {"name": "John"}


def test_filter_multiple_conditions(query):
    """Test adding multiple filter conditions"""
    query.filter({"name": "John"})
    result = query.filter({"age": 30})

    assert result == query
    assert query.filters == {"name": "John", "age": 30}


def test_filter_update_existing_condition(query):
    """Test updating an existing filter condition"""
    query.filter({"name": "John"})
    result = query.filter({"name": "Jane"})

    assert result == query
    assert query.filters == {"name": "Jane"}


def test_sort_single_field(query):
    """Test adding a single sort field"""
    result = query.sort("name")

    assert result == query
    assert query.sort_fields == [("name", 1)]


def test_sort_multiple_fields(query):
    """Test adding multiple sort fields"""
    query.sort("name")
    result = query.sort("age", ascending=False)

    assert result == query
    assert query.sort_fields == [("name", 1), ("age", -1)]


def test_sort_descending(query):
    """Test adding a descending sort field"""
    result = query.sort("name", ascending=False)

    assert result == query
    assert query.sort_fields == [("name", -1)]


def test_limit(query):
    """Test setting a limit on the query"""
    result = query.limit(10)

    assert result == query
    assert query.limit_val == 10


def test_execute(query):
    """Test query execution"""
    expected_documents = [MockDocument({"name": "John"}), MockDocument({"name": "Jane"})]
    query.collection._execute_query.return_value = expected_documents

    result = query.execute()

    assert result == expected_documents
    query.collection._execute_query.assert_called_once_with(query)


def test_method_chaining(query):
    """Test that all methods can be chained together"""
    expected_documents = [MockDocument({"name": "John"})]
    query.collection._execute_query.return_value = expected_documents

    result = query.filter({"name": "John"}).sort("age", ascending=False).limit(5).execute()

    assert result == expected_documents
    assert query.filters == {"name": "John"}
    assert query.sort_fields == [("age", -1)]
    assert query.limit_val == 5
    query.collection._execute_query.assert_called_once_with(query)


def test_empty_query_execution(query):
    """Test executing a query with no conditions"""
    expected_documents = []
    query.collection._execute_query.return_value = expected_documents

    result = query.execute()

    assert result == expected_documents
    query.collection._execute_query.assert_called_once_with(query)


def test_filter_with_complex_conditions(query):
    """Test filtering with more complex data types"""
    complex_conditions = {"age": {"$gt": 25}, "tags": ["python", "testing"], "active": True, "score": 4.5}

    result = query.filter(complex_conditions)

    assert result == query
    assert query.filters == complex_conditions
