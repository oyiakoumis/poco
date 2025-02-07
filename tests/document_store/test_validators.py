"""Tests for the document store validators."""

import pytest

from document_store.exceptions import InvalidFieldValueError, InvalidRecordDataError
from document_store.types import Field, FieldType
from document_store.validators import validate_query_fields, validate_record_data


@pytest.fixture
def sample_structure():
    """Sample dataset structure for testing."""
    return [
        Field(field_name="age", description="User age", type=FieldType.INTEGER, required=True),
        Field(field_name="height", description="User height in meters", type=FieldType.FLOAT, required=False),
        Field(field_name="name", description="User name", type=FieldType.STRING, required=True),
        Field(field_name="nickname", description="User nickname", type=FieldType.STRING, required=False, default="anonymous"),
    ]


def test_validate_record_data_valid(sample_structure):
    """Test validation of valid record data."""
    data = {"age": 25, "height": 1.75, "name": "John Doe"}
    validated = validate_record_data(data, sample_structure)
    assert validated["age"] == 25
    assert validated["height"] == 1.75
    assert validated["name"] == "John Doe"
    assert validated["nickname"] == "anonymous"  # Default value


def test_validate_record_data_type_conversion(sample_structure):
    """Test type conversion during validation."""
    data = {
        "age": "25",  # String that can be converted to int
        "height": "1.75",  # String that can be converted to float
        "name": 123,  # Number that can be converted to string
    }
    validated = validate_record_data(data, sample_structure)
    assert isinstance(validated["age"], int)
    assert isinstance(validated["height"], float)
    assert isinstance(validated["name"], str)
    assert validated["age"] == 25
    assert validated["height"] == 1.75
    assert validated["name"] == "123"


def test_validate_record_data_missing_required(sample_structure):
    """Test validation fails with missing required field."""
    data = {
        "age": 25,
        # Missing required 'name' field
    }
    with pytest.raises(InvalidRecordDataError) as exc:
        validate_record_data(data, sample_structure)
    assert "Required field 'name' is missing" in str(exc.value)


def test_validate_record_data_unknown_field(sample_structure):
    """Test validation fails with unknown field."""
    data = {"age": 25, "name": "John", "unknown_field": "value"}
    with pytest.raises(InvalidRecordDataError) as exc:
        validate_record_data(data, sample_structure)
    assert "Unknown fields in record data: unknown_field" in str(exc.value)


def test_validate_record_data_invalid_type(sample_structure):
    """Test validation fails with invalid field type."""
    data = {"age": "not_a_number", "name": "John"}
    with pytest.raises(InvalidFieldValueError) as exc:
        validate_record_data(data, sample_structure)
    assert "Invalid value for field 'age'" in str(exc.value)


def test_validate_record_data_optional_fields(sample_structure):
    """Test validation with optional fields omitted."""
    data = {"age": 25, "name": "John"}
    validated = validate_record_data(data, sample_structure)
    assert "height" not in validated
    assert validated["nickname"] == "anonymous"  # Default value


def test_validate_record_data_null_optional(sample_structure):
    """Test validation with null values for optional fields."""
    data = {"age": 25, "name": "John", "height": None}
    validated = validate_record_data(data, sample_structure)
    assert "height" not in validated


def test_validate_query_fields_valid(sample_structure):
    """Test validation of valid query fields."""
    query = {"age": 25, "name": "John"}
    # Should not raise any exception
    validate_query_fields(query, sample_structure)


def test_validate_query_fields_unknown(sample_structure):
    """Test validation fails with unknown query fields."""
    query = {"age": 25, "unknown_field": "value"}
    with pytest.raises(InvalidRecordDataError) as exc:
        validate_query_fields(query, sample_structure)
    assert "Query contains unknown fields: unknown_field" in str(exc.value)


def test_validate_query_fields_empty(sample_structure):
    """Test validation with empty query."""
    query = {}
    # Should not raise any exception
    validate_query_fields(query, sample_structure)
