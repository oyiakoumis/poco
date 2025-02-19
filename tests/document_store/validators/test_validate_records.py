"""Tests for the document store validators."""

import pytest

from document_store.exceptions import InvalidFieldValueError, InvalidRecordDataError
from document_store.models.record import validate_query_fields
from document_store.types import FieldType, SchemaField
from document_store.type_validators import validate_record_data


@pytest.fixture
def sample_schema():
    """Sample dataset schema for testing."""
    return [
        SchemaField(field_name="age", description="User age", type=FieldType.INTEGER, required=True),
        SchemaField(field_name="height", description="User height in meters", type=FieldType.FLOAT, required=False),
        SchemaField(field_name="name", description="User name", type=FieldType.STRING, required=True),
        SchemaField(field_name="nickname", description="User nickname", type=FieldType.STRING, required=False, default="anonymous"),
        SchemaField(field_name="status", description="User status", type=FieldType.SELECT, required=True, options=["active", "inactive", "pending"]),
        SchemaField(
            field_name="roles", description="User roles", type=FieldType.MULTI_SELECT, required=False, options=["admin", "user", "moderator"], default=["user"]
        ),
    ]


def test_validate_record_data_valid(sample_schema):
    """Test validation of valid record data."""
    data = {"age": 25, "height": 1.75, "name": "John Doe", "status": "active", "roles": ["user", "moderator"]}
    validated = validate_record_data(data, sample_schema)
    assert validated["age"] == 25
    assert validated["height"] == 1.75
    assert validated["name"] == "John Doe"
    assert validated["nickname"] == "anonymous"  # Default value for string
    assert validated["status"] == "active"
    assert validated["roles"] == ["moderator", "user"]  # Note: sorted order


def test_validate_record_data_type_conversion(sample_schema):
    """Test type conversion during validation."""
    data = {
        "age": "25",  # String that can be converted to int
        "height": "1.75",  # String that can be converted to float
        "name": 123,  # Number that can be converted to string
        "status": "active",
        "roles": "admin,user",  # String that can be converted to list
    }
    validated = validate_record_data(data, sample_schema)
    assert isinstance(validated["age"], int)
    assert isinstance(validated["height"], float)
    assert isinstance(validated["name"], str)
    assert validated["age"] == 25
    assert validated["height"] == 1.75
    assert validated["name"] == "123"


def test_validate_record_data_missing_required(sample_schema):
    """Test validation fails with missing required field."""
    data = {
        "age": 25,
        # Missing required 'name' field
    }
    with pytest.raises(InvalidRecordDataError) as exc:
        validate_record_data(data, sample_schema)
    assert "Required field 'name' is missing" in str(exc.value)


def test_validate_record_data_unknown_field(sample_schema):
    """Test validation fails with unknown field."""
    data = {"age": 25, "name": "John", "unknown_field": "value"}
    with pytest.raises(InvalidRecordDataError) as exc:
        validate_record_data(data, sample_schema)
    assert "Unknown fields in record data: unknown_field" in str(exc.value)


def test_validate_record_data_invalid_type(sample_schema):
    """Test validation fails with invalid field type."""
    data = {"age": "not_a_number", "name": "John"}
    with pytest.raises(InvalidFieldValueError) as exc:
        validate_record_data(data, sample_schema)
    assert "Invalid value for field 'age'" in str(exc.value)


def test_validate_record_data_optional_fields(sample_schema):
    """Test validation with optional fields omitted."""
    data = {"age": 25, "name": "John", "status": "active"}
    validated = validate_record_data(data, sample_schema)
    assert "height" not in validated
    assert validated["nickname"] == "anonymous"  # Default value for string
    assert validated["roles"] == ["user"]  # Default value for multi-select


def test_validate_record_data_null_optional(sample_schema):
    """Test validation with null values for optional fields."""
    data = {"age": 25, "name": "John", "status": "active", "height": None, "roles": None}
    validated = validate_record_data(data, sample_schema)
    assert "height" not in validated


def test_validate_query_fields_valid(sample_schema):
    """Test validation of valid query fields."""
    query = {"age": 25, "name": "John", "status": "active", "roles": ["admin"]}
    # Should not raise any exception
    validate_query_fields(query, sample_schema)


def test_validate_query_fields_unknown(sample_schema):
    """Test validation fails with unknown query fields."""
    query = {"age": 25, "unknown_field": "value", "invalid_status": "active"}
    with pytest.raises(InvalidRecordDataError) as exc:
        validate_query_fields(query, sample_schema)
    assert "Query contains unknown fields: " in str(exc.value)
    assert "unknown_field" in str(exc.value)
    assert "invalid_status" in str(exc.value)


def test_validate_query_fields_empty(sample_schema):
    """Test validation with empty query."""
    query = {}
    # Should not raise any exception
    validate_query_fields(query, sample_schema)


def test_validate_record_data_invalid_select(sample_schema):
    """Test validation fails with invalid select value."""
    data = {"age": 25, "name": "John", "status": "invalid_status"}  # Invalid select value
    with pytest.raises(InvalidFieldValueError) as exc:
        validate_record_data(data, sample_schema)
    assert "Invalid value for field 'status'" in str(exc.value)
    assert "Value must be one of: active, inactive, pending" in str(exc.value)


def test_validate_record_data_invalid_multi_select(sample_schema):
    """Test validation fails with invalid multi-select value."""
    data = {"age": 25, "name": "John", "status": "active", "roles": ["invalid_role", "admin"]}  # One invalid role
    with pytest.raises(InvalidFieldValueError) as exc:
        validate_record_data(data, sample_schema)
    assert "Invalid value for field 'roles'" in str(exc.value)
    assert "Invalid options: invalid_role" in str(exc.value)


def test_validate_record_data_missing_options(sample_schema):
    """Test validation fails when options not provided for select/multi-select."""
    # Create schema with select field but no options
    bad_schema = [SchemaField(field_name="bad_select", description="Bad select field", type=FieldType.SELECT, required=True)]
    data = {"bad_select": "any"}
    with pytest.raises(InvalidFieldValueError) as exc:
        validate_record_data(data, bad_schema)
    assert "Options not provided for FieldType.SELECT field 'bad_select'" in str(exc.value)
