"""Tests for schema validation and schema updates."""

from datetime import date, datetime

import pytest

from document_store.exceptions import (
    InvalidDatasetSchemaError,
    InvalidRecordDataError,
)
from document_store.models import Dataset, validate_schema, validate_field_update
from document_store.types import FieldType, SchemaField


def test_validate_schema():
    """Test basic schema validation."""
    schema = [
        SchemaField(
            field_name="age",
            description="User age",
            type=FieldType.INTEGER,
            required=True,
        ),
        SchemaField(
            field_name="name",
            description="User name",
            type=FieldType.STRING,
            required=True,
        ),
    ]

    # Should not raise any exceptions
    validated = validate_schema(schema)
    assert len(validated) == 2
    assert validated[0].field_name == "age"
    assert validated[1].field_name == "name"


def test_validate_schema_duplicate_fields():
    """Test schema validation with duplicate field names."""
    schema = [
        SchemaField(field_name="name", description="First name", type=FieldType.STRING),
        SchemaField(field_name="name", description="Last name", type=FieldType.STRING),
    ]

    with pytest.raises(InvalidDatasetSchemaError) as exc:
        validate_schema(schema)
    assert "Duplicate field names" in str(exc.value)


def test_validate_schema_select_options():
    """Test schema validation for select fields."""
    # Test missing options
    schema = [
        SchemaField(
            field_name="status",
            description="User status",
            type=FieldType.SELECT,
            required=True,
        )
    ]

    with pytest.raises(InvalidDatasetSchemaError) as exc:
        validate_schema(schema)
    assert "Options not provided" in str(exc.value)

    # Test valid options
    schema = [
        SchemaField(
            field_name="status",
            description="User status",
            type=FieldType.SELECT,
            required=True,
            options=["active", "inactive"],
        )
    ]

    # Should not raise any exceptions
    validated = validate_schema(schema)
    assert validated[0].options == ["active", "inactive"]


def test_validate_schema_default_values():
    """Test schema validation with default values."""
    schema = [
        SchemaField(
            field_name="age",
            description="User age",
            type=FieldType.INTEGER,
            default="25",  # String that can be converted to int
        ),
        SchemaField(
            field_name="height",
            description="User height",
            type=FieldType.FLOAT,
            default=175,  # Int that can be converted to float
        ),
    ]

    validated = validate_schema(schema)
    assert validated[0].default == 25  # Converted to int
    assert validated[1].default == 175.0  # Converted to float

    # Test invalid default value
    schema = [
        SchemaField(
            field_name="age",
            description="User age",
            type=FieldType.INTEGER,
            default="not a number",
        )
    ]

    with pytest.raises(InvalidDatasetSchemaError) as exc:
        validate_schema(schema)
    assert "Invalid default value" in str(exc.value)


def test_validate_field_update():
    """Test field update validation."""
    # Create a test dataset
    dataset = Dataset(
        user_id="test_user",
        name="test_dataset",
        description="Test dataset",
        dataset_schema=[
            SchemaField(
                field_name="age",
                description="User age",
                type=FieldType.INTEGER,
            ),
            SchemaField(
                field_name="name",
                description="User name",
                type=FieldType.STRING,
            ),
        ],
    )

    # Test valid update (same type)
    field_update = SchemaField(
        field_name="age",
        description="Updated age description",
        type=FieldType.INTEGER,
    )
    old_field, new_schema = validate_field_update(dataset, "age", field_update)
    assert old_field is not None
    assert old_field.type == FieldType.INTEGER
    assert len(new_schema) == 2
    assert new_schema[0].description == "Updated age description"

    # Test valid type conversion (integer to float)
    field_update = SchemaField(
        field_name="age",
        description="Age as float",
        type=FieldType.FLOAT,
    )
    old_field, new_schema = validate_field_update(dataset, "age", field_update)
    assert old_field is not None
    assert old_field.type == FieldType.INTEGER
    assert new_schema[0].type == FieldType.FLOAT

    # Test invalid type conversion (string to integer)
    field_update = SchemaField(
        field_name="name",
        description="Name as integer",
        type=FieldType.INTEGER,
    )
    with pytest.raises(InvalidRecordDataError) as exc:
        validate_field_update(dataset, "name", field_update)
    assert "Cannot safely convert" in str(exc.value)

    # Test non-existent field
    field_update = SchemaField(
        field_name="invalid",
        description="Invalid field",
        type=FieldType.STRING,
    )
    with pytest.raises(InvalidDatasetSchemaError) as exc:
        validate_field_update(dataset, "invalid", field_update)
    assert "not found in schema" in str(exc.value)
