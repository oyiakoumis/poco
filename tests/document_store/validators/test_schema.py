"""Tests for schema validation and schema updates."""

from datetime import date, datetime

import pytest

from document_store.exceptions import (
    InvalidDatasetSchemaError,
    InvalidSchemaUpdateError,
    TypeConversionError,
)
from document_store.types import FieldType, SchemaField
from document_store.validators.schema import validate_schema


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
