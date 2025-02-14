"""Tests for schema validation and schema updates."""

import pytest
from datetime import datetime, date

from document_store.exceptions import (
    InvalidDatasetSchemaError,
    InvalidSchemaUpdateError,
    TypeConversionError,
)
from document_store.types import FieldType, SchemaField
from document_store.validators.schema import validate_schema, validate_schema_update


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


def test_validate_schema_update_type_conversions():
    """Test schema update validation for type conversions."""
    current_schema = [
        SchemaField(
            field_name="count",
            description="Item count",
            type=FieldType.INTEGER,
        ),
        SchemaField(
            field_name="price",
            description="Item price",
            type=FieldType.FLOAT,
        ),
        SchemaField(
            field_name="created",
            description="Creation date",
            type=FieldType.DATE,
        ),
    ]
    
    # Test valid conversions
    new_schema = [
        SchemaField(
            field_name="count",
            description="Item count",
            type=FieldType.FLOAT,  # INT to FLOAT is allowed
        ),
        SchemaField(
            field_name="price",
            description="Item price",
            type=FieldType.STRING,  # FLOAT to STRING is allowed
        ),
        SchemaField(
            field_name="created",
            description="Creation date",
            type=FieldType.DATETIME,  # DATE to DATETIME is allowed
        ),
    ]
    
    # Should not raise any exceptions
    validate_schema_update(current_schema, new_schema)
    
    # Test invalid conversions
    invalid_schema = [
        SchemaField(
            field_name="count",
            description="Item count",
            type=FieldType.DATE,  # INT to DATE is not allowed
        ),
    ]
    
    with pytest.raises(InvalidSchemaUpdateError) as exc:
        validate_schema_update(current_schema, invalid_schema)
    assert "Cannot convert field" in str(exc.value)


def test_validate_schema_update_new_fields():
    """Test schema update validation for new fields."""
    current_schema = [
        SchemaField(
            field_name="name",
            description="User name",
            type=FieldType.STRING,
        ),
    ]
    
    # Test new required field with default
    new_schema = [
        SchemaField(
            field_name="name",
            description="User name",
            type=FieldType.STRING,
        ),
        SchemaField(
            field_name="age",
            description="User age",
            type=FieldType.INTEGER,
            required=True,
            default=18,
        ),
    ]
    
    # Should not raise any exceptions
    validate_schema_update(current_schema, new_schema)
    
    # Test new required field without default
    new_schema = [
        SchemaField(
            field_name="name",
            description="User name",
            type=FieldType.STRING,
        ),
        SchemaField(
            field_name="age",
            description="User age",
            type=FieldType.INTEGER,
            required=True,  # Required but no default
        ),
    ]
    
    with pytest.raises(InvalidSchemaUpdateError) as exc:
        validate_schema_update(current_schema, new_schema)
    assert "must have a default value" in str(exc.value)


def test_validate_schema_update_removed_fields():
    """Test schema update validation for removed fields."""
    current_schema = [
        SchemaField(
            field_name="name",
            description="User name",
            type=FieldType.STRING,
        ),
        SchemaField(
            field_name="age",
            description="User age",
            type=FieldType.INTEGER,
        ),
    ]
    
    # Test removing a field
    new_schema = [
        SchemaField(
            field_name="name",
            description="User name",
            type=FieldType.STRING,
        ),
    ]
    
    # Should not raise any exceptions
    validate_schema_update(current_schema, new_schema)


def test_validate_schema_update_select_fields():
    """Test schema update validation for select fields."""
    current_schema = [
        SchemaField(
            field_name="status",
            description="User status",
            type=FieldType.SELECT,
            options=["active", "inactive"],
        ),
    ]
    
    # Test updating options
    new_schema = [
        SchemaField(
            field_name="status",
            description="User status",
            type=FieldType.SELECT,
            options=["active", "inactive", "pending"],  # Added new option
        ),
    ]
    
    # Should not raise any exceptions
    validate_schema_update(current_schema, new_schema)
    
    # Test removing options
    new_schema = [
        SchemaField(
            field_name="status",
            description="User status",
            type=FieldType.SELECT,
            options=["active"],  # Removed option
        ),
    ]
    
    # Should not raise any exceptions - removing options is allowed
    # Data validation will handle existing values during update
    validate_schema_update(current_schema, new_schema)
