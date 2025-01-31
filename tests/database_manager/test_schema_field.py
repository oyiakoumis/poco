import pytest
from datetime import datetime
from database_manager.exceptions import ValidationError
from src.database_manager.schema_field import SchemaField, DataType


def test_schema_field_initialization():
    field = SchemaField(
        name="test_field",
        description="A test field",
        field_type=DataType.STRING,
        required=True,
        default="default_value",
    )

    assert field.name == "test_field"
    assert field.description == "A test field"
    assert field.field_type == DataType.STRING
    assert field.required is True
    assert field.default == "default_value"


def test_schema_field_validation():
    string_field = SchemaField("name", "desc", DataType.STRING, required=True)
    integer_field = SchemaField("age", "desc", DataType.INTEGER, required=True)
    float_field = SchemaField("price", "desc", DataType.FLOAT, required=True)
    boolean_field = SchemaField("active", "desc", DataType.BOOLEAN, required=True)
    datetime_field = SchemaField("timestamp", "desc", DataType.DATETIME, required=True)

    assert string_field.validate("hello")
    assert integer_field.validate(10)
    assert float_field.validate(10.5)
    assert float_field.validate(10)  # Integers should be valid for FLOAT
    assert boolean_field.validate(True)
    assert boolean_field.validate(False)
    assert datetime_field.validate(datetime.now())

    with pytest.raises(ValidationError, match="Field name is required"):
        string_field.validate(None)

    with pytest.raises(ValidationError, match="expected string, got <class 'int'>"):
        string_field.validate(100)

    with pytest.raises(ValidationError, match="expected integer, got <class 'str'>"):
        integer_field.validate("not an int")

    with pytest.raises(ValidationError, match="expected float, got <class 'str'>"):
        float_field.validate("not a float")

    with pytest.raises(ValidationError, match="expected boolean, got <class 'str'>"):
        boolean_field.validate("not a bool")

    with pytest.raises(ValidationError, match="expected datetime, got <class 'str'>"):
        datetime_field.validate("not a datetime")


def test_schema_field_optional_validation():
    optional_field = SchemaField("optional", "desc", DataType.STRING, required=False)
    assert optional_field.validate(None)  # Should not raise an error


def test_schema_field_to_dict():
    field = SchemaField("name", "desc", DataType.STRING, required=True, default="default")
    expected_dict = {
        "name": "name",
        "description": "desc",
        "field_type": "string",
        "required": True,
        "default": "default",
    }
    assert field.to_dict() == expected_dict


def test_schema_field_from_dict():
    data = {
        "name": "name",
        "description": "desc",
        "field_type": "string",
        "required": True,
        "default": "default",
    }
    field = SchemaField.from_dict(data)
    assert field.name == "name"
    assert field.description == "desc"
    assert field.field_type == DataType.STRING
    assert field.required is True
    assert field.default == "default"
