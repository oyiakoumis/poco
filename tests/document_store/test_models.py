"""Tests for the document store models."""

from datetime import datetime, timezone

import pytest
from bson import ObjectId
from pydantic import ValidationError

from src.document_store.exceptions import (
    InvalidDatasetStructureError,
    InvalidFieldTypeError,
)
from src.document_store.models import Dataset, Record
from src.document_store.types import Field, FieldType


def test_dataset_creation():
    """Test creating a dataset with valid data."""
    dataset = Dataset(
        user_id="user123",
        name="test_dataset",
        description="Test dataset",
        structure=[
            Field(field_name="age", description="User age", type=FieldType.INTEGER, required=True),
            Field(field_name="name", description="User name", type=FieldType.STRING, required=True),
        ],
    )
    assert dataset.user_id == "user123"
    assert dataset.name == "test_dataset"
    assert len(dataset.structure) == 2
    assert isinstance(dataset.created_at, datetime)
    assert dataset.created_at.tzinfo == timezone.utc


def test_dataset_duplicate_fields():
    """Test that dataset creation fails with duplicate field names."""
    with pytest.raises(InvalidDatasetStructureError):
        Dataset(
            user_id="user123",
            name="test_dataset",
            description="Test dataset",
            structure=[
                Field(field_name="name", description="First name", type=FieldType.STRING),
                Field(field_name="name", description="Last name", type=FieldType.STRING),
            ],
        )


def test_dataset_invalid_field_type():
    """Test that dataset creation fails with invalid field type."""
    with pytest.raises(ValidationError) as exc:
        Dataset(
            user_id="user123",
            name="test_dataset",
            description="Test dataset",
            structure=[Field(field_name="field1", description="Test field", type="invalid_type")],
        )
    assert "Input should be" in str(exc.value)
    assert "int" in str(exc.value)
    assert "float" in str(exc.value)
    assert "str" in str(exc.value)


def test_dataset_default_value_validation():
    """Test validation of default values against field types."""
    # Valid default values
    dataset = Dataset(
        user_id="user123",
        name="test_dataset",
        description="Test dataset",
        structure=[
            Field(field_name="age", description="Age", type=FieldType.INTEGER, default=25),
            Field(field_name="height", description="Height", type=FieldType.FLOAT, default=1.75),
            Field(field_name="name", description="Name", type=FieldType.STRING, default="John"),
        ],
    )
    assert isinstance(dataset.structure[0].default, int)
    assert isinstance(dataset.structure[1].default, float)
    assert isinstance(dataset.structure[2].default, str)

    # Invalid default values
    with pytest.raises(InvalidFieldTypeError):
        Dataset(
            user_id="user123",
            name="test_dataset",
            description="Test dataset",
            structure=[Field(field_name="age", description="Age", type=FieldType.INTEGER, default="not_a_number")],
        )


def test_record_creation():
    """Test creating a record with valid data."""
    record = Record(user_id="user123", dataset_id="67a4b04db3e538515b35a158", data={"name": "John Doe", "age": 30})
    assert record.user_id == "user123"
    assert record.dataset_id == ObjectId("67a4b04db3e538515b35a158")
    assert record.data["name"] == "John Doe"
    assert record.data["age"] == 30
    assert isinstance(record.created_at, datetime)
    assert record.created_at.tzinfo == timezone.utc


def test_record_empty_data():
    """Test creating a record with empty data."""
    record = Record(user_id="user123", dataset_id="67a4b04db3e538515b35a158", data={})
    assert record.data == {}


def test_record_invalid_data_type():
    """Test that record creation fails with invalid data type."""
    with pytest.raises(ValidationError):
        Record(user_id="user123", dataset_id="67a4b04db3e538515b35a158", data="invalid_data")  # Should be a dict


def test_model_id_alias():
    """Test that _id alias works for both Dataset and Record."""
    # Test Dataset
    dataset = Dataset(_id="67a4b03bb3e538515b35a156", user_id="user123", name="test_dataset", description="Test dataset", structure=[])
    assert dataset.id == ObjectId("67a4b03bb3e538515b35a156")

    # Test Record
    record = Record(_id="67a4b04db3e538515b35a157", user_id="user123", dataset_id="67a4b03bb3e538515b35a156", data={})
    assert record.id == ObjectId("67a4b04db3e538515b35a157")
