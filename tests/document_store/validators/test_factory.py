"""Tests for validator factory."""

import pytest

from document_store.types import FieldType
from document_store.validators.base import TypeValidator
from document_store.validators.factory import ValidatorFactory, get_validator
from document_store.validators.validators import (
    BooleanValidator,
    DateTimeValidator,
    DateValidator,
    FloatValidator,
    IntegerValidator,
    StringValidator,
)


def test_get_validator_integer():
    """Should return IntegerValidator for INTEGER type."""
    validator = get_validator(FieldType.INTEGER)
    assert isinstance(validator, IntegerValidator)


def test_get_validator_float():
    """Should return FloatValidator for FLOAT type."""
    validator = get_validator(FieldType.FLOAT)
    assert isinstance(validator, FloatValidator)


def test_get_validator_string():
    """Should return StringValidator for STRING type."""
    validator = get_validator(FieldType.STRING)
    assert isinstance(validator, StringValidator)


def test_get_validator_boolean():
    """Should return BooleanValidator for BOOLEAN type."""
    validator = get_validator(FieldType.BOOLEAN)
    assert isinstance(validator, BooleanValidator)


def test_get_validator_date():
    """Should return DateValidator for DATE type."""
    validator = get_validator(FieldType.DATE)
    assert isinstance(validator, DateValidator)


def test_get_validator_datetime():
    """Should return DateTimeValidator for DATETIME type."""
    validator = get_validator(FieldType.DATETIME)
    assert isinstance(validator, DateTimeValidator)


def test_get_validator_unknown():
    """Should raise ValueError for unknown field type."""
    # Create a field type that isn't registered
    class UnknownType:
        value = "unknown"

    with pytest.raises(ValueError) as exc:
        get_validator(UnknownType())
    assert "No validator registered for field type" in str(exc.value)


def test_register_new_validator():
    """Should allow registering new validator types."""

    # Create a new field type
    class BooleanType:
        value = "bool"

    # Create a new validator
    class BooleanValidator(TypeValidator):
        field_type = BooleanType()

        def validate(self, value):
            if isinstance(value, str):
                return value.lower() == "true"
            return bool(value)

        def validate_default(self, value):
            if value is None:
                return None
            return self.validate(value)

    # Register the new validator
    ValidatorFactory.register_validator(BooleanValidator)

    # Should be able to get an instance
    validator = get_validator(BooleanType())
    assert isinstance(validator, BooleanValidator)

    # Should work with the validator
    assert validator.validate("true") is True
    assert validator.validate("false") is False
    assert validator.validate(1) is True
    assert validator.validate(0) is False


def test_validator_instances():
    """Should return new instances for each call."""
    validator1 = get_validator(FieldType.INTEGER)
    validator2 = get_validator(FieldType.INTEGER)
    assert validator1 is not validator2
