"""Tests for validator implementations."""

import pytest

from document_store.types import FieldType
from document_store.validators.validators import (
    BooleanValidator,
    FloatValidator,
    IntegerValidator,
    StringValidator,
)


class TestBooleanValidator:
    """Test cases for BooleanValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = BooleanValidator()

    def test_validate_boolean(self):
        """Should validate and convert to boolean."""
        assert self.validator.validate(True) is True
        assert self.validator.validate(False) is False
        assert self.validator.validate("true") is True
        assert self.validator.validate("false") is False
        assert self.validator.validate("yes") is True
        assert self.validator.validate("no") is False
        assert self.validator.validate("1") is True
        assert self.validator.validate("0") is False
        assert self.validator.validate(1) is True
        assert self.validator.validate(0) is False

    def test_validate_case_insensitive(self):
        """Should handle case-insensitive string inputs."""
        assert self.validator.validate("TRUE") is True
        assert self.validator.validate("FALSE") is False
        assert self.validator.validate("Yes") is True
        assert self.validator.validate("No") is False

    def test_validate_invalid_boolean(self):
        """Should raise ValueError for invalid boolean values."""
        with pytest.raises(ValueError):
            self.validator.validate("not a boolean")
        with pytest.raises(ValueError):
            self.validator.validate("2")

    def test_validate_default(self):
        """Should validate default values."""
        assert self.validator.validate_default(True) is True
        assert self.validator.validate_default(False) is False
        assert self.validator.validate_default("true") is True
        assert self.validator.validate_default(None) is None

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.BOOLEAN


class TestIntegerValidator:
    """Test cases for IntegerValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = IntegerValidator()

    def test_validate_integer(self):
        """Should validate and convert to integer."""
        assert self.validator.validate(42) == 42
        assert self.validator.validate("42") == 42
        assert self.validator.validate(42.0) == 42

    def test_validate_invalid_integer(self):
        """Should raise ValueError for invalid integers."""
        with pytest.raises(ValueError):
            self.validator.validate("not a number")
        with pytest.raises(ValueError):
            self.validator.validate(True)
        with pytest.raises(ValueError):
            self.validator.validate(False)

    def test_validate_default(self):
        """Should validate default values."""
        assert self.validator.validate_default(42) == 42
        assert self.validator.validate_default("42") == 42
        assert self.validator.validate_default(None) is None

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.INTEGER


class TestFloatValidator:
    """Test cases for FloatValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = FloatValidator()

    def test_validate_float(self):
        """Should validate and convert to float."""
        assert self.validator.validate(3.14) == 3.14
        assert self.validator.validate("3.14") == 3.14
        assert self.validator.validate(42) == 42.0

    def test_validate_invalid_float(self):
        """Should raise ValueError for invalid floats."""
        with pytest.raises(ValueError):
            self.validator.validate("not a number")
        with pytest.raises(ValueError):
            self.validator.validate(True)
        with pytest.raises(ValueError):
            self.validator.validate(False)

    def test_validate_default(self):
        """Should validate default values."""
        assert self.validator.validate_default(3.14) == 3.14
        assert self.validator.validate_default("3.14") == 3.14
        assert self.validator.validate_default(None) is None

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.FLOAT


class TestStringValidator:
    """Test cases for StringValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = StringValidator()

    def test_validate_string(self):
        """Should validate and convert to string."""
        assert self.validator.validate("hello") == "hello"
        assert self.validator.validate(42) == "42"
        assert self.validator.validate(3.14) == "3.14"
        assert self.validator.validate(True) == "True"

    def test_validate_default(self):
        """Should validate default values."""
        assert self.validator.validate_default("hello") == "hello"
        assert self.validator.validate_default(42) == "42"
        assert self.validator.validate_default(None) is None

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.STRING
