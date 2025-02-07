"""Tests for validator implementations."""

import pytest

from document_store.types import FieldType
from document_store.validators.validators import (
    FloatValidator,
    IntegerValidator,
    StringValidator,
)


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
