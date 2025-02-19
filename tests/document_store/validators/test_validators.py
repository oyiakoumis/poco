"""Tests for validator implementations."""

from datetime import date, datetime

import pytest

from document_store.types import FieldType
from document_store.type_validators.type_validators import (
    BooleanValidator,
    DateTimeValidator,
    DateValidator,
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


class TestDateValidator:
    """Test cases for DateValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = DateValidator()

    def test_validate_date(self):
        """Should validate and convert to date."""
        # Test date object
        test_date = date(2024, 2, 9)
        assert self.validator.validate(test_date) == test_date

        # Test datetime object
        test_datetime = datetime(2024, 2, 9, 12, 34, 56)
        assert self.validator.validate(test_datetime) == test_datetime.date()

        # Test string in YYYY-MM-DD format
        assert self.validator.validate("2024-02-09") == date(2024, 2, 9)

    def test_validate_invalid_date(self):
        """Should raise ValueError for invalid dates."""
        # Test invalid format
        with pytest.raises(ValueError) as exc:
            self.validator.validate("09/02/2024")
        assert "Invalid date format" in str(exc.value)

        # Test invalid date string
        with pytest.raises(ValueError):
            self.validator.validate("2024-13-45")  # Invalid month and day

        # Test invalid type
        with pytest.raises(ValueError):
            self.validator.validate(123)

    def test_validate_default(self):
        """Should validate default values."""
        # Test None value
        assert self.validator.validate_default(None) is None

        # Test valid default
        test_date = date(2024, 2, 9)
        assert self.validator.validate_default(test_date) == test_date

        # Test string default
        assert self.validator.validate_default("2024-02-09") == date(2024, 2, 9)

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.DATE


class TestDateTimeValidator:
    """Test cases for DateTimeValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = DateTimeValidator()

    def test_validate_datetime(self):
        """Should validate and convert to datetime."""
        # Test datetime object
        test_datetime = datetime(2024, 2, 9, 12, 34, 56)
        assert self.validator.validate(test_datetime) == test_datetime

        # Test ISO format string with T
        assert self.validator.validate("2024-02-09T12:34:56") == datetime(2024, 2, 9, 12, 34, 56)

        # Test space-separated format
        assert self.validator.validate("2024-02-09 12:34:56") == datetime(2024, 2, 9, 12, 34, 56)

    def test_validate_invalid_datetime(self):
        """Should raise ValueError for invalid datetimes."""
        # Test invalid format
        with pytest.raises(ValueError) as exc:
            self.validator.validate("09/02/2024 12:34:56")
        assert "Invalid datetime format" in str(exc.value)

        # Test invalid datetime string
        with pytest.raises(ValueError):
            self.validator.validate("2024-13-45T25:67:89")  # Invalid values

        # Test invalid type
        with pytest.raises(ValueError):
            self.validator.validate(123)

        # Test date without time
        with pytest.raises(ValueError):
            self.validator.validate("2024-02-09")

    def test_validate_default(self):
        """Should validate default values."""
        # Test None value
        assert self.validator.validate_default(None) is None

        # Test valid default
        test_datetime = datetime(2024, 2, 9, 12, 34, 56)
        assert self.validator.validate_default(test_datetime) == test_datetime

        # Test string default
        assert self.validator.validate_default("2024-02-09T12:34:56") == datetime(2024, 2, 9, 12, 34, 56)

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.DATETIME
