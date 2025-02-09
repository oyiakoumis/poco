"""Tests for select and multi-select validators."""

import pytest

from document_store.types import FieldType
from document_store.validators.validators import MultiSelectValidator, SelectValidator


class TestSelectValidator:
    """Test cases for SelectValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = SelectValidator()
        self.validator.set_options(["pending", "processing", "completed", "cancelled"])

    def test_validate_valid_option(self):
        """Should validate valid option."""
        assert self.validator.validate("pending") == "pending"
        assert self.validator.validate("completed") == "completed"

    def test_validate_case_sensitive(self):
        """Should be case sensitive."""
        with pytest.raises(ValueError) as exc:
            self.validator.validate("PENDING")
        assert "Value must be one of:" in str(exc.value)

    def test_validate_invalid_option(self):
        """Should raise ValueError for invalid option."""
        with pytest.raises(ValueError) as exc:
            self.validator.validate("invalid")
        assert "Value must be one of:" in str(exc.value)

    def test_validate_without_options(self):
        """Should raise ValueError if options not set."""
        validator = SelectValidator()
        with pytest.raises(ValueError) as exc:
            validator.validate("anything")
        assert "Options not set for select field" in str(exc.value)

    def test_validate_default(self):
        """Should validate default values."""
        assert self.validator.validate_default("pending") == "pending"
        assert self.validator.validate_default(None) is None
        with pytest.raises(ValueError):
            self.validator.validate_default("invalid")

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.SELECT


class TestMultiSelectValidator:
    """Test cases for MultiSelectValidator."""

    def setup_method(self):
        """Set up test cases."""
        self.validator = MultiSelectValidator()
        self.validator.set_options(["urgent", "vip", "international", "fragile"])

    def test_validate_valid_options_list(self):
        """Should validate valid options from list."""
        assert self.validator.validate(["urgent", "vip"]) == ["urgent", "vip"]
        assert self.validator.validate(["fragile"]) == ["fragile"]
        assert self.validator.validate([]) == []

    def test_validate_valid_options_string(self):
        """Should validate valid options from comma-separated string."""
        assert self.validator.validate("urgent,vip") == ["urgent", "vip"]
        assert self.validator.validate("fragile") == ["fragile"]
        assert self.validator.validate("") == []

    def test_validate_case_sensitive(self):
        """Should be case sensitive."""
        with pytest.raises(ValueError) as exc:
            self.validator.validate(["URGENT"])
        assert "Invalid options:" in str(exc.value)

    def test_validate_invalid_options(self):
        """Should raise ValueError for invalid options."""
        with pytest.raises(ValueError) as exc:
            self.validator.validate(["urgent", "invalid"])
        assert "Invalid options: invalid" in str(exc.value)

    def test_validate_without_options(self):
        """Should raise ValueError if options not set."""
        validator = MultiSelectValidator()
        with pytest.raises(ValueError) as exc:
            validator.validate(["anything"])
        assert "Options not set for multi-select field" in str(exc.value)

    def test_validate_invalid_input_type(self):
        """Should raise ValueError for invalid input type."""
        with pytest.raises(ValueError) as exc:
            self.validator.validate(123)
        assert "Value must be string (comma-separated) or list/tuple/set" in str(exc.value)

    def test_validate_default(self):
        """Should validate default values."""
        assert self.validator.validate_default(["urgent", "vip"]) == ["urgent", "vip"]
        assert self.validator.validate_default(None) is None
        with pytest.raises(ValueError):
            self.validator.validate_default(["invalid"])

    def test_field_type(self):
        """Should return correct field type."""
        assert self.validator.get_field_type() == FieldType.MULTI_SELECT

    def test_consistent_order(self):
        """Should return values in consistent order."""
        assert self.validator.validate(["vip", "urgent"]) == ["urgent", "vip"]
        assert self.validator.validate("vip,urgent") == ["urgent", "vip"]
