"""Schema model definitions for the document store module."""

from collections import UserList
from typing import List, Optional

from document_store.models.field import SchemaField


class DatasetSchema(UserList):
    """Schema definition for a dataset."""

    data: List[SchemaField]

    def __init__(self, initlist: Optional[List[SchemaField]] = None):
        """Initialize schema with list of fields."""
        super().__init__(initlist or [])

    def get_field(self, field_name: str) -> SchemaField:
        """Get field from schema by name.

        Args:
            field_name: Name of the field to get

        Returns:
            SchemaField: The field definition

        Raises:
            KeyError: If field not found
        """
        for field in self.data:
            if field.field_name == field_name:
                return field
        raise KeyError(f"Field '{field_name}' not found in schema")

    def has_field(self, field_name: str) -> bool:
        """Check if field exists in schema.

        Args:
            field_name: Name of the field to check

        Returns:
            bool: True if field exists, False otherwise
        """
        return any(field.field_name == field_name for field in self.data)

    def get_field_names(self) -> List[str]:
        """Get list of all field names in schema.

        Returns:
            List[str]: List of field names
        """
        return [field.field_name for field in self.data]
