"""Schema model definitions for the document store module."""

from typing import List, Optional, Tuple

from pydantic import BaseModel, Field, model_validator

from document_store.exceptions import InvalidDatasetSchemaError, InvalidRecordDataError
from document_store.models.field import SchemaField
from document_store.models.types import SAFE_TYPE_CONVERSIONS


class DatasetSchema(BaseModel):
    """Schema definition for a dataset."""

    fields: List[SchemaField] = Field(default_factory=list, description="List of fields in the schema")

    @model_validator(mode="after")
    def validate_unique_fields(self) -> "DatasetSchema":
        """Validate field names are unique."""
        field_names = [field.field_name for field in self.fields]
        unique_names = set(field_names)
        if len(field_names) != len(unique_names):
            # Find the duplicate names
            duplicates = [name for name in field_names if field_names.count(name) > 1]
            raise InvalidDatasetSchemaError(f"Duplicate field names in schema: {', '.join(set(duplicates))}")
        return self

    def __len__(self) -> int:
        """Get number of fields in schema."""
        return len(self.fields)

    def __getitem__(self, index: int) -> SchemaField:
        """Get field at index."""
        return self.fields[index]

    def __iter__(self):
        """Iterate over fields in the schema."""
        return iter(self.fields)

    def append(self, field: SchemaField) -> None:
        """Add a field to the schema.

        Args:
            field: Field to add to the schema

        Raises:
            InvalidDatasetSchemaError: If field name already exists in schema
        """
        if field.field_name in (f.field_name for f in self.fields):
            raise InvalidDatasetSchemaError(f"Field name '{field.field_name}' already exists in schema")

        # Field is already validated by Pydantic
        self.fields.append(field)

    def get_field(self, field_name: str) -> SchemaField:
        """Get field from schema by name.

        Args:
            field_name: Name of the field to get

        Returns:
            SchemaField: The field definition

        Raises:
            KeyError: If field not found
        """
        for field in self.fields:
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
        return any(field.field_name == field_name for field in self.fields)

    def get_field_names(self) -> List[str]:
        """Get list of all field names in schema.

        Returns:
            List[str]: List of field names
        """
        return [field.field_name for field in self.fields]

    def validate_field_update(self, field_name: str, field_update: SchemaField) -> Tuple[Optional[SchemaField], Optional["DatasetSchema"]]:
        """Validates field update and returns old field and new schema.

        Args:
            field_name: Name of field to update
            field_update: New field definition

        Returns:
            Tuple of (old field, new schema), or (None, None) if no changes needed

        Raises:
            InvalidDatasetSchemaError: If field not found or update invalid
            InvalidRecordDataError: If type conversion not safe
        """
        old_field = None
        new_schema = []

        # Find and validate field
        for field in self.fields:
            if field.field_name == field_name:
                old_field = field
                # Check if anything changed
                if (
                    field.type == field_update.type
                    and field.description == field_update.description
                    and field.required == field_update.required
                    and field.default == field_update.default
                    and field.options == field_update.options
                ):
                    # No changes needed
                    return None, None

                # Validate type conversion if type is changing
                if field.type != field_update.type:
                    safe_conversions = SAFE_TYPE_CONVERSIONS.get(field.type, set())
                    if field_update.type not in safe_conversions:
                        raise InvalidRecordDataError(f"Cannot safely convert field '{field_name}' from {field.type} " f"to {field_update.type}")
                new_schema.append(field_update)
            else:
                new_schema.append(field)

        if not old_field:
            raise InvalidDatasetSchemaError(f"Field '{field_name}' not found in schema")

        # Create and validate new schema (validation happens automatically)
        new_schema_obj = DatasetSchema(fields=new_schema)
        return old_field, new_schema_obj
