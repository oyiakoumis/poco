from typing import List, Literal

from pydantic import Field, model_validator

from models.base import BaseTableOperation
from models.fields import IndexDefinition, TableSchemaField


class CreateTableModel(BaseTableOperation):
    """Model for table creation operation."""

    intent: Literal["create_table"]
    table_schema: List[TableSchemaField] = Field(min_items=1)
    indexes: List[IndexDefinition] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_schema(self) -> "CreateTableModel":
        """Validate table schema and ensure required fields."""
        field_names = set()
        for field in self.table_schema:
            if field.name in field_names:
                raise ValueError(f"Duplicate field name: {field.name}")
            field_names.add(field.name)
        return self


class RenameTableModel(BaseTableOperation):
    """Model for renaming a table."""

    intent: Literal["rename_table"]
    new_name: str = Field(min_length=1, description="The new name for the table")


class DeleteTableModel(BaseTableOperation):
    """Model for deleting a table."""

    intent: Literal["delete_table"]
    confirm_deletion: bool = Field(default=False, description="Safety flag that must be set to True to delete the table")


class AddFieldsModel(BaseTableOperation):
    """Model for adding new fields to an existing table."""

    intent: Literal["add_fields"]
    fields: List[TableSchemaField] = Field(min_items=1, description="The new fields to add to the table")

    @model_validator(mode="after")
    def validate_new_fields(self) -> "AddFieldsModel":
        """Validate that required fields have default values."""
        for field in self.fields:
            if field.required and not hasattr(field, "default_value"):
                raise ValueError(f"New required field '{field.name}' must have a default value")
        return self


class RemoveFieldsModel(BaseTableOperation):
    """Model for removing fields from an existing table."""

    intent: Literal["remove_fields"]
    field_names: List[str] = Field(min_items=1, description="The names of the fields to remove")
    confirm_deletion: bool = Field(default=False, description="Safety flag that must be set to True to remove fields")


class ModifyFieldModel(BaseTableOperation):
    """Model for modifying an existing field's properties."""

    intent: Literal["modify_field"]
    field_name: str = Field(description="The name of the field to modify")
    new_properties: TableSchemaField = Field(description="The new properties for the field")
    force_cast: bool = Field(default=False, description="Whether to force type casting of existing values")


class AddIndexModel(BaseTableOperation):
    """Model for adding a new index to an existing table."""

    intent: Literal["add_index"]
    index: IndexDefinition = Field(description="The index to add")


class RemoveIndexModel(BaseTableOperation):
    """Model for removing an index from a table."""

    intent: Literal["remove_index"]
    fields: List[str] = Field(min_items=1, description="The fields that make up the index to remove")
