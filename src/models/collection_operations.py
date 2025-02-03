from typing import List, Literal

from pydantic import Field, model_validator

from models.base import BaseCollectionOperation
from models.fields import CollectionSchemaField, IndexDefinition


class CreateCollectionModel(BaseCollectionOperation):
    """Model for collection creation operation."""

    intent: Literal["create_collection"]
    collection_schema: List[CollectionSchemaField] = Field(min_items=1)
    indexes: List[IndexDefinition] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_schema(self) -> "CreateCollectionModel":
        """Validate collection schema and ensure required fields."""
        field_names = set()
        for field in self.collection_schema:
            if field.name in field_names:
                raise ValueError(f"Duplicate field name: {field.name}")
            field_names.add(field.name)
        return self


class RenameCollectionModel(BaseCollectionOperation):
    """Model for renaming a collection."""

    intent: Literal["rename_collection"]
    new_name: str = Field(min_length=1, description="The new name for the collection")


class DeleteCollectionModel(BaseCollectionOperation):
    """Model for deleting a collection."""

    intent: Literal["delete_collection"]
    confirm_deletion: bool = Field(default=False, description="Safety flag that must be set to True to delete the collection")


class AddFieldsModel(BaseCollectionOperation):
    """Model for adding new fields to an existing collection."""

    intent: Literal["add_fields"]
    fields: List[CollectionSchemaField] = Field(min_items=1, description="The new fields to add to the collection")

    @model_validator(mode="after")
    def validate_new_fields(self) -> "AddFieldsModel":
        """Validate that required fields have default values."""
        for field in self.fields:
            if field.required and not hasattr(field, "default_value"):
                raise ValueError(f"New required field '{field.name}' must have a default value")
        return self


class RemoveFieldsModel(BaseCollectionOperation):
    """Model for removing fields from an existing collection."""

    intent: Literal["remove_fields"]
    field_names: List[str] = Field(min_items=1, description="The names of the fields to remove")
    confirm_deletion: bool = Field(default=False, description="Safety flag that must be set to True to remove fields")


class ModifyFieldModel(BaseCollectionOperation):
    """Model for modifying an existing field's properties."""

    intent: Literal["modify_field"]
    field_name: str = Field(description="The name of the field to modify")
    new_properties: CollectionSchemaField = Field(description="The new properties for the field")
    force_cast: bool = Field(default=False, description="Whether to force type casting of existing values")


class AddIndexModel(BaseCollectionOperation):
    """Model for adding a new index to an existing collection."""

    intent: Literal["add_index"]
    index: IndexDefinition = Field(description="The index to add")


class RemoveIndexModel(BaseCollectionOperation):
    """Model for removing an index from a collection."""

    intent: Literal["remove_index"]
    fields: List[str] = Field(min_items=1, description="The fields that make up the index to remove")
