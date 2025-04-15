"""Field model definitions for the document store module."""

from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from database.document_store.exceptions import InvalidDatasetSchemaError
from database.document_store.models.types import FieldType, TypeRegistry


class SchemaField(BaseModel):
    """Field definition for dataset schema."""

    field_name: str = Field(description="Name of the field in the dataset schema", min_length=1, max_length=128)
    description: str = Field(description="Detailed description of what this field represents", min_length=1, max_length=500)
    type: FieldType = Field(description="Data type of the field")
    required: bool = Field(default=False, description="Whether this field must be present in all records")
    unique: bool = Field(default=False, description="Whether this field's values must be unique across all records")
    default: Optional[Any] = Field(default=None, description="Default value for the field")
    options: Optional[List[str]] = Field(
        default=None, description="List of allowed values for select/multi-select fields. Cannot be null if type is select or multi-select"
    )

    model_config = {"arbitrary_types_allowed": True, "populate_by_name": True, "from_attributes": True}

    @field_validator("type")
    @classmethod
    def validate_field_type(cls, v: Any) -> FieldType:
        """Validate and convert field type."""
        if not isinstance(v, FieldType):
            try:
                return FieldType(v)
            except ValueError:
                raise InvalidDatasetSchemaError(f"Invalid field type: {v}")
        return v

    @model_validator(mode="after")
    def validate_field_options_and_default(self) -> "SchemaField":
        """Validate field options and default value."""
        type_impl = TypeRegistry.get_type(self.type)

        if self.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
            if not self.options:
                raise InvalidDatasetSchemaError(f"Options not provided for {self.type} field '{self.field_name}'")
            type_impl.set_options(self.options)

        if self.default is not None:
            try:
                self.default = type_impl.validate_default(self.default)
            except ValueError as e:
                raise InvalidDatasetSchemaError(f"Invalid default value for field '{self.field_name}': {str(e)}")

        return self
