"""Field model definitions for the document store module."""

from typing import Any, List, Optional

from pydantic import BaseModel, Field

from document_store.models.types import FieldType


class SchemaField(BaseModel):
    """Field definition for dataset schema."""

    field_name: str = Field(description="Name of the field in the dataset schema", min_length=1, max_length=100, json_schema_extra={"examples": ["user_age"]})
    description: str = Field(
        description="Detailed description of what this field represents",
        min_length=1,
        max_length=500,
        json_schema_extra={"examples": ["Age of the user in years"]},
    )
    type: FieldType = Field(description="Data type of the field", json_schema_extra={"examples": [FieldType.INTEGER]})
    required: bool = Field(default=False, description="Whether this field must be present in all records", json_schema_extra={"examples": [True]})
    default: Optional[Any] = Field(default=None, description="Default value for the field if not provided", json_schema_extra={"examples": [0]})
    options: Optional[List[str]] = Field(
        default=None, description="List of allowed values for select/multi-select fields", json_schema_extra={"examples": [["option1", "option2"]]}
    )

    model_config = {"arbitrary_types_allowed": True, "populate_by_name": True, "from_attributes": True}
