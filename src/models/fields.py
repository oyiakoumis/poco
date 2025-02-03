from datetime import date, datetime
from typing import List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from database_manager.schema_field import FieldType
from models.enums import ComparisonOperator, IndexOrder, SortDirection


class OrderByModel(BaseModel):
    """Model for specifying sort order in queries."""

    field: str = Field(description="The name of the field to order by.")
    direction: SortDirection = Field(description="The direction to order the results by.")


class TableSchemaField(BaseModel):
    """Model for defining table schema fields."""

    name: str = Field(description="The name of the column/field in the table.")
    type: FieldType = Field(description="The data type of the field.")
    nullable: bool = Field(default=True, description="Whether the field can accept null values.")
    required: bool = Field(default=False, description="Whether this field must be provided when adding records.")
    options: Optional[List[str]] = Field(default=None, description="Predefined options for 'select' or 'multi-select' field types.")

    @field_validator("options")
    def validate_options(cls, v: Optional[List[str]], values: dict) -> Optional[List[str]]:
        """Validate that options are provided only for select/multi-select fields."""
        field_type = values.get("type")
        if field_type not in (FieldType.SELECT, FieldType.MULTI_SELECT) and v is not None:
            raise ValueError("Options can only be specified for select or multi-select fields")
        if field_type in (FieldType.SELECT, FieldType.MULTI_SELECT) and not v:
            raise ValueError("Options must be provided for select or multi-select fields")
        return v


class IndexField(BaseModel):
    """Model for defining index fields."""

    field_name: str
    order: IndexOrder


class IndexDefinition(BaseModel):
    """Model for defining table indexes."""

    fields: List[IndexField]
    unique: bool = Field(default=False)

    @model_validator(mode="after")
    def validate_fields(self) -> "IndexDefinition":
        """Ensure at least one field is specified for the index."""
        if not self.fields:
            raise ValueError("At least one field must be specified for an index")
        return self


class RecordValue(BaseModel):
    """Model for handling record field values with type validation."""

    value: Union[str, int, float, bool, date, datetime, List[str]]


class RecordModel(BaseModel):
    """Model for record operations."""

    field: str
    value: Union[str, int, float, bool, date, datetime, List[str]] = Field(description="The value to assign to the field.")


class ConditionModel(BaseModel):
    """Model for query conditions."""

    field: str
    operator: ComparisonOperator
    value: Union[str, int, float, bool, date, datetime]
