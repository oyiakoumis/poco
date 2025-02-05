from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class FieldType(str, Enum):
    STRING = "str"
    INTEGER = "int"
    FLOAT = "float"
    BOOLEAN = "bool"
    DATETIME = "datetime"
    SELECT = "select"
    MULTI_SELECT = "multi_select"


class AggregateFunction(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVERAGE = "avg"
    MAX = "max"
    MIN = "min"


class FieldDefinition(BaseModel):
    name: str = Field(..., description="Name of the field")
    description: str = Field(..., description="Description of the field")
    field_type: FieldType = Field(..., description="Type of the field")
    required: bool = Field(default=False, description="Whether the field is required")
    default: Optional[Any] = Field(default=None, description="Default value for the field")
    options: Optional[List[str]] = Field(default=None, description="Options for select/multi_select fields")

    def validate_field_value(self, value: Any) -> Any:
        """Validate and convert a field value according to its type."""
        if value is None:
            if self.required:
                raise ValueError(f"Field {self.name} is required")
            if callable(self.default):
                return self.default()
            return self.default

        if self.field_type == FieldType.STRING:
            return str(value)
        elif self.field_type == FieldType.INTEGER:
            return int(value)
        elif self.field_type == FieldType.FLOAT:
            return float(value)
        elif self.field_type == FieldType.BOOLEAN:
            return bool(value)
        elif self.field_type == FieldType.DATETIME:
            if isinstance(value, datetime):
                return value
            return datetime.fromisoformat(value)
        elif self.field_type == FieldType.SELECT:
            if not self.options:
                raise ValueError(f"No options defined for select field {self.name}")
            if value not in self.options:
                raise ValueError(f"Invalid value for field {self.name}. Must be one of: {self.options}")
            return value
        elif self.field_type == FieldType.MULTI_SELECT:
            if not self.options:
                raise ValueError(f"No options defined for multi_select field {self.name}")
            if not isinstance(value, list):
                raise ValueError(f"Field {self.name} must be a list")
            for v in value:
                if v not in self.options:
                    raise ValueError(f"Invalid value in field {self.name}. Must be from: {self.options}")
            return value
        raise ValueError(f"Unknown field type: {self.field_type}")


class CollectionSchema(BaseModel):
    name: str = Field(..., description="Name of the collection")
    description: str = Field(..., description="Description of the collection")
    fields: List[FieldDefinition] = Field(..., description="Fields in the collection")

    def validate_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a document against the schema."""
        validated = {}
        field_map = {field.name: field for field in self.fields}

        # Validate provided fields
        for field_name, value in document.items():
            # Skip MongoDB internal fields
            if field_name == "_id":
                validated[field_name] = value
                continue
                
            if field_name not in field_map:
                raise ValueError(f"Unknown field: {field_name}")
            field = field_map[field_name]
            validated[field_name] = field.validate_field_value(value)

        # Check required fields and apply defaults
        for field in self.fields:
            if field.name not in validated:
                validated[field.name] = field.validate_field_value(None)

        return validated


class AggregateMetric(BaseModel):
    field: str = Field(..., description="Field to aggregate")
    function: AggregateFunction = Field(..., description="Aggregation function to apply")


class DocumentQuery(BaseModel):
    filter: Dict[str, Any] = Field(default_factory=dict, description="MongoDB filter query")
    sort: Optional[Dict[str, int]] = Field(default=None, description="Sort specification")
    skip: Optional[int] = Field(default=None, description="Number of documents to skip")
    limit: Optional[int] = Field(default=None, description="Maximum number of documents")


class AggregationQuery(BaseModel):
    group_by: List[str] = Field(..., description="Fields to group by")
    metrics: List[AggregateMetric] = Field(..., description="Metrics to calculate")
    filter: Optional[Dict[str, Any]] = Field(default=None, description="Pre-aggregation filter")
