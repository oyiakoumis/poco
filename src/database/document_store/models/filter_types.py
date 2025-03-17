"""Type definitions for filter expressions and conditions."""

from enum import Enum
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field


class ComparisonOperator(str, Enum):
    """Supported comparison operators for filtering."""

    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    GREATER_THAN_EQUALS = "gte"
    LESS_THAN = "lt"
    LESS_THAN_EQUALS = "lte"


class LogicalOperator(str, Enum):
    """Logical operators for combining filter conditions."""

    AND = "and"
    OR = "or"


class FilterCondition(BaseModel):
    """Single field condition."""

    field: str = Field(description="Field name to filter on")
    operator: ComparisonOperator = Field(description="Comparison operator to use for filtering")
    value: Any = Field(description="Value to compare against")


class FilterExpression(BaseModel):
    """Logical combination of conditions or other expressions."""

    operator: LogicalOperator = Field(description="Logical operator to combine filter expressions")
    expressions: List[Union[FilterCondition, "FilterExpression"]] = Field(description="List of filter conditions or nested expressions to combine")
