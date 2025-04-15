"""Constants and enums for type system."""

from enum import Enum
from typing import Dict, Set


class FieldType(str, Enum):
    """Supported field types for dataset schema."""

    INTEGER = "Integer"
    FLOAT = "Float"
    STRING = "String"
    BOOLEAN = "Boolean"
    DATE = "Date"
    DATETIME = "Datetime"
    SELECT = "Select"
    MULTI_SELECT = "Multi Select"


class AggregationType(str, Enum):
    """Supported aggregation operations."""

    SUM = "sum"  # For INTEGER, FLOAT
    AVG = "avg"  # For INTEGER, FLOAT
    MIN = "min"  # For INTEGER, FLOAT, DATE, DATETIME
    MAX = "max"  # For INTEGER, FLOAT, DATE, DATETIME
    COUNT = "count"  # For any type


# Safe type conversion mappings
SAFE_TYPE_CONVERSIONS: Dict[FieldType, Set[FieldType]] = {
    FieldType.INTEGER: {FieldType.FLOAT, FieldType.STRING, FieldType.BOOLEAN},
    FieldType.FLOAT: {FieldType.STRING, FieldType.INTEGER},  # INTEGER conversion may lose precision
    FieldType.STRING: {FieldType.INTEGER, FieldType.FLOAT, FieldType.BOOLEAN, FieldType.DATE, FieldType.DATETIME, FieldType.SELECT, FieldType.MULTI_SELECT},  # With validation
    FieldType.BOOLEAN: {FieldType.STRING},
    FieldType.DATE: {FieldType.STRING, FieldType.DATETIME},
    FieldType.DATETIME: {FieldType.STRING, FieldType.DATE},
    # SELECT and MULTI_SELECT conversions
    FieldType.SELECT: {FieldType.SELECT, FieldType.STRING},
    FieldType.MULTI_SELECT: {FieldType.MULTI_SELECT, FieldType.STRING},
}


# Valid aggregation operations for each field type
VALID_AGGREGATIONS: Dict[FieldType, Set[AggregationType]] = {
    FieldType.INTEGER: {AggregationType.SUM, AggregationType.AVG, AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.FLOAT: {AggregationType.SUM, AggregationType.AVG, AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.STRING: {AggregationType.COUNT},
    FieldType.BOOLEAN: {AggregationType.COUNT},
    FieldType.DATE: {AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.DATETIME: {AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.SELECT: {AggregationType.COUNT},
    FieldType.MULTI_SELECT: {AggregationType.COUNT},
}
