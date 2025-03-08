"""Constants and enums for type system."""

from enum import Enum
from typing import Dict, Set


class FieldType(str, Enum):
    """Supported field types for dataset schema."""

    INTEGER = "int"
    FLOAT = "float"
    STRING = "str"
    BOOLEAN = "bool"
    DATE = "date"
    DATETIME = "datetime"
    SELECT = "select"
    MULTI_SELECT = "multi_select"


class AggregationType(str, Enum):
    """Supported aggregation operations."""

    SUM = "sum"  # For INTEGER, FLOAT
    AVG = "avg"  # For INTEGER, FLOAT
    MIN = "min"  # For INTEGER, FLOAT, DATE, DATETIME
    MAX = "max"  # For INTEGER, FLOAT, DATE, DATETIME
    COUNT = "count"  # For any type


# Safe type conversion mappings
SAFE_TYPE_CONVERSIONS: Dict[FieldType, Set[FieldType]] = {
    FieldType.INTEGER: {FieldType.FLOAT, FieldType.STRING},
    FieldType.FLOAT: {FieldType.STRING},
    FieldType.BOOLEAN: {FieldType.STRING},
    FieldType.DATE: {FieldType.STRING, FieldType.DATETIME},
    FieldType.DATETIME: {FieldType.STRING},
    # SELECT and MULTI_SELECT only allow same type conversions
    FieldType.SELECT: {FieldType.SELECT},
    FieldType.MULTI_SELECT: {FieldType.MULTI_SELECT},
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
