"""MongoDB filter utilities for building comparison and filter expressions."""

from typing import Any, Dict, Union

from database.document_store.models.query import (
    ComparisonOperator,
    FilterCondition,
    FilterExpression,
    LogicalOperator,
)


def build_comparison(operator: ComparisonOperator, value: Any) -> Dict:
    """Build MongoDB comparison operator expression."""
    operator_map = {
        ComparisonOperator.EQUALS: "$eq",
        ComparisonOperator.NOT_EQUALS: "$ne",
        ComparisonOperator.GREATER_THAN: "$gt",
        ComparisonOperator.GREATER_THAN_EQUALS: "$gte",
        ComparisonOperator.LESS_THAN: "$lt",
        ComparisonOperator.LESS_THAN_EQUALS: "$lte",
    }
    return {operator_map[operator]: value}


def build_filter_dict(node: Union[FilterCondition, FilterExpression]) -> Dict:
    """Recursively build MongoDB filter expression."""
    if isinstance(node, FilterCondition):
        return {f"data.{node.field}": build_comparison(node.operator, node.value)}
    else:
        # Map logical operators to MongoDB operators
        operator_map = {
            LogicalOperator.AND: "$and",
            LogicalOperator.OR: "$or",
        }
        return {operator_map[node.operator]: [build_filter_dict(expr) for expr in node.expressions]}
