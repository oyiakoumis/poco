"""MongoDB pipeline builder for aggregation queries."""

from typing import Any, Dict, List, Union

from database.document_store.models.query import (
    AggregationType,
    ComparisonOperator,
    FilterCondition,
    FilterExpression,
    LogicalOperator,
    RecordQuery,
)


def _build_comparison(operator: ComparisonOperator, value: Any) -> Dict:
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


def _build_filter_expression(node: Union[FilterCondition, FilterExpression]) -> Dict:
    """Recursively build MongoDB filter expression."""
    if isinstance(node, FilterCondition):
        return {f"data.{node.field}": _build_comparison(node.operator, node.value)}
    else:
        # Map logical operators to MongoDB operators
        operator_map = {
            LogicalOperator.AND: "$and",
            LogicalOperator.OR: "$or",
        }
        return {operator_map[node.operator]: [_build_filter_expression(expr) for expr in node.expressions]}


def _build_match_stage(filter_node: Union[FilterCondition, FilterExpression]) -> Dict:
    """Build MongoDB $match stage from filter node."""
    return {"$match": _build_filter_expression(filter_node)}


def _build_group_stage(query: RecordQuery) -> Dict:
    """Build MongoDB $group stage from aggregation query."""
    group_stage: Dict[str, Any] = {"$group": {"_id": None if not query.group_by else {field: f"$data.{field}" for field in query.group_by}}}

    for agg in query.aggregations:
        if agg.operation == AggregationType.COUNT:
            group_stage["$group"][agg.alias] = {"$sum": 1}
        else:
            operator_map = {
                AggregationType.SUM: "$sum",
                AggregationType.AVG: "$avg",
                AggregationType.MIN: "$min",
                AggregationType.MAX: "$max",
            }
            group_stage["$group"][agg.alias] = {operator_map[agg.operation]: f"$data.{agg.field}"}

    return group_stage


def _build_sort_stage(sort_config: Dict[str, bool]) -> Dict:
    """Build MongoDB $sort stage from sort configuration."""
    return {"$sort": {field: 1 if ascending else -1 for field, ascending in sort_config.items()}}


def build_aggregation_pipeline(user_id: str, dataset_id: str, query: RecordQuery) -> List[Dict]:
    """Build MongoDB aggregation pipeline from query.

    Args:
        user_id: User ID for filtering
        dataset_id: Dataset ID for filtering
        query: Aggregation query

    Returns:
        List of pipeline stages
    """
    pipeline: List[Dict] = [
        # Initial match to filter by user and dataset
        {"$match": {"user_id": user_id, "dataset_id": dataset_id}}
    ]

    # Add pre-aggregation filter if specified
    if query.filter:
        pipeline.append(_build_match_stage(query.filter))

    # Add group stage only if we have aggregations or group_by
    if query.aggregations is not None or query.group_by is not None:
        pipeline.append(_build_group_stage(query))

    # Add sort stage if specified
    if query.sort:
        pipeline.append(_build_sort_stage(query.sort))

    # Add limit stage if specified
    if query.limit:
        pipeline.append({"$limit": query.limit})

    return pipeline
