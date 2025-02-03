from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from database_manager.query import AggregateFn
from models.base import BaseCollectionOperation
from models.fields import ConditionModel, OrderByModel


class AggregateField(BaseModel):
    """Model for defining aggregation operations on fields."""

    field: str = Field(description="The field to aggregate")
    function: AggregateFn = Field(description="The aggregate function to apply")
    alias: Optional[str] = Field(default=None, description="Optional alias for the aggregated result")

    @field_validator("field")
    def validate_field_for_function(cls, v: str, values: dict) -> str:
        """Validate that the field is appropriate for the chosen aggregate function."""
        function = values.get("function")
        if function == AggregateFn.COUNT:
            # COUNT can be applied to any field
            return v
        # Other validations could be added here if needed
        return v

    @field_validator("alias")
    def set_default_alias(cls, v: Optional[str], values: dict) -> str:
        """Set a default alias if none is provided."""
        if v is None:
            field = values.get("field", "")
            function = values.get("function", AggregateFn.COUNT)
            return f"{function.value}_{field}"
        return v


class GroupByModel(BaseModel):
    """Model for grouping operations in queries."""

    fields: List[str] = Field(min_items=1, description="The fields to group by")
    aggregates: List[AggregateField] = Field(min_items=1, description="The aggregate operations to perform")

    @model_validator(mode="after")
    def validate_group_by(self) -> "GroupByModel":
        """Validate that the grouping configuration is valid."""
        # Ensure no duplicate aggregate aliases
        aliases = [agg.alias for agg in self.aggregates]
        if len(aliases) != len(set(aliases)):
            raise ValueError("Duplicate aggregate aliases are not allowed")
        return self


class QueryDocumentsModel(BaseCollectionOperation):
    """Model for querying documents operation."""

    intent: Literal["query"]
    conditions: List[ConditionModel] = Field(default_factory=list)
    query_fields: Optional[List[str]] = None
    limit: Optional[int] = Field(default=None, gt=0)
    order_by: Optional[List[OrderByModel]] = None
    group_by: Optional[GroupByModel] = None

    @model_validator(mode="after")
    def validate_query_configuration(self) -> "QueryDocumentsModel":
        """Validate the overall query configuration."""
        if self.group_by:
            # When using group_by, query_fields should only contain grouped fields
            if self.query_fields:
                invalid_fields = [field for field in self.query_fields if field not in self.group_by.fields]
                if invalid_fields:
                    raise ValueError("When using group_by, query_fields must only contain " f"grouped fields. Invalid fields: {invalid_fields}")

            # When using group_by, order_by should only reference grouped fields
            # or aggregate aliases
            if self.order_by:
                valid_order_fields = set(self.group_by.fields)
                valid_order_fields.update(agg.alias for agg in self.group_by.aggregates)
                invalid_order_fields = [order.field for order in self.order_by if order.field not in valid_order_fields]
                if invalid_order_fields:
                    raise ValueError(
                        "When using group_by, order_by must only reference grouped " f"fields or aggregate aliases. Invalid fields: {invalid_order_fields}"
                    )

        return self
