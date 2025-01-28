from typing import Annotated, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field


class OrderByModel(BaseModel):
    field: str = Field(description="The name of the field to order by.")
    direction: Literal["ASC", "DESC"] = Field(description="The direction to order the results by. Use 'ASC' for ascending or 'DESC' for descending.")


class TableSchemaField(BaseModel):
    name: str = Field(description="The name of the column/field in the table.")
    type: Literal["string", "integer", "boolean", "float", "date", "datetime", "select", "multi-select"] = Field(
        description=(
            "The data type of the field. Supported types include: 'string', 'integer', 'boolean', 'float', 'date', 'datetime', 'select', and 'multi-select'."
        )
    )
    nullable: Optional[bool] = Field(description="Whether the field can accept null values. If True, the field is nullable. If False, the field is required.")
    required: Optional[bool] = Field(description="Whether this field must be provided when adding records. This is for application-level validation.")
    options: Optional[List[str]] = Field(
        default=None,
        description=(
            "A list of predefined options for 'select' or 'multi-select' field types. "
            "For example: ['Option 1', 'Option 2', 'Option 3']. Ignored for other field types."
        ),
    )


class IndexField(BaseModel):
    field_name: str
    order: Literal["-1", "1", "text"]


class IndexDefinition(BaseModel):
    fields: List[IndexField] = Field(description=("The fields to include in the index."))
    unique: Optional[bool] = Field(default=False, description="Whether the index should enforce unique values.")


class RecordModel(BaseModel):
    field: str = Field(description="The name of the field to set or update.")
    value: Union[str, int, float, bool] = Field(description="The value to assign to the field.")


class ConditionModel(BaseModel):
    field: str = Field(description="The name of the field to filter by.")
    operator: Literal["=", "!=", ">", "<", ">=", "<=", "contains"] = Field(
        description="The operator to use for the condition, such as '=', '!=', '>', '<', etc."
    )
    value: Union[str, int, float, bool] = Field(description="The value to compare the field against.")


# Separate models for each intent
class CreateTableModel(BaseModel):
    intent: Literal["create_table"]
    target_table: str = Field(description="The name of the table to create.")
    table_schema: List[TableSchemaField] = Field(
        description=(
            "The schema of the table to create, represented as a list of fields with their names, types, nullable status, and whether they are required."
        )
    )
    indexes: Optional[List[IndexDefinition]] = Field(
        default=None,
        description=("A list of indexes to create for the table. Each index specifies the fields to include, their sort order, and whether it is unique."),
    )

    class Config:
        json_schema_extra = {
            "example": {
                "intent": "create_table",
                "target_table": "grocery_list",
                "table_schema": [
                    {
                        "name": "item",
                        "type": "string",
                        "nullable": False,
                        "required": True,
                    },
                    {
                        "name": "quantity",
                        "type": "integer",
                        "nullable": True,
                        "required": False,
                    },
                    {
                        "name": "tags",
                        "type": "multi-select",
                        "nullable": True,
                        "required": False,
                        "options": ["Organic", "Local"],
                    },
                ],
                "indexes": [
                    {"fields": ["item"], "unique": True},
                    {"fields": [("tags", 1)], "unique": False},
                    {"fields": [("quantity", -1)], "unique": False},
                    {"fields": [("item", "text")], "unique": False},
                ],
            }
        }


class AddRecordsModel(BaseModel):
    intent: Literal["add"]
    target_table: str = Field(description="The name of the table to add records to.")
    records: List[List[RecordModel]] = Field(
        description=(
            "A list of records to add. Each record is represented as a list of field-value pairs. "
            "For example: [[{'field': 'item', 'value': 'milk'}, {'field': 'quantity', 'value': 12}]]."
        )
    )

    class Config:
        json_schema_extra = {
            "example": {
                "target_table": "grocery_list",
                "records": [
                    [
                        {"field": "item", "value": "milk"},
                        {"field": "quantity", "value": 12},
                    ],
                    [
                        {"field": "item", "value": "bread"},
                        {"field": "quantity", "value": 2},
                    ],
                ],
            }
        }


class UpdateRecordsModel(BaseModel):
    intent: Literal["update"]
    target_table: str = Field(description="The name of the table to update records in.")
    records: List[RecordModel] = Field(
        description="A list of field-value pairs representing the new values for the record(s). " "For example: [{'field': 'quantity', 'value': 18}].",
    )
    conditions: List[ConditionModel] = Field(
        default_factory=list,
        description=(
            "A list of conditions to identify the records to update. Each condition specifies a field, operator, and value. If empty, all records will be updated. "
            "For example: [{'field': 'item', 'operator': '=', 'value': 'milk'}]."
        ),
    )

    class Config:
        json_schema_extra = {
            "example": {
                "target_table": "grocery_list",
                "records": [{"field": "quantity", "value": 18}],
                "conditions": [{"field": "item", "operator": "=", "value": "milk"}],
            }
        }


class DeleteRecordsModel(BaseModel):
    intent: Literal["delete"]
    target_table: str = Field(description="The name of the table to delete records from.")
    conditions: List[ConditionModel] = Field(
        default_factory=list,
        description=(
            "A list of conditions to identify the records to delete. Each condition specifies a field, operator, and value. If empty, all records will be deleted. "
            "For example: [{'field': 'item', 'operator': '=', 'value': 'milk'}]."
        ),
    )

    class Config:
        json_schema_extra = {
            "example": {
                "target_table": "grocery_list",
                "conditions": [{"field": "item", "operator": "=", "value": "milk"}],
            }
        }


class QueryRecordsModel(BaseModel):
    intent: Literal["query"]
    target_table: str = Field(description="The name of the table to query records from.")
    conditions: List[ConditionModel] = Field(
        default_factory=list,
        description=(
            "A list of conditions to filter the records. Each condition specifies a field, operator, and value. If empty, all records will be queried. "
            "For example: [{'field': 'quantity', 'operator': '>', 'value': 5}]."
        ),
    )
    query_fields: Optional[List[str]] = Field(default=None, description="A list of fields to retrieve. If not specified, all fields will be retrieved.")
    limit: Optional[int] = Field(default=None, description="The maximum number of records to retrieve.")
    order_by: Optional[List[OrderByModel]] = Field(
        default=None,
        description=(
            "A list of fields to order the results by, with their corresponding directions. "
            "Each entry specifies the field name and the sort direction (ASC or DESC). "
            "For example: [{'field': 'quantity', 'direction': 'DESC'}]."
        ),
    )

    class Config:
        json_schema_extra = {
            "example": {
                "target_table": "grocery_list",
                "conditions": [{"field": "quantity", "operator": ">", "value": 5}],
                "query_fields": ["item", "quantity"],
                "limit": 10,
                "order_by": [{"field": "quantity", "direction": "DESC"}],
            }
        }


class IntentModel(BaseModel):
    intent: Literal["create_table", "add", "update", "delete", "query"]
    details: Annotated[
        Union[CreateTableModel, AddRecordsModel, UpdateRecordsModel, DeleteRecordsModel, QueryRecordsModel],
        Field(discriminator="intent", description="structured intent model for database operations"),
    ]
