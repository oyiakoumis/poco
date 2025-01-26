from models.extract_intent import (
    AddRecordsModel,
    CreateTableModel,
    DeleteRecordsModel,
    QueryRecordsModel,
    UpdateRecordsModel,
)
from tools.extract_intent import (
    extract_add_intent,
    extract_create_intent,
    extract_delete_intent,
    extract_find_intent,
    extract_update_intent,
)


def test_extract_create_intent():
    user_input = "Create a table named 'grocery_list' with fields 'item' (string, required) and 'quantity' (integer, optional)."
    result = extract_create_intent(user_input)

    assert isinstance(result, CreateTableModel)
    assert result.target_table == "grocery_list"
    assert len(result.table_schema) == 2
    assert result.table_schema[0].name == "item"
    assert result.table_schema[0].type == "string"
    assert result.table_schema[0].nullable is False
    assert result.table_schema[1].name == "quantity"
    assert result.table_schema[1].type == "integer"
    assert result.table_schema[1].nullable is True


def test_extract_add_intent():
    user_input = "Add milk and bread to my grocery list with quantities 1 and 2 respectively."
    result = extract_add_intent(user_input)

    assert isinstance(result, AddRecordsModel)
    assert result.target_table == "grocery_list"
    assert len(result.records) == 2
    assert result.records[0][0].field == "item"
    assert result.records[0][0].value == "milk"
    assert result.records[0][1].field == "quantity"
    assert result.records[0][1].value == 1
    assert result.records[1][0].field == "item"
    assert result.records[1][0].value == "bread"
    assert result.records[1][1].field == "quantity"
    assert result.records[1][1].value == 2


def test_extract_update_intent():
    user_input = "Update the quantity of milk in the grocery list to 3."
    result = extract_update_intent(user_input)

    assert isinstance(result, UpdateRecordsModel)
    assert result.target_table == "grocery_list"
    assert len(result.records) == 1
    assert result.records[0].field == "quantity"
    assert result.records[0].value == 3
    assert len(result.conditions) == 1
    assert result.conditions[0].field == "item"
    assert result.conditions[0].operator == "="
    assert result.conditions[0].value == "milk"


def test_extract_delete_intent():
    user_input = "Delete all records from my grocery list where the item is milk."
    result = extract_delete_intent(user_input)

    assert isinstance(result, DeleteRecordsModel)
    assert result.target_table == "grocery_list"
    assert len(result.conditions) == 1
    assert result.conditions[0].field == "item"
    assert result.conditions[0].operator == "="
    assert result.conditions[0].value == "milk"


def test_extract_find_intent():
    user_input = "Find all products and their quantities in my grocery list where the quantity is greater than 5, sorted by quantity in descending order, limited to 10 results."
    result = extract_find_intent(user_input)

    assert isinstance(result, QueryRecordsModel)
    assert result.target_table == "grocery_list"
    assert len(result.conditions) == 1
    assert result.conditions[0].field == "quantity"
    assert result.conditions[0].operator == ">"
    assert result.conditions[0].value == 5
    assert result.query_fields == ["item", "quantity"]
    assert result.limit == 10
    assert len(result.order_by) == 1
    assert result.order_by[0].field == "quantity"
    assert result.order_by[0].direction == "DESC"
