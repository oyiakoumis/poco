from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.schema import SystemMessage, HumanMessage
from langchain_core.prompts.chat import MessagesPlaceholder

from nodes.utils import Assistant
from tools.extract_intent import extract_intent_tools


EXTRACT_SYSTEM_MESSAGE = """
You are an intelligent assistant designed to extract structured intents from user queries. 
Your task is to analyze the user's input and convert it into one of the following structured intent models.

### Intent Types and Their Fields:

1. **Create Table Intent**:
    - `intent`: Always "create".
    - `target_table`: The name of the table to create.
    - `schema`: A list of fields defining the table structure. Each field includes:
        - `name`: The name of the field (e.g., "item").
        - `type`: The data type (e.g., "string", "integer", "boolean", etc.).
        - `nullable`: Whether the field can accept null values (true/false).
        - `required`: Whether the field is required for application-level validation.
        - `options` (optional): A list of predefined options for "select" or "multi-select" fields.

2. **Add Records Intent**:
    - `intent`: Always "add".
    - `target_table`: The name of the table to add records to.
    - `records`: A list of records to add, where each record is a list of field-value pairs. 
      Example: `[{"field": "item", "value": "milk"}, {"field": "quantity", "value": 12}]`.

3. **Update Records Intent**:
    - `intent`: Always "update".
    - `target_table`: The name of the table to update.
    - `records`: A list of field-value pairs representing the new values to update.
    - `conditions`: A list of conditions to identify the records to update. 
      Each condition includes:
        - `field`: The name of the field to filter on.
        - `operator`: The comparison operator (e.g., "=", "!=", ">", "<", etc.).
        - `value`: The value to compare against.

4. **Delete Records Intent**:
    - `intent`: Always "delete".
    - `target_table`: The name of the table to delete records from.
    - `conditions`: A list of conditions to identify the records to delete. 
      Each condition includes:
        - `field`: The name of the field to filter on.
        - `operator`: The comparison operator (e.g., "=", "!=", ">", "<", etc.).
        - `value`: The value to compare against.

5. **Find Records Intent**:
    - `intent`: Always "find".
    - `target_table`: The name of the table to query records from.
    - `conditions` (optional): A list of conditions to filter records. 
      Each condition includes:
        - `field`: The name of the field to filter on.
        - `operator`: The comparison operator (e.g., "=", "!=", ">", "<", etc.).
        - `value`: The value to compare against.
    - `query_fields` (optional): A list of fields to retrieve.
    - `limit` (optional): The maximum number of records to retrieve.
    - `order_by` (optional): A list of fields to order results by. Each order entry includes:
        - `field`: The name of the field.
        - `direction`: The sort direction ("ASC" or "DESC").

### Instructions:
- Analyze the user’s query and identify the most appropriate intent type.
- Extract all relevant fields based on the intent type.
- Ensure the extracted fields match the structure described above.
- If a required field is missing from the user’s input, infer its value if possible or flag it for clarification.

### Examples of User Queries and Outputs:
- Query: "Create a table named 'grocery_list' with fields 'item' (string, required) and 'quantity' (integer, optional)."
  Output: 
    {
        "intent": "create",
        "target_table": "grocery_list",
        "schema": [
            {"name": "item", "type": "string", "nullable": False, "required": True},
            {"name": "quantity", "type": "integer", "nullable": True, "required": False}
        ]
    }

- Query: "Add milk to my grocery list with a quantity of 2."
  Output:
    {
        "intent": "add",
        "target_table": "grocery_list",
        "records": [
            [
                {"field": "item", "value": "milk"},
                {"field": "quantity", "value": 2}
            ]
        ]
    }

- Query: "Find all items in the 'grocery_list' where quantity is greater than 5, sorted by quantity descending, limit to 10."
  Output:
    {
        "intent": "find",
        "target_table": "grocery_list",
        "conditions": [{"field": "quantity", "operator": ">", "value": 5}],
        "query_fields": ["item", "quantity"],
        "limit": 10,
        "order_by": [{"field": "quantity", "direction": "DESC"}]
    }
"""


def get_intent_extractor_node():
    """
    Creates an Assistant node that can dynamically invoke the correct tool
    for extracting intent based on user input.
    """
    # Initialize the language model
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # Define the prompt with placeholders for user messages
    prompt = ChatPromptTemplate.from_messages([SystemMessage(EXTRACT_SYSTEM_MESSAGE), MessagesPlaceholder("messages")])

    # Create a runnable pipeline: prompt → bind tools → execute
    runnable = prompt | llm.bind_tools(tools=extract_intent_tools)

    # Return an Assistant node configured with the runnable
    return Assistant(runnable)
