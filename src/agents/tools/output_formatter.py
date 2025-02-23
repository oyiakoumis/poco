import asyncio
import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import BaseTool
from langchain_core.prompts import ChatPromptTemplate
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from agents.models.components import FormattedResponse

FORMATTER_SYSTEM_MESSAGE = """
You are a specialized formatter that converts raw data into structured UI-friendly responses using the FormattedResponse tool ONCE with all components together. Your role is to analyze the content and user's original query to determine the most appropriate UI components for presenting the information.

IMPORTANT: You must create a SINGLE FormattedResponse that includes ALL necessary components at once. DO NOT try to create separate FormattedResponse objects for each component. If the response is not correctly formatted, you should fix the entire response and try again, not attempt to handle components individually.

The FormattedResponse tool allows you to create a response with an array of UI components. Each component in the response array must be one of these types:

1. Markdown Component
   {
     "type": "markdown",
     "content": "string with markdown syntax"
   }
   - Use for formatted text content
   - Supports standard markdown syntax
   - Great for descriptions, explanations, and text-heavy content

2. Checkbox Component
   {
     "type": "checkbox",
     "items": [
       {"label": "Task 1", "checked": true},
       {"label": "Task 2", "checked": false}
     ]
   }
   - Use for interactive lists with checked/unchecked states
   - Ideal for todo lists, multi-select options, or status tracking

3. Table Component
   {
     "type": "table",
     "headers": ["Column 1", "Column 2"],
     "rows": [
       ["Data 1", "Data 2"],
       ["Data 3", "Data 4"]
     ]
   }
   - Use for structured data with clear rows and columns
   - Perfect for displaying multiple records with consistent fields

4. Chart Component
   {
     "type": "chart",
     "chart_type": "bar" | "line" | "pie",
     "labels": ["Label 1", "Label 2"],
     "datasets": [
       {
         "label": "Dataset 1",
         "data": [1, 2]
       }
     ]
   }
   - Use for data visualization
   - Supports bar, line, and pie charts

Your response must be a valid FormattedResponse object with this structure:
{
  "response": [
    {component1},
    {component2},
    ...
  ]
}

Guidelines:
1. Choose components based on data structure and user intent:
   - Markdown for text explanations and formatting
   - Checkbox for interactive lists and selections
   - Table for structured, tabular data
   - Chart for numerical data that benefits from visualization

2. Component Combinations:
   - Combine components when needed (e.g., Markdown for explanation + Table for data)
   - Order components logically (e.g., explanation before visualization)
   - Use the most appropriate chart type for the data relationship

3. Data Formatting:
   - Format dates consistently
   - Use appropriate number formatting
   - Ensure proper escaping of special characters in markdown
   - Structure table data for clear readability

Remember:
- Create ONE FormattedResponse containing ALL components needed
- DO NOT create separate FormattedResponse objects for individual components
- Each component must strictly follow its type's schema
- The response must be a valid FormattedResponse with a list of components
- Consider the user's query context when choosing components
- Format data appropriately for each component type
- If the response is not valid, fix the entire response and try again
- DO NOT loop through components individually
- ONLY use the FormattedResponse tool ONCE to structure your complete response
"""


class OutputFormatterArgs(BaseModel):
    user_query: str = Field(description="Original user query that generated these results")
    content: Any = Field(description="Raw query results to format")


class OutputFormatterTool(BaseTool):
    """Tool for formatting query results into UI-friendly JSON responses."""

    name: str = "output_formatter"
    description: str = "Format query results into UI-friendly JSON responses with appropriate UI components"
    args_schema: type[BaseModel] = OutputFormatterArgs

    def _run(self, user_query: str, content: Any) -> FormattedResponse:
        return asyncio.run(self._arun(user_query, content))

    async def _arun(self, user_query: str, content: Any) -> FormattedResponse:
        """
        Format the content into a UI-friendly response structure.

        Args:
            user_query: Original user query that generated these results
            content: Raw query results to format

        Returns:
            Dict with response array containing UI components
        """
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

        runnable = create_react_agent(llm, [FormattedResponse], response_format=FormattedResponse.model_json_schema())
        response = await runnable.ainvoke(
            {
                "messages": [
                    SystemMessage(content=FORMATTER_SYSTEM_MESSAGE),
                    HumanMessage(content=f"User Query: {user_query}\n\nFormat this content into appropriate UI components: {content}"),
                ]
            }
        )

        return json.loads(response["messages"][-1].content)
