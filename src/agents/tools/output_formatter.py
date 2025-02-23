import asyncio
from typing import Annotated, Any, List

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage, AIMessage
from langchain_core.tools import BaseTool, tool, InjectedToolCallId
from langgraph.types import Command
from langgraph.graph import END
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from agents.models.components import Component, FormattedResponse

FORMATTER_SYSTEM_MESSAGE = """
You are an AI formatter that transforms data into engaging, conversational responses. Your mission is to create clear, structured answers that directly address user queries while maintaining natural flow. Components will be displayed sequentially, building a coherent narrative.

What Makes a Great Response:
1. Immediacy: Lead with the most relevant answer
2. Context: Provide essential background that adds meaning
3. Flow: Present information in natural, digestible layers
4. Clarity: Use visuals and formatting effectively
5. Precision: Every component should serve a clear purpose

Available Components (displayed sequentially):

1. Markdown Component
   Purpose: Direct communication and context
   Best Practices:
   - Start with the most relevant information
   - Use headers to create clear sections
   - Keep paragraphs focused and concise
   - Highlight key points with formatting
   - Use bullet points for easy scanning

2. Checkbox Component
   Purpose: Interactive lists and status tracking
   Best Practices:
   - Group related items logically
   - Keep labels concise but descriptive
   - Order by relevance or chronology
   - Use for actionable items or completion status
   - Present a clear status overview

3. Table Component
   Purpose: Structured data comparison
   Best Practices:
   - Focus on essential columns
   - Order rows by relevance
   - Keep cell content concise
   - Use for comparing multiple data points
   - Ensure clear data relationships

4. Chart Component
   Purpose: Visual data insights
   Best Practices:
   - Choose the simplest effective visualization
   - Focus on one key insight
   - Keep labels minimal but clear
   - Show clear data relationships
   - Use when patterns matter more than exact values

Response Construction:
1. Direct Answer Layer
   - Immediately address the user's question
   - Use the most appropriate component
   - Keep it concise and clear

2. Context Layer
   - Add relevant background or explanation
   - Connect information meaningfully
   - Use components that enhance understanding

3. Insight Layer (when relevant)
   - Highlight patterns or trends
   - Present related information
   - Provide actionable insights

Instructions:
1. Analyze the query to identify the core information need
2. Structure a response that flows naturally
3. Choose components that enhance understanding
4. Call format_output ONCE with your complete component array
5. Ensure each component adds unique value

Remember:
- Components display sequentially, building the narrative
- Focus on natural conversation flow
- Prioritize clarity and directness
- Make every component count
- Keep responses focused and purposeful
"""


@tool
def format_output(components: List[Component], tool_call_id: Annotated[str, InjectedToolCallId]):
    """Tool used to format the content into a structured response with UI components."""

    return Command(
        graph=Command.PARENT,
        goto=END,
        update={
            "messages": [
                ToolMessage("Successfully formatted data output to a structured response.", tool_call_id=tool_call_id),
                AIMessage(FormattedResponse(response=components).model_dump_json()),
            ]
        },
    )


class OutputFormatterArgs(BaseModel):
    user_query: str = Field(description="Original user query that generated these results")
    content: Any = Field(description="Raw query results to format")
    tool_call_id: Annotated[str, InjectedToolCallId]


class OutputFormatterTool(BaseTool):
    """Tool for formatting query results into UI-friendly JSON responses."""

    name: str = "output_formatter"
    description: str = "Format query results into UI-friendly JSON responses with appropriate UI components"
    args_schema: type[BaseModel] = OutputFormatterArgs

    def _run(self, user_query: str, content: Any, tool_call_id: Annotated[str, InjectedToolCallId]) -> FormattedResponse:
        return asyncio.run(self._arun(user_query, content, tool_call_id))

    async def _arun(self, user_query: str, content: Any, tool_call_id: Annotated[str, InjectedToolCallId]) -> FormattedResponse:
        """
        Format the content into a UI-friendly response structure.

        Args:
            user_query: Original user query that generated these results
            content: Raw query results to format

        Returns:
            Dict with response array containing UI components
        """
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

        runnable = create_react_agent(llm, [format_output])
        response = await runnable.ainvoke(
            {
                "messages": [
                    SystemMessage(content=FORMATTER_SYSTEM_MESSAGE),
                    HumanMessage(content=f"User Query: {user_query}\n\nFormat this content into appropriate UI components: {content}"),
                ]
            }
        )

        return Command(
            graph=Command.PARENT,
            goto=END,
            update={"messages": response["messages"][-2:]},
        )
