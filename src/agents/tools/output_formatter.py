import asyncio
from typing import Annotated, Any, List

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool, InjectedToolCallId, tool
from langchain_openai import ChatOpenAI
from langgraph.graph import END
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command
from pydantic import BaseModel, Field

from agents.models.components import Component, FormattedResponse

FORMATTER_SYSTEM_MESSAGE = """
You are an AI formatter that transforms data into engaging, conversational responses. Your mission is to create clear, structured answers that directly address user queries while maintaining natural flow. Components will be displayed sequentially, building a coherent narrative.

What Makes a Great Response:
1. Immediacy: Lead with the most relevant answer
2. Flow: Present information in natural, digestible layers
3. Clarity: Use visuals and formatting effectively
4. Precision: Every component should serve a clear purpose

Available Components (displayed sequentially):

1. Markdown: For text content and context
   - Lead with key information
   - Use clear headers and formatting
   - Keep content focused and scannable

2. Checkbox: For interactive lists and tracking
   - Group items logically
   - Use clear, concise labels
   - Show status effectively

3. Table: For structured data comparison
   - Focus on essential data
   - Order by relevance
   - Keep relationships clear

4. Chart: For visual insights
   - Choose simple, effective visualizations
   - Focus on key patterns
   - Keep labels clear and minimal

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
def output_formatter(components: List[Component], tool_call_id: Annotated[str, InjectedToolCallId]):
    "Format query results into UI-friendly JSON responses with appropriate UI components"

    return Command(
        graph=Command.PARENT,
        goto=END,
        update={
            "messages": [
                ToolMessage(FormattedResponse(response=components).model_dump_json(), tool_call_id=tool_call_id),
            ]
        },
    )
