from typing import Optional

from langchain_core.tools import tool
from pydantic import BaseModel, Field


class HelpRequestInput(BaseModel):
    """Input schema for the help information tool."""

    topic: Optional[str] = Field(default=None, description="Specific help topic to get information about. If None, returns general help information.")


@tool(args_schema=HelpRequestInput)
def get_help_information(self, topic: Optional[str] = None) -> str:
    """Return help information as formatted text based on the requested topic."""

    general_help = """
*Hey there! 👋 I'm your AI sidekick!*

I help you organize and manage your information with a personal touch. Think of me as your digital memory and assistant rolled into one!

*What I can do:*
🔍 Find & organize anything you share with me
📱 Process images you send (under 5MB)
🧠 Remember everything important for you

*Quick examples:*
- "Remember John's email is john@example.com"
- "What meetings do I have tomorrow?"
- "Find my notes about the marketing campaign"

Need more details? Ask about "capabilities", "usage", or "media"!
"""

    capabilities_help = """
*My Superpowers* 💪

🗄️ *Smart Storage* - Custom databases for your information
🔎 *Intelligent Search* - I understand what you're looking for
📊 *Data Management* - Track, update, organize everything
📱 *Image Handling* - Work with the photos you send
"""

    usage_help = """
*How to Talk to Me* 🗣️

Just chat naturally! Try phrases like:
- "Add a meeting with Alex tomorrow at 3pm"
- "What was that website Sarah mentioned?"
- "Find all my notes about project Phoenix"
"""

    media_help = """
*Working with Images* 📸

*Quick Guide:*
- ✅ Formats: PNG, JPEG/JPG, GIF (non-animated)
- ✅ Size: Under 5MB per image
- ✅ Limit: 1 image per message

Just attach your image and tell me what it's about!
"""

    # Map topics to help text
    help_topics = {
        "general": general_help,
        "capabilities": capabilities_help,
        "usage": usage_help,
        "media": media_help,
    }

    # Return specific topic or general help
    if topic and topic in help_topics:
        return help_topics[topic]
    return general_help
