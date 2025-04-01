import asyncio

from langchain.schema import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from agents.assistant import ASSISTANT_SYSTEM_MESSAGE
from agents.graph import setup_graph
from agents.print_event import print_event


async def main():
    graph, client = await setup_graph()
    try:
        # Configuration for the graph
        config = RunnableConfig(
            configurable={"thread_id": "1", "user_id": "whatsapp:+971565312695", "time_zone": "UTC", "first_day_of_the_week": 0}, recursion_limit=25
        )

        messages = [HumanMessage(content="/new I am going to my grandma house. what can I bring to her? Check my database for ideas.")]

        for message in messages:
            print_event((), {"Human": {"messages": [message]}})

            async for namespace, event in graph.astream(
                {"messages": [SystemMessage(content=ASSISTANT_SYSTEM_MESSAGE), message]}, config, stream_mode="updates", subgraphs=True
            ):
                print_event(namespace, event)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
