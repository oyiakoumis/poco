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

        human_messages = [HumanMessage(content="I did 1 hour of Football today.")]

        is_first_message = True
        for message in human_messages:
            print_event((), {"Human": {"messages": [message]}})

            messages = [message]
            if is_first_message:
                messages = [SystemMessage(content=ASSISTANT_SYSTEM_MESSAGE), message]
                is_first_message = False

            async for namespace, event in graph.astream({"messages": messages}, config, stream_mode="updates", subgraphs=True):
                print_event(namespace, event)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
