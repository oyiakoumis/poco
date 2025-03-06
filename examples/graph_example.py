import asyncio
from langchain.schema import HumanMessage
from langchain_core.runnables import RunnableConfig

from agents.graph import setup_graph
from agents.print_event import print_event


async def main():
    graph, client = await setup_graph()
    try:
        # Configuration for the graph
        config = RunnableConfig(configurable={"thread_id": "1", "user_id": "user_123", "time_zone": "UTC", "first_day_of_the_week": 0}, recursion_limit=25)

        messages = [HumanMessage(content="Remove duplicates from my watch list")]

        for message in messages:
            input_messages = {"messages": [message]}
            print_event((), {"Human": input_messages})

            async for namespace, event in graph.astream(input_messages, config, stream_mode="updates", subgraphs=True):
                print_event(namespace, event)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
