from typing import Set

from dotenv import load_dotenv
from langchain.schema import HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.runnables import RunnableConfig

from database_connector import DatabaseConnector
from graph import get_graph, get_query_processor_graph
from print_event import print_event

load_dotenv()


def main() -> None:
    database_connector = DatabaseConnector("mongodb://localhost:27017", "test_database")

    query_processor_graph = get_query_processor_graph(database_connector)
    graph = get_graph(query_processor_graph)
    memory = MemorySaver()
    graph = graph.compile(checkpointer=memory)

    # Configuration for the graph
    config = RunnableConfig(configurable={"thread_id": "1"}, recursion_limit=10)

    # Define graph's input
    initial_message = HumanMessage(content="Delete my tasks from last monday.")

    # Process and print each event
    for namespace, event in graph.stream({"messages": [initial_message]}, config, stream_mode="updates", subgraphs=True):
        print_event(namespace, event)


if __name__ == "__main__":
    main()
