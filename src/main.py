from langchain.schema import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver

from constants import DATABASE_CONNECTION_STRING
from database_connector import DatabaseConnector
from graph import get_graph
from print_event import print_event, print_message


def main() -> None:
    db_connector = DatabaseConnector(DATABASE_CONNECTION_STRING, "task_manager")

    graph = get_graph(db_connector)
    graph = graph.compile(checkpointer=MemorySaver())

    # Configuration for the graph
    config = RunnableConfig(configurable={"thread_id": "1"}, recursion_limit=10)

    # messages = [HumanMessage(content="Create a table to store my grocery list."), HumanMessage(content="Improvise")]
    messages = [HumanMessage(content="Add apple to my list of groceries.")]

    for message in messages:
        print_message(message, "Human")
        # Process and print each event
        for namespace, event in graph.stream({"messages": [message]}, config, stream_mode="updates", subgraphs=True):
            print_event(namespace, event)


if __name__ == "__main__":
    main()
