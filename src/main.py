from langchain.schema import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import MemorySaver

from constants import DATABASE_CONNECTION_STRING
from print_event import print_event, print_message


def main() -> None:
    from database_manager.database import Database
    from database_manager.connection import Connection

    connection = Connection()

    database = Database("task_manager", )


if __name__ == "__main__":
    main()
