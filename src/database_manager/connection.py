from typing import Optional

from pymongo import MongoClient


class Connection:
    """
    Handles connection to the MongoDB database.
    """

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string
        self.client: Optional[MongoClient] = None

    def connect(self) -> None:
        """
        Establish a connection to MongoDB.
        """
        self.client = MongoClient(self.connection_string)
        # Test connection
        self.client.server_info()

    def disconnect(self) -> None:
        """
        Disconnect from MongoDB.
        """
        if self.client:
            self.client.close()
            self.client = None

    def is_connected(self) -> bool:
        """
        Check if the connection to MongoDB is active.
        """
        return self.client is not None
