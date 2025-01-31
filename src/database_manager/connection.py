from pymongo import MongoClient


class Connection:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def connect(self) -> None:
        self.client = MongoClient(self.connection_string)
        # Test connection
        self.client.server_info()

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def is_connected(self) -> bool:
        return self.client is not None
