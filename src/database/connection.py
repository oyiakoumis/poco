from pymongo import MongoClient


class Connection:
    def __init__(self, host: str, port: int, username: str = None, password: str = None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None

    def connect(self) -> None:
        connection_string = f"mongodb://"
        if self.username and self.password:
            connection_string += f"{self.username}:{self.password}@"
        connection_string += f"{self.host}:{self.port}"

        self.client = MongoClient(connection_string)
        # Test connection
        self.client.server_info()

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def is_connected(self) -> bool:
        return self.client is not None
