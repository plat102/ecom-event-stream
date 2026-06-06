from __future__ import annotations
from pymongo import MongoClient as PyMongoClient
from pymongo.collection import Collection


class MongoDBClient:
    def __init__(self, uri: str, db_name: str) -> None:
        self._client = PyMongoClient(uri)
        self._db = self._client[db_name]

    def get_collection(self, name: str) -> Collection:
        return self._db[name]

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> MongoDBClient:
        return self

    def __exit__(self, *_) -> None:
        self.close()


def make_mongo_client(settings) -> MongoDBClient:
    """Factory uses settings.MONGO_URI and settings.MONGO_DB."""
    return MongoDBClient(uri=settings.MONGO_URI, db_name=settings.MONGO_DB)
