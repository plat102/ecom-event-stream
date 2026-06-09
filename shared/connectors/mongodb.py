from __future__ import annotations
from urllib.parse import quote_plus

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
    uri = (
        f"mongodb://{quote_plus(settings.MONGO_USER)}"
        f":{quote_plus(settings.MONGO_PASSWORD)}"
        f"@{settings.MONGO_HOST}"
    )
    return MongoDBClient(uri=uri, db_name=settings.MONGO_DB)
