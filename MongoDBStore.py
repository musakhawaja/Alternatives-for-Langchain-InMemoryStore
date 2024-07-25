from langchain.schema import Document
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
import logging
from typing import Generic, Iterator, Sequence, TypeVar
from langchain_core.stores import BaseStore
from typing import Optional
load_dotenv()

D = TypeVar("D", bound=Document)
logger = logging.getLogger(__name__)

class MongoDBFileStore(BaseStore[str, Document], Generic[D]):
    def __init__(self, connection_string: str, database_name: str, collection_name: str):
        self.client = MongoClient(connection_string)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
    
    def serialize_document(self, doc: Document) -> dict:
        return {"page_content": doc.page_content, "metadata": doc.metadata}

    def deserialize_document(self, value: dict) -> Document:
        return Document(page_content=value.get("page_content", ""), metadata=value.get("metadata", {}))

    def mset(self, key_value_pairs: Sequence[tuple[str, Document]]) -> None:
        documents_to_insert = []
        for key, document in key_value_pairs:
            try:
                if isinstance(document, Document):
                    serialized_doc = self.serialize_document(document)
                    documents_to_insert.append({"id": key, **serialized_doc})
                else:
                    logger.error(f"Expected Document, got {type(document)}")
            except Exception as e:
                logger.error(f"Error in mset: {e}")
        if documents_to_insert:
            try:
                self.collection.insert_many(documents_to_insert, ordered=False)
            except Exception as e:
                logger.error(f"Error inserting documents: {e}")

    def mget(self, keys: Sequence[str]) -> list[Document]:
        result = []
        try:
            query = {"id": {"$in": keys}}
            documents = self.collection.find(query)
            for doc in documents:
                doc_id = doc.pop("id")
                result.append(self.deserialize_document(doc))
        except Exception as e:
            logger.error(f"Error in mget: {e}")
        return result

    def mdelete(self, keys: Sequence[str]) -> None:
        try:
            query = {"id": {"$in": keys}}
            self.collection.delete_many(query)
        except Exception as e:
            logger.error(f"Error in mdelete: {e}")

    def yield_keys(self, *, prefix: Optional[str] = None) -> Iterator[str]:
        try:
            if prefix:
                query = {"id": {"$regex": f"^{prefix}"}}
            else:
                query = {}
            for doc in self.collection.find(query, {"id": 1}):
                yield doc["id"]
        except Exception as e:
            logger.error(f"Error in yield_keys: {e}")

    