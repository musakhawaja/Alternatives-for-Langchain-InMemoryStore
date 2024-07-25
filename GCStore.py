from langchain.schema import Document
from dotenv import load_dotenv
import logging
from typing import Generic, Iterator, Sequence, TypeVar
from langchain_core.stores import BaseStore
from typing import Optional
from google.cloud import storage
import json

load_dotenv()

D = TypeVar("D", bound=Document)
logger = logging.getLogger(__name__)


class GCSFileStore(BaseStore[str, Document], Generic[D]):
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
    
    def read(self, file_path):
        blob = self.bucket.blob(file_path)
        file_content = blob.download_as_bytes()
        return file_content  
    
    def write(self, file_path, data):
        blob = self.bucket.blob(file_path)
        if isinstance(data, str):
            data = data.encode('utf-8')  
        blob.upload_from_string(data)
    
    def exists(self, file_path):
        blob = self.bucket.blob(file_path)
        return blob.exists()

    def serialize_document(self, doc: Document) -> dict:
        return {"page_content": doc.page_content, "metadata": doc.metadata}

    def deserialize_document(self, value: dict) -> Document:
        return Document(page_content=value.get("page_content", ""), metadata=value.get("metadata", {}))

    def mset(self, key_value_pairs: Sequence[tuple[str, Document]]) -> None:
        for key, document in key_value_pairs:
            try:
                if isinstance(document, Document):
                    serialized_doc = self.serialize_document(document)
                    serialized_doc_str = json.dumps(serialized_doc).encode('utf-8')  
                    self.write(key, serialized_doc_str)
                else:
                    logger.error(f"Expected Document, got {type(document)}")
            except Exception as e:
                logger.error(f"Error in mset: {e}")

    def mget(self, keys: Sequence[str]) -> list[Document]:
        result = []
        for key in keys:
            try:
                if self.exists(key):
                    value = self.read(key)
                    json_data = json.loads(value.decode('utf-8'))  
                    doc = self.deserialize_document(json_data)
                    result.append(doc)
            except Exception as e:
                logger.error(f"Error in mget: {e}")
        return result

    def mdelete(self, keys: Sequence[str]) -> None:
        for key in keys:
            try:
                blob = self.bucket.blob(key)
                blob.delete()
            except Exception as e:
                logger.error(f"Error in mdelete: {e}")

    def yield_keys(self, *, prefix: Optional[str] = None) -> Iterator[str]:
        try:
            blobs = self.bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                yield blob.name
        except Exception as e:
            logger.error(f"Error in yield_keys: {e}")
