import os

from opensearchpy import OpenSearch
from dotenv import load_dotenv

from logger import get_json_logger

logger = get_json_logger("OpenSearchClient")

load_dotenv(".env")
OS_HOST = os.getenv("OS_HOST", "localhost")
OS_PORT = int(os.getenv("OS_PORT", "9200"))
OS_USER = os.getenv("OS_USER", "admin")
OS_PSWD = os.getenv("OS_PSWD")
OS_SSL = os.getenv("OS_SSL", "true").lower() == "true"
OS_CERT = os.getenv("OS_CERT", "false").lower() == "true"


class OpenSearchClient:
    def __init__(self):
        self.client = OpenSearch(
            hosts=[{"host": OS_HOST, "port": OS_PORT}],
            http_auth=(OS_USER, OS_PSWD),
            use_ssl=OS_SSL,
            verify_certs=OS_CERT,
            ssl_show_warn=False,
        )

    def insert_doc(self, index, doc):
        try:
            res = self.client.index(index=index, body=doc)
            self.client.indices.refresh(index=index)
        except Exception as e:
            logger.error(f"Failed to insert document to index '{index}': {e}")

    def list_index(self) -> list[str]:
        try:
            indices = self.client.indices.get_alias(index="*").keys()
            return list(indices)
        except Exception as e:
            logger.error(f"Failed to list indices: {e}")
            return None

    def create_index(self, index, config):
        try:
            self.client.indices.create(index=index, body=config)
        except Exception as e:
            print(f"Failed to create index '{index}': {e}")

    def list_docs(self, index, size=10):
        """List documents in index"""
        try:
            res = self.client.search(
                index=index, body={"query": {"match_all": {}}}, size=size
            )
            return res["hits"]["hits"]
        except Exception as e:
            logger.error(f"Failed to list documents from index '{index}': {e}")
            return None

    def search_docs(self, index, keyword, field="content", size=10):
        """Search keyword in index"""
        try:
            res = self.client.search(
                index=index, body={"query": {"match": {field: keyword}}}, size=size
            )
            return res["hits"]["hits"]
        except Exception as e:
            logger.error(f"Failed to search in index '{index}': {e}")
            return None

    def delete_doc(self, index, doc_id):
        """Delete a document by ID"""
        try:
            res = self.client.delete(index=index, id=doc_id)
            return res
        except Exception as e:
            logger.error(
                f"Failed to delete document {doc_id} from index '{index}': {e}"
            )
            return None


if __name__ == "__main__":
    import random

    client = OpenSearchClient()
    indices = client.list_index()
    print(f"Indices:", indices, "\n")

    index = "test"
    config = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "content": {"type": "text"},
            }
        }
    }
    if index not in indices:
        client.create_index(index, config)
        print(f"Index {index} created!\n")

        indices = client.list_index()
        print(f"Indices:", indices, "\n")

    title = f"title-{random.randint(0, 1<<30)}"
    doc = {"title": title, "context": "demo"}
    client.insert_doc(index, doc)
    print(f"Document {title} is inserted")
