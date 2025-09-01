import os
from typing import List

import requests
from dotenv import load_dotenv

from os_client import OpenSearchClient

load_dotenv(".env")
INDEX = os.getenv("KNN_INDEX", "knn")
OPENAI_URL = os.getenv("OPENAI_URL")
OPENAI_KEY = os.getenv("OPENAI_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")

EMBEDDING_DIM = os.getenv("EMBEDDING_DIM")
INDEX_CONFIG = {
    "settings": {"index.knn": True},
    "mappings": {
        "properties": {
            "file_path": {"type": "text"},
            "text": {"type": "text"},
            "embedding": {
                "type": "knn_vector",
                "dimension": EMBEDDING_DIM,
                "space_type": "l2",
                "mode": "on_disk",
                "method": {"name": "hnsw"},
            },
        }
    },
}


class OpenAIEmbedding:
    def __init__(self):
        self.url = OPENAI_URL
        self.key = OPENAI_KEY
        self.model = OPENAI_MODEL

    def embedding(self, text):
        payload = {"model": self.model, "input": text}
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.key}",
        }
        resp = requests.post(self.url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        emb = data["data"][0]["embedding"]
        if len(emb) != EMBEDDING_DIM:
            # TODO: check dim
            pass
        return emb


class VectorDB:
    def __init__(self):
        self.client = OpenSearchClient()
        self.embedding_model = OpenAIEmbedding()

        if INDEX not in self.client.list_index():
            self.client.create_index(INDEX, INDEX_CONFIG)

    def embedding(self, text: str) -> List[float]:
        return self.embedding_model.embedding(text)

    def insert(self, file_path: str, chunk_size: int = 768):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read()

        chunks = [
            content[i : i + chunk_size] for i in range(0, len(content), chunk_size)
        ]
        for chunk in chunks:
            embedding = self.embedding(chunk)
            doc = {"file_path": file_path, "text": chunk, "embedding": embedding}
            self.client.insert_doc(INDEX, doc)

    def search(self, text, top_k=3):
        embedding = self.embedding(text)
        return self.client.knn_search(INDEX, embedding, top_k)


if __name__ == "__main__":
    db = VectorDB()
    text = "控制器"
    res = db.search(text)

    print(res)
