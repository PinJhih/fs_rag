import threading
import time

from fastmcp import FastMCP

from fs import FileSystem
from vecdb import VectorDB

mcp = FastMCP("FS-RAG")
fs = FileSystem()
vecdb = VectorDB()


def worker():
    while True:
        fs.check_modified()
        time.sleep(15)


t = threading.Thread(target=worker, daemon=True)
t.start()


@mcp.tool
def search(text: str) -> str:
    """
    Search from a vector store
    """
    result = vecdb.search(text)
    return str(result)


if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000)
