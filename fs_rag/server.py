import threading
import time

from fastmcp import FastMCP

from fs import FileSystem
from vecdb import VectorDB
from logger import get_json_logger

mcp = FastMCP("FS-RAG")
fs = FileSystem()
vecdb = VectorDB()
logger = get_json_logger("server")

def worker():
    while True:
        modified_files, new_files = fs.check_modified()
        for file in modified_files:
            vecdb.delete(file)
            vecdb.insert(file)
            logger.info(f"Update file: {file}")

        for file in new_files:
            vecdb.insert(file)
            logger.info(f"Insert file: {file}")
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
