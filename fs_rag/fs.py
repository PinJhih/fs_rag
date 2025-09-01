import os

from os_client import OpenSearchClient

DATA_DIR = os.getenv("DATA_DIR", "./data")
INDEX = "file_metadata"
INDEX_CONFIG = {
    "mappings": {
        "properties": {
            "file_name": {"type": "text"},
            "last_modify": {"type": "date"},
        }
    }
}


class FileSystem:
    def __init__(self):
        self.client = OpenSearchClient()
        self.last_modified = {}

        if INDEX not in self.client.list_index():
            self.client.create_index(INDEX, INDEX_CONFIG)

        meta_data = self.client.list_docs(INDEX)
        for meta in meta_data:
            src = meta["_source"]
            file = src["file_name"]
            last_modify = int(src["last_modify"])
            self.last_modified[file] = last_modify

    def _mtime_ms(self, path: str) -> int:
        return os.stat(path).st_mtime_ns // 1_000_000

    def check_modified(self):
        modified_files = []
        for root, _, files in os.walk(DATA_DIR):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                rel_path = os.path.relpath(file_path, DATA_DIR)

                last_modify_ms = self._mtime_ms(file_path)

                if (
                    rel_path not in self.last_modified
                    or self.last_modified[rel_path] != last_modify_ms
                ):
                    self.client.insert_doc(
                        INDEX,
                        {"file_name": rel_path, "last_modify": last_modify_ms},
                    )
                    self.last_modified[rel_path] = last_modify_ms
                    modified_files.append(rel_path)

                    # TODO: insert/update the vector DB
        return modified_files


if __name__ == "__main__":
    import random

    fs = FileSystem()
    fs.check_modified()
    print(fs.last_modified)
    print("=" * 40)

    with open("./data/test.txt", "w") as f:
        f.writelines("=" * random.randint(1, 40))

    fs.check_modified()
    print(fs.last_modified)
