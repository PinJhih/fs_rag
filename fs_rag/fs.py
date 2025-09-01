import os

from os_client import OpenSearchClient

DATA_DIR = os.getenv("DATA_DIR", "./data")
INDEX = "file_metadata"
MAPPING = {
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
        self.last_modified = dict()

    def check_modified(self):
        for root, _, files in os.walk(DATA_DIR):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                last_modify_time = os.path.getmtime(file_path)

                if (
                    file_name not in self.last_modified
                    or self.last_modified[file_name] != last_modify_time
                ):
                    self.client.insert_doc(
                        INDEX,
                        {"file_name": file_name, "last_modify": last_modify_time},
                    )
                    self.last_modified[file_name] = last_modify_time

                    # TODO: insert/update the vector DB


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
