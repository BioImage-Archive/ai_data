from ..datasets import WebArchiveDataset
import requests
from dataclasses import dataclass


@dataclass
class ZenodoDataset(WebArchiveDataset):
    record_id: str
    zip_file: str

    def __init__(self, record_id, zip_file, md5, *args, **kwargs):
        # TODO convert the record_id to a zip file
        url = f"https://zenodo.org/record/{record_id}/files/{zip_file}.zip?download=1"
        super().__init__(url, md5, zip_file, record_id, *args, **kwargs)


class StarDistZenodoDataset(ZenodoDataset):
    record_id = "id3715492"
    zip_file = "StarDist_v2.zip"
    md5 = "5b7490336e5c5b8035e43cd8302e80a3"

    def __init__(self, *args, **kwargs):
        super().__init__(self.record_id, self.record_id, self.md5, *args, **kwargs)

def get_zenodo_filelist(record=None):
    response = requests.get(f"https://zenodo.org/api/records/{record_id}", timeout=10)
    record = response.json()
    return record["files"]
