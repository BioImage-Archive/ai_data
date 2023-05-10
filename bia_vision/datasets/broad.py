from .torch import WebArchiveDataset


class LookupInfo:
    url: str
    md5: str
    dataset: str
    filename: str


class BroadDataset(WebArchiveDataset, LookupInfo):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BBBC010(BroadDataset):

    url = "https://data.broadinstitute.org/bbbc/BBBC010/BBBC010_v1_foreground_eachworm.zip"
    md5 = "ae73910ed293397b4ea5082883d278b1"
    dataset = "bbbc010"
    filename = "BBBC010_v1_foreground_eachworm.zip"

    def __init__(self, *args, **kwargs):
        super().__init__(
            self.url, self.md5, self.filename, self.dataset, *args, **kwargs
        )
