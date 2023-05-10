import glob
import random
import numpy as np

# Note - you must have torchvision installed for this example
from torch.utils.data import Dataset
from PIL import Image
import os

from urllib.error import URLError

from PIL import Image, ImageSequence

from torchvision.datasets.utils import (
    check_integrity,
    download_and_extract_archive,
    extract_archive,
    verify_str_arg,
)


class DatasetFileList(Dataset):
    def __init__(self, file_paths, transform=None, samples=-1, shuffle=True, **kwargs):
        # self.image_paths = glob.glob(path_glob, recursive=True)
        self.file_paths = file_paths
        if shuffle:
            random.shuffle(self.file_paths)
        if samples > 0 and samples < len(self.file_paths):
            self.file_paths = self.file_paths[0:samples]
        self.transform = transform
        self.samples = samples
        assert len(self.file_paths) > 0

    def __len__(self):
        return len(self.file_paths)

    def getitem(self, index):
        try:
            x = Image.open(self.file_paths[index])
            # TODO breaking issue with multicolour TIFFs
            x = ImageSequence.Iterator(x)
            if self.transform is not None:
                x = self.transform(x[0])
            return x
        except:
            return None

    def __getitem__(self, index):

        dummy_list = np.arange(0, self.__len__())
        loop = np.array(dummy_list[index])
        if isinstance(index, slice):
            return [self.getitem(i) for i in loop]
        return self.getitem(index)


class DatasetGlob(DatasetFileList):
    def __init__(self, pattern, transform=None, samples=-1, shuffle=True, **kwargs):
        filepaths = glob.glob(pattern, recursive=True)
        super().__init__(filepaths, transform, samples, shuffle, **kwargs)


class WebArchiveDataset(DatasetGlob):
    def __init__(
        self,
        url,
        md5,
        filename,
        dataset,
        data_folder="data",
        download=True,
        transform=None,
        **kwargs,
    ):
        self.url = url
        self.md5 = md5
        self.dataset = dataset
        self.raw_folder = f"{data_folder}/{self.dataset}"
        self.filename = filename
        self.images_file = self.filename
        path_glob = f"{self.raw_folder}/**/*.png"

        if download:
            self.download()
        super(WebArchiveDataset, self).__init__(path_glob, transform, **kwargs)

    def _check_exists(self) -> bool:
        return all(check_integrity(file) for file in (self.images_file))

    def download(self) -> None:

        if self._check_exists():
            return

        os.makedirs(self.raw_folder, exist_ok=True)

        try:
            print(f"Downloading {self.url}")
            download_and_extract_archive(
                self.url,
                download_root=self.raw_folder,
                filename=self.filename,
                md5=self.md5,
            )
        except URLError as error:
            print(f"Failed to download (trying next):\n{error}")
