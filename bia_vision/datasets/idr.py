# from idr import connection
import numpy as np

from PIL import Image
from torch.utils.data import Dataset


#TODO 
# This is now generic and possibly replacable by torchvision.datasets.folder.ImageFolder
# I should make it so that experiments or image sets are downloaded and cached in the same way as torchvision.datasets like in WebArchive
# Otherwise then it's just a wrapper for ImageFolder

class IDRDataSet(Dataset):
    def __init__(self, file_list, transform=None):
        super(IDRDataSet).__init__()
        self.transform = transform
        self.file_list = file_list

    def __getitem__(self, index):

        image = self.get_image(index)

        if image == None:
            return None

        if self.transform:
            transformed = self.transform(np.array(image))
            return transformed
        return np.array(image)

    def __len__(self):
        return len(self.file_list)

    def get_image(self, file_list, index=0):
        return Image.open(file_list[index])
