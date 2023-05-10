import torch
import math
# from idr import connection
from torchvision.datasets.vision import VisionDataset
from torchvision.transforms import ToTensor
import numpy as np
from torch.utils.data import Dataset, DataLoader

from torchvision.transforms import ToTensor
import matplotlib.pyplot as plt
from torchvision.datasets.folder import default_loader
from PIL import Image
from torchvision import transforms

def collate_none(batch):
    batch = list(filter(lambda x: x is not None, batch))
    return torch.utils.data.dataloader.default_collate(batch)

def get_test_image(dataset):
    test_img = next(item for item in dataset if item is not None)
    return test_img.unsqueeze(0)


def make_size(im, size=[2048, 2048]):
    if list(im.size) < list(size):
        image = pad_to(im, size)
    else:
        image = crop_to(im, size)
    return image


def pad_to(im, size=[2048, 2048]):

    left = int(size[0] / 2 - im.size[0] / 2)
    top = int(size[1] / 2 - im.size[1] / 2)

    image = Image.new(im.mode, size, 0)
    image.paste(im, (left, top))
    return image


def crop_to(im, size=[2048, 2048]):
    left = int(size[0] / 2 - im.size[0] / 2)
    upper = int(size[1] / 2 - im.size[1] / 2)
    right = left + size[0]
    lower = upper + size[1]
    image = im.crop((left, upper, right, lower))
    return image

def to_img(x):
    x = 0.5 * (x + 1)
    x = x.clamp(0, 1)
    x = x.view(x.size(0), 1, 28, 28)
    return x

def is_image_cropped(image):
    if (
        np.sum(
            np.sum(image[:, 0]),
            np.sum(image[:, -1]),
            np.sum(image[0, :]),
            np.sum(image[-1, :]),
        )
        == 0
    ):
        return False
    else:
        return True