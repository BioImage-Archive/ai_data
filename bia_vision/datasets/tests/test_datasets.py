from bia_vision import datasets
import pytest


@pytest.mark.parametrize(
    "dataset",
    [
        datasets.broad.BBBC010,
    ],
)
def test_download(dataset):
    data = dataset()
    assert len(data) > 0
    assert data[0] is not None


@pytest.mark.parametrize(
    "dataset",
    [
        "BBBC010",
    ],
)

def test_download(dataset):
    data = datasets.BroadDataset(dataset)
    assert len(data) > 0
    assert data[0] is not None
