import os

from timdex_dataset_api import TIMDEXDataset


def get_dataset() -> TIMDEXDataset:
    return TIMDEXDataset(os.environ["TIMDEX_DATASET_LOCATION"])
