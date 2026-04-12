from timdex_dataset_api import TIMDEXDataset


def get_dataset(
    location: str,
    *,
    preload_current_records: bool = True,
) -> TIMDEXDataset:
    return TIMDEXDataset(
        location,
        preload_current_records=preload_current_records,
    )
