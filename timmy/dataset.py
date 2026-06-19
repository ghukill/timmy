import threading

from flask import current_app

from timdex_dataset_api import TIMDEXDataset

# The app shares a single TIMDEXDataset (and its one DuckDB connection) across
# threaded requests. A DuckDB connection cannot run queries concurrently, so all
# dataset/DuckDB access must be serialized through this lock. Without it,
# overlapping requests (e.g. the versions page fetching two payloads in parallel
# to diff them) raise "closed pending query result".
dataset_lock = threading.Lock()


def load_dataset(
    location: str,
    *,
    preload_current_records: bool = True,
) -> TIMDEXDataset:
    """Load a TIMDEXDataset.

    This function is ideally only called during app setup, then get_app_dataset() is
    used from then on.
    """
    return TIMDEXDataset(
        location,
        preload_current_records=preload_current_records,
    )


def get_app_dataset() -> TIMDEXDataset:
    """Retrieve instantiated TIMDEXDataset from current app context."""
    return current_app.extensions["td"]
