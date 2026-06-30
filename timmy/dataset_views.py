"""Web layer for the dataset management space (the ``/dataset`` blueprint).

Operational actions that maintain the underlying ``TIMDEXDataset`` itself, as opposed
to the read-only profiling (analysis) and provenance (sources) wings. Each action runs
as a single in-process background job (see :mod:`timmy.dataset_job`) and reports
progress on a dedicated page, leaving room for more actions to be added as cards on the
landing page over time.

Actions:

- **Rebuild dataset metadata** -- fully rebuild the dataset's static metadata database
  via ``td.metadata.rebuild_dataset_metadata()``. This is a *write*: it clears the
  append deltas and overwrites the canonical ``metadata.duckdb`` (e.g. in S3), so the
  UI guards it behind a confirmation.

Routes:

- ``GET  /dataset/``                 management landing page (action cards)
- ``POST /dataset/metadata/rebuild`` start the metadata rebuild job, then show progress
- ``GET  /dataset/job``              progress page for the running/just-finished action
- ``GET  /dataset/job.json``         progress snapshot (polled by the progress page)
"""

from __future__ import annotations

from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    url_for,
)

from timmy import dataset_ops
from timmy.dataset import dataset_lock, get_app_dataset
from timmy.dataset_job import dataset_job

dataset_bp = Blueprint("dataset", __name__, url_prefix="/dataset")


@dataset_bp.get("/")
def index() -> str:
    """The management landing page: dataset location + one card per action."""
    td = get_app_dataset()
    return render_template(
        "dataset.html",
        location=td.location,
        job=dataset_job.snapshot(),
    )


@dataset_bp.post("/metadata/rebuild")
def rebuild_metadata():
    """Kick off a full metadata rebuild in the background, then show its progress."""
    # Capture the dataset in the request thread: the daemon job thread has no app
    # context, so it can't call get_app_dataset() itself.
    td = get_app_dataset()

    def runner(on_progress):
        # The rebuild ends with td.refresh(), mutating the shared connection, so hold
        # dataset_lock for the whole operation (blocks other dataset reads meanwhile).
        with dataset_lock:
            return dataset_ops.rebuild_metadata(td, on_progress)

    try:
        dataset_job.start("metadata_rebuild", "Rebuild dataset metadata", runner)
    except RuntimeError:
        pass  # one already running -- just fall through to its progress page
    return redirect(url_for("dataset.job_page"))


@dataset_bp.get("/job")
def job_page() -> str:
    """Progress page for the running (or just-finished) dataset action."""
    return render_template("dataset_job.html")


@dataset_bp.get("/job.json")
def job_json():
    """Progress snapshot, polled by the progress page."""
    return jsonify(dataset_job.snapshot())
