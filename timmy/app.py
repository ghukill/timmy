from __future__ import annotations

from logging.config import dictConfig
from typing import Any

from flask import Flask

from timmy.analysis_views import analysis_bp
from timmy.dataset import load_dataset
from timmy.main import main


DEFAULT_CONFIG: dict[str, Any] = {
    "SECRET_KEY": "dev",
    "LOG_LEVEL": "INFO",
    "TIMDEX_DATASET_LOCATION": None,
    # Directory where materialized analysis DuckDB files are written/read.
    "TIMDEX_ANALYSIS_DIR": "analyses",
}


def configure_logging(app: Flask) -> None:
    log_level = app.config["LOG_LEVEL"].upper()

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
                }
            },
            "handlers": {
                "wsgi": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://flask.logging.wsgi_errors_stream",
                    "formatter": "default",
                }
            },
            "root": {
                "level": log_level,
                "handlers": ["wsgi"],
            },
        }
    )


def create_app(test_config: dict[str, Any] | None = None) -> Flask:
    """Application factory for the Flask app."""
    app = Flask(__name__)
    app.config.from_mapping(DEFAULT_CONFIG)

    if test_config is None:
        app.config.from_prefixed_env(prefix="TIMMY")
    else:
        app.config.update(test_config)

    configure_logging(app)

    app.logger.debug("Initializing Timmy")

    dataset_location = app.config["TIMDEX_DATASET_LOCATION"]
    if not dataset_location:
        raise RuntimeError(
            "TIMDEX_DATASET_LOCATION is not configured. "
            "Set it in Flask config or via TIMMY_TIMDEX_DATASET_LOCATION."
        )

    # load TIMDEXDataset once when the app boots
    app.extensions["td"] = load_dataset(dataset_location)
    app.logger.info("Dataset loaded from %s", app.extensions["td"].location)

    app.register_blueprint(main)
    app.register_blueprint(analysis_bp)

    register_shell_context(app)

    return app


def register_shell_context(app: Flask) -> None:
    """Inject analysis helpers into ``flask shell`` (ipython) for interactive use.

    Lets you flatten records in the REPL with zero setup, e.g.::

        flatten_record(SAMPLE_RECORD)          # offline, hand-built sample
        rec = sample_transformed(limit=1)[0]   # a real transformed record
        flatten_record(rec)                    # flatten it
        sample_transformed(source="libguides") # narrow by any TDA filter
    """
    from timmy import analysis

    @app.shell_context_processor
    def _shell_context() -> dict[str, Any]:
        td = app.extensions["td"]
        analyses_dir = app.config["TIMDEX_ANALYSIS_DIR"]

        def sample_transformed(
            limit: int = 1,
            table: str = "current_records",
            **filters: Any,
        ) -> list[dict]:
            """Pull parsed transformed records via TDA (honours TDA filters)."""
            return list(
                td.records.read_transformed_records_iter(
                    table=table, limit=limit, **filters
                )
            )

        def build_analysis(**kwargs: Any) -> dict[str, Any]:
            """Build an analysis DB from a filter (pre-bound to td + dir)."""
            return analysis.build_analysis(td, analyses_dir, **kwargs)

        def open_analysis(analysis_id: str, *, read_only: bool = True):
            """Open an analysis DB by id (pre-bound to dir)."""
            return analysis.open_analysis(analyses_dir, analysis_id, read_only=read_only)

        def list_analyses() -> list[dict[str, Any]]:
            """List built analyses, newest first (pre-bound to dir)."""
            return analysis.list_analyses(analyses_dir)

        return {
            "td": td,
            "flatten": analysis.flatten,
            "flatten_record": analysis.flatten_record,
            "EAVRow": analysis.EAVRow,
            "SAMPLE_RECORD": analysis.SAMPLE_RECORD,
            "make_timdex_composite_id": analysis.make_timdex_composite_id,
            "sample_transformed": sample_transformed,
            "build_analysis": build_analysis,
            "open_analysis": open_analysis,
            "list_analyses": list_analyses,
        }


if __name__ == "__main__":
    create_app().run(debug=True)
