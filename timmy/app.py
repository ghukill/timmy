from __future__ import annotations

from logging.config import dictConfig
from typing import Any

from flask import Flask

from timmy.analysis_views import analysis_bp
from timmy.config import default_flask_config, load_config, to_flask_config
from timmy.dataset import load_dataset
from timmy.logging_setup import apply_log_level
from timmy.main import main
from timmy.sources_views import sources_bp


# Base layer applied before either resolved config or test_config, so the app
# always has these keys present (the resolver/test_config then override).
DEFAULT_CONFIG: dict[str, Any] = {
    "SECRET_KEY": "dev",
    **default_flask_config(),
}


def configure_logging(app: Flask) -> None:
    log_level = apply_log_level(app.config["LOG_LEVEL"])

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


def create_app(
    test_config: dict[str, Any] | None = None,
    *,
    config_overrides: dict[str, Any] | None = None,
) -> Flask:
    """Application factory for the Flask app.

    Config resolution lives in :mod:`timmy.config` so the web app and CLI agree
    on where values come from. ``config_overrides`` carries CLI flag values (e.g.
    ``timmy --dataset-location ... webapp run``); ``test_config`` bypasses the
    resolver entirely for tests.
    """
    app = Flask(__name__)
    app.config.from_mapping(DEFAULT_CONFIG)

    if test_config is None:
        app.config.update(to_flask_config(load_config(config_overrides)))
    else:
        app.config.update(test_config)

    configure_logging(app)

    app.logger.debug("Initializing Timmy")

    dataset_location = app.config["TIMDEX_DATASET_LOCATION"]
    if not dataset_location:
        raise RuntimeError(
            "dataset_location is not configured. Run `timmy init`, set it in "
            "~/.timmy/config.toml, or export TIMMY_TIMDEX_DATASET_LOCATION."
        )

    # load TIMDEXDataset once when the app boots
    app.extensions["td"] = load_dataset(dataset_location)
    app.logger.info("Dataset loaded from %s", app.extensions["td"].location)

    app.register_blueprint(main)
    app.register_blueprint(sources_bp)
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

        def build_corpus(**kwargs: Any) -> dict[str, Any]:
            """(Re)build the corpus from all current records (pre-bound to td + dir)."""
            return analysis.build_corpus(td, analyses_dir, **kwargs)

        def update_corpus(**kwargs: Any) -> dict[str, Any]:
            """Reconcile the corpus against the live dataset (pre-bound to td + dir)."""
            return analysis.update_corpus(td, analyses_dir, **kwargs)

        def open_corpus(*, read_only: bool = True):
            """Open the corpus DB (pre-bound to dir)."""
            return analysis.open_corpus(analyses_dir, read_only=read_only)

        return {
            "td": td,
            "flatten": analysis.flatten,
            "flatten_record": analysis.flatten_record,
            "EAVRow": analysis.EAVRow,
            "SAMPLE_RECORD": analysis.SAMPLE_RECORD,
            "make_timdex_composite_id": analysis.make_timdex_composite_id,
            "sample_transformed": sample_transformed,
            "build_corpus": build_corpus,
            "update_corpus": update_corpus,
            "open_corpus": open_corpus,
            "read_corpus_meta": lambda: analysis.read_corpus_meta(analyses_dir),
        }


if __name__ == "__main__":
    create_app().run(debug=True)
