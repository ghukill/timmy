from __future__ import annotations

from logging.config import dictConfig
from typing import Any

from flask import Flask, render_template

from timmy.dataset import get_dataset


DEFAULT_CONFIG: dict[str, Any] = {
    "SECRET_KEY": "dev",
    "LOG_LEVEL": "INFO",
    "TIMDEX_DATASET_LOCATION": None,
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
    app.extensions["td"] = get_dataset(dataset_location)
    app.td = app.extensions["td"]  # NOTE: convenience dot notation, may remove
    app.logger.info("Dataset loaded from %s", app.extensions["td"].location)

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            td=app.td,
        )

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
