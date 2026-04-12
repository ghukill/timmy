from __future__ import annotations

from logging.config import dictConfig
from typing import Any

from flask import Flask, current_app, render_template

from timmy.dataset import get_dataset


DEFAULT_CONFIG: dict[str, Any] = {
    "SECRET_KEY": "dev",
    "LOG_LEVEL": "INFO",
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
        app.config.from_prefixed_env()
    else:
        app.config.update(test_config)

    configure_logging(app)

    app.logger.info("Initializing Timmy app")

    # load TIMDEXDataset once when the app boots
    app.extensions["td"] = get_dataset()
    app.td = app.extensions["td"]  # NOTE: convenience dot notation
    app.logger.info("Dataset loaded from %s", app.extensions["td"].location)

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            td=app.td,
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
