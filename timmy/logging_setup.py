"""Shared logging setup for Timmy's web and CLI surfaces.

A single ``log_level`` config value (see :mod:`timmy.config`, written to
``~/.timmy/config.toml`` by ``timmy init``) drives both Timmy's own loggers and
the timdex_dataset_api (TDA) library's loggers. This replaces the former
``LOG_LEVEL`` / ``TDA_LOG_LEVEL`` env vars from the old ``.env`` file: setting
``log_level = "DEBUG"`` now turns on DEBUG logging everywhere.
"""

from __future__ import annotations

import logging

# TDA's own logger namespace; we drive it from our single log_level so its loggers
# follow Timmy's setting instead of the now-retired TDA_LOG_LEVEL env var.
TDA_LOGGER = "timdex_dataset_api"

# Third-party loggers too chatty to be useful at DEBUG. Mirrors the old .env
# WARNING_ONLY_LOGGERS so a DEBUG run isn't drowned in boto/urllib noise.
NOISY_LOGGERS = ("asyncio", "botocore", "urllib3", "s3transfer", "boto3")


def normalize_level(log_level: str | None) -> str:
    """Upper-case and validate a level name, defaulting to INFO."""
    level = (log_level or "INFO").strip().upper()
    if level not in logging.getLevelNamesMapping():
        raise ValueError(f"Invalid log level: {log_level!r}")
    return level


def apply_log_level(log_level: str | None) -> str:
    """Apply ``log_level`` to the root, TDA, and noisy third-party loggers.

    Returns the normalized level name. Callers are responsible for installing
    handlers (the CLI uses ``basicConfig``; the Flask app uses ``dictConfig``);
    this only sets levels so both surfaces agree on what a given config means.
    """
    level = normalize_level(log_level)
    logging.getLogger().setLevel(level)
    logging.getLogger(TDA_LOGGER).setLevel(level)
    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    return level
