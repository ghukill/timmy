"""Flask-free, layered configuration for Timmy.

This is the single place config resolution lives. Both the CLI (``timmy.cli``)
and the Flask app factory (``timmy.app.create_app``) consume it, so the web and
agent surfaces agree on where ``dataset_location`` and friends come from.

Precedence, highest wins:

1. explicit CLI flag overrides (``--dataset-location``, ``--analysis-dir``)
2. ``TIMMY_*`` environment variables (keeps the pre-CLI behaviour working)
3. a local ``./timmy.toml`` (per-project override)
4. ``~/.timmy/config.toml`` (written by ``timmy init``)
5. built-in defaults

Reading TOML is stdlib (``tomllib``); writing is handled by ``timmy.cli`` via
``tomli_w`` and is intentionally not part of this read-only resolver.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

USER_CONFIG_DIR = Path.home() / ".timmy"
USER_CONFIG_PATH = USER_CONFIG_DIR / "config.toml"
PROJECT_CONFIG_NAME = "timmy.toml"

# Default analysis dir for an installed tool lives under the user's home, not
# the repo-relative "analyses" the Flask defaults used. A local timmy.toml (or a
# flag/env) still wins for a working copy.
DEFAULT_ANALYSIS_DIR = str(USER_CONFIG_DIR / "analyses")

# Timmy clones Transmogrifier (the source -> transformed engine) under the user's
# home so an agent has the real transform code to interrogate. The URL is a
# config field so a fork/branch can be pointed at instead of the canonical repo.
DEFAULT_TRANSMOG_DIR = str(USER_CONFIG_DIR / "transmogrifier")
DEFAULT_TRANSMOG_REPO_URL = "https://github.com/MITLibraries/transmogrifier"

# Corpus-build parallelism (see timmy.analysis.corpus). The flatten fan-out helps up
# to roughly the core count; 8 is plenty and we never oversubscribe a small box.
# build_workers <= 1 selects the original serial path. The batch size is the IPC unit:
# benchmarked against a local rustfs dataset, throughput climbs to ~4000 records/task
# (~30% over the old 1000) then plateaus, so 4000 is the default knee.
DEFAULT_BUILD_WORKERS = min(8, os.cpu_count() or 8)
DEFAULT_BUILD_BATCH_SIZE = 4000


@dataclass(frozen=True)
class Field:
    """One config value: its canonical/TOML key, env var, and default."""

    name: str
    env: str
    flask_key: str
    default: Any


# Canonical fields. ``name`` is the TOML key and CLI/dict key; ``flask_key`` is
# how the value lands in Flask's config (preserving the existing app contract).
FIELDS: tuple[Field, ...] = (
    Field("dataset_location", "TIMMY_TIMDEX_DATASET_LOCATION", "TIMDEX_DATASET_LOCATION", None),
    Field("analysis_dir", "TIMMY_TIMDEX_ANALYSIS_DIR", "TIMDEX_ANALYSIS_DIR", DEFAULT_ANALYSIS_DIR),
    Field("transmog_dir", "TIMMY_TRANSMOG_DIR", "TRANSMOG_DIR", DEFAULT_TRANSMOG_DIR),
    Field("transmog_repo_url", "TIMMY_TRANSMOG_REPO_URL", "TRANSMOG_REPO_URL", DEFAULT_TRANSMOG_REPO_URL),
    Field("log_level", "TIMMY_LOG_LEVEL", "LOG_LEVEL", "INFO"),
    Field("build_workers", "TIMMY_BUILD_WORKERS", "BUILD_WORKERS", DEFAULT_BUILD_WORKERS),
    Field("build_batch_size", "TIMMY_BUILD_BATCH", "BUILD_BATCH_SIZE", DEFAULT_BUILD_BATCH_SIZE),
)

_FIELDS_BY_NAME = {f.name: f for f in FIELDS}


def _read_toml(path: Path) -> dict[str, Any]:
    """Return the recognised keys from a TOML file, or {} if absent/unreadable keys."""
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except FileNotFoundError:
        return {}
    return {f.name: data[f.name] for f in FIELDS if f.name in data}


def _read_env() -> dict[str, Any]:
    return {f.name: os.environ[f.env] for f in FIELDS if f.env in os.environ}


def resolve_config(
    overrides: dict[str, Any] | None = None,
    *,
    cwd: Path | None = None,
    user_config_path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Resolve every field to ``{"value": ..., "source": ...}`` with provenance.

    ``overrides`` are CLI flag values; ``None`` entries are ignored so an unset
    flag doesn't shadow a lower layer. ``cwd``/``user_config_path`` are injectable
    for tests.
    """
    cwd = cwd or Path.cwd()
    user_config_path = user_config_path or USER_CONFIG_PATH
    overrides = {k: v for k, v in (overrides or {}).items() if v is not None}

    # Every field is seeded from defaults so it always appears (a None default,
    # e.g. an unconfigured dataset_location, still shows up as "unset"). Higher
    # layers only override when they actually carry a non-None value, so an
    # absent env var or file key never shadows a lower layer.
    resolved: dict[str, dict[str, Any]] = {
        f.name: {"value": f.default, "source": "default"} for f in FIELDS
    }

    higher_layers: list[tuple[str, dict[str, Any]]] = [
        ("user-config", _read_toml(user_config_path)),
        ("project-config", _read_toml(cwd / PROJECT_CONFIG_NAME)),
        ("env", _read_env()),
        ("flag", overrides),
    ]
    for source, values in higher_layers:
        for key, value in values.items():
            if key in _FIELDS_BY_NAME and value is not None:
                resolved[key] = {"value": value, "source": source}
    return resolved


def load_config(
    overrides: dict[str, Any] | None = None,
    *,
    cwd: Path | None = None,
    user_config_path: Path | None = None,
) -> dict[str, Any]:
    """Resolve config to a plain ``{name: value}`` dict (provenance discarded)."""
    return {
        key: entry["value"]
        for key, entry in resolve_config(
            overrides, cwd=cwd, user_config_path=user_config_path
        ).items()
    }


def to_flask_config(resolved: dict[str, Any]) -> dict[str, Any]:
    """Map a ``load_config()`` dict onto the Flask config keys the app expects."""
    return {_FIELDS_BY_NAME[name].flask_key: value for name, value in resolved.items()}


def default_flask_config() -> dict[str, Any]:
    """Flask-keyed defaults, so the app always has these keys even under test_config."""
    return {f.flask_key: f.default for f in FIELDS}
