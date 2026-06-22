"""Clone and manage a local checkout of Transmogrifier.

Transmogrifier (https://github.com/MITLibraries/transmogrifier) is the engine
that turns a source record into the normalized TIMDEX ``transformed_record`` --
i.e. *how records enter the TIMDEX dataset*. Timmy itself only reads the
finished records; it never runs the transform. But for the "why does this field
look like this?" class of question, the definitive answer lives in the transform
code, not in the payloads.

So Timmy clones the real repo under the user's home (``~/.timmy/transmogrifier``
by default) and hands an agent the breadcrumb: the docs/skill explain the
pipeline, ``timmy transmog path`` says where the code is, and the agent reads the
actual transformer for a given source to reason about a mapping definitively.

This module is the Flask-free git layer (thin ``subprocess`` wrappers); the CLI
in :mod:`timmy.cli` is the only caller today. No new dependency -- it shells out
to the user's ``git``.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any


class TransmogError(Exception):
    """A git/clone operation against the Transmogrifier checkout failed."""


def _git(args: list[str], *, cwd: Path | None = None) -> str:
    """Run a git command, returning trimmed stdout or raising TransmogError.

    Both "git isn't installed" and "git exited non-zero" become a TransmogError
    with a readable message, so the CLI can map either to a clean failure.
    """
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as exc:  # git not on PATH
        raise TransmogError("git is not installed or not on PATH.") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        raise TransmogError(f"git {' '.join(args)} failed: {detail}") from exc
    return proc.stdout.strip()


def _is_git_repo(path: Path) -> bool:
    """True if ``path`` is the top of a git working tree."""
    if not (path / ".git").exists():
        return False
    try:
        top = _git(["rev-parse", "--show-toplevel"], cwd=path)
    except TransmogError:
        return False
    return Path(top).resolve() == path.resolve()


def repo_status(transmog_dir: str | Path, repo_url: str | None = None) -> dict[str, Any]:
    """Describe the local Transmogrifier checkout (cloned? which commit?).

    Always returns the same shape so callers (human table or ``--json``) get a
    stable record. ``repo_url`` is the *configured* upstream; ``remote_url`` is
    whatever the clone's ``origin`` actually points at (they can differ if a fork
    was cloned and the config later changed).
    """
    path = Path(transmog_dir).expanduser()
    status: dict[str, Any] = {
        "cloned": False,
        "path": str(path),
        "repo_url": repo_url,
        "remote_url": None,
        "branch": None,
        "commit": None,
        "commit_date": None,
        "dirty": None,
    }
    if not _is_git_repo(path):
        return status

    status["cloned"] = True
    # Each lookup is best-effort: a detached HEAD or a missing origin shouldn't
    # blank out the rest of the status record.
    for key, args in (
        ("remote_url", ["config", "--get", "remote.origin.url"]),
        ("branch", ["rev-parse", "--abbrev-ref", "HEAD"]),
        ("commit", ["rev-parse", "--short", "HEAD"]),
        ("commit_date", ["log", "-1", "--format=%cs"]),
    ):
        try:
            status[key] = _git(args, cwd=path)
        except TransmogError:
            status[key] = None
    try:
        status["dirty"] = bool(_git(["status", "--porcelain"], cwd=path))
    except TransmogError:
        status["dirty"] = None
    return status


def clone_repo(
    transmog_dir: str | Path,
    repo_url: str,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Clone Transmogrifier into ``transmog_dir``; return the resulting status.

    A non-empty target is refused unless ``force`` is set, in which case it is
    removed first. The parent directory (e.g. ``~/.timmy``) is created as needed.
    """
    path = Path(transmog_dir).expanduser()
    if path.exists() and any(path.iterdir()):
        if not force:
            raise TransmogError(
                f"{path} already exists and is not empty; force a re-clone "
                "(--force) or update the existing checkout instead."
            )
        shutil.rmtree(path)

    path.parent.mkdir(parents=True, exist_ok=True)
    _git(["clone", repo_url, str(path)])
    return repo_status(path, repo_url)


def update_repo(transmog_dir: str | Path, repo_url: str | None = None) -> dict[str, Any]:
    """Fast-forward the existing checkout to its upstream; return before/after.

    Uses ``pull --ff-only`` so a diverged or locally-modified checkout fails
    loudly rather than producing a merge commit. The returned status carries an
    extra ``previous_commit`` and ``updated`` flag so a caller can report whether
    anything actually moved.
    """
    path = Path(transmog_dir).expanduser()
    if not _is_git_repo(path):
        raise TransmogError(
            f"No Transmogrifier checkout at {path}; clone it first."
        )

    before = repo_status(path, repo_url)["commit"]
    _git(["pull", "--ff-only"], cwd=path)
    status = repo_status(path, repo_url)
    status["previous_commit"] = before
    status["updated"] = before != status["commit"]
    return status
