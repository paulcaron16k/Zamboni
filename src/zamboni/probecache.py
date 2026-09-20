# SPDX-License-Identifier: Apache-2.0
"""Remember what the probes found, keyed to the exact build that was probed.

`detect()` costs about half a second, because two probes create a table and
commit to it. It is `lru_cache`d, so a `zamboni maintenance` run over a hundred
tables pays that once -- but an operator whose cron line invokes the CLI per
table pays it per table, and that is the case this module exists for.

**Keyed on content, never on a version.** Two installs both declaring
`pyiceberg 0.12.0` -- the maintenance fork and stock from PyPI -- answer
`added_files_honour_spec` differently, which is why ZMBNI-39's version-keyed
table was closed as not planned. The signature here is a hash over what is
actually installed: each distribution's version, its `direct_url.json` (which
carries the git commit for a VCS install) and its `RECORD`, whose per-file
hashes change whenever any shipped file does. Measured: the fork and stock
installs hash differently, and reading both RECORDs costs ~2ms against ~528ms
of probing.

**An editable install is never cached, and that includes Zamboni's own.** An
editable `RECORD` lists the `.pth` and dist-info -- 15 entries, not the 105
source files -- and the package imports straight from the working tree. Editing
the source changes the version, the RECORD hash and the dist-info mtime not at
all, so any install-level signature goes stale while the code underneath moves.
That is the documented workflow for probe work (`uv pip install -e
../iceberg-python`) and the scenario ZMBNI-39 recorded: a checkout declaring
0.12.0 on a tree with a half-applied fix, where the probes correctly refused and
a remembered answer would have been confidently wrong. Zamboni's own editable
install disables it too, because its probe *logic* can change under a cache the
same way.

**Failure direction.** Caching "this build cannot do X" is harmless -- the worst
case is work we decline. Caching "this build can do X" is the direction that can
run a corrupting path, and it is acceptable only because the key is derived from
the installed bytes: a build whose files changed cannot match a stored answer.
Nothing here ever raises: a cache that cannot be read or written means probe, so
an unwritable filesystem costs time and not a run.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import tempfile
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

logger = logging.getLogger(__name__)

#: Bumped whenever probe *logic* changes in a way that invalidates stored
#: answers. The installed Zamboni version covers a released change; this covers
#: the rest, and costs nothing to bump.
CACHE_SCHEMA = 1

#: Both are fingerprinted: PyIceberg because it is what the probes ask about,
#: Zamboni because it is what does the asking.
_SIGNED = ("pyiceberg", "iceberg-zamboni")


def _is_editable(direct_url: str) -> bool:
    """True for a `pip install -e` / `uv pip install -e` install.

    Parsed rather than pattern-matched: the JSON is written by the installer and
    may or may not carry spaces, and a near-miss here caches against a tree that
    can change underneath it -- the one direction this module must not fail in.
    """
    try:
        return bool(json.loads(direct_url).get("dir_info", {}).get("editable"))
    except (ValueError, AttributeError):
        # Unparseable provenance is not evidence of a stable build.
        return True


def _fingerprint(name: str) -> str | None:
    """A distribution's identity, or ``None`` if it must not be cached against."""
    try:
        dist = distribution(name)
    except PackageNotFoundError:
        return None

    direct_url = dist.read_text("direct_url.json")
    if direct_url and _is_editable(direct_url):
        return None

    record = dist.read_text("RECORD")
    if record is None:
        # No manifest to hash means no way to notice the build changing.
        return None

    return f"{name}|{dist.version}|{direct_url or ''}|{hashlib.sha256(record.encode()).hexdigest()}"


def build_signature() -> str | None:
    """A stable key for the installed build, or ``None`` when it must not be cached."""
    parts = [f"schema={CACHE_SCHEMA}"]
    for name in _SIGNED:
        fingerprint = _fingerprint(name)
        if fingerprint is None:
            return None
        parts.append(fingerprint)
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:32]


def _candidate_dirs() -> list[Path]:
    """Where to keep it, most durable first.

    The venv outlives a pod restart and isolates one install from another, which
    is what this cache wants. A tmpfs `/tmp` is per-pod, so it degrades the win
    from "probe once per install" to "probe once per pod" -- still the whole
    point where the CLI is invoked per table.

    Deliberately *not* the spill directory ZMBNI-90 resolves. Same concept, two
    different needs: spill wants room and is happy on a tmpfs; this wants
    persistence and is tiny.
    """
    explicit = os.environ.get("ZAMBONI_CACHE_DIR")
    if explicit:
        return [Path(explicit)]
    xdg = os.environ.get("XDG_CACHE_HOME")
    return [
        Path(sys.prefix) / "var" / "zamboni",
        Path(xdg) / "zamboni" if xdg else Path.home() / ".cache" / "zamboni",
        Path(tempfile.gettempdir()) / "zamboni",
    ]


def _readable_path(signature: str) -> Path | None:
    for directory in _candidate_dirs():
        path = directory / f"capabilities-{signature}.json"
        if path.is_file():
            return path
    return None


def load(signature: str) -> dict | None:
    """Stored answers for this build, or ``None``. Never raises."""
    path = _readable_path(signature)
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        logger.debug("probe cache at %s is unreadable; re-probing", path, exc_info=True)
        return None
    if not isinstance(payload, dict) or payload.get("signature") != signature:
        return None
    answers = payload.get("answers")
    return answers if isinstance(answers, dict) else None


def store(signature: str, answers: dict) -> Path | None:
    """Write the answers, returning where. ``None`` if nowhere is writable."""
    document = json.dumps({"signature": signature, "answers": answers}, sort_keys=True)
    for directory in _candidate_dirs():
        try:
            directory.mkdir(parents=True, exist_ok=True)
            # Written to a neighbour and moved, so a reader never sees half a
            # file and two processes racing cannot interleave.
            with tempfile.NamedTemporaryFile(
                "w", dir=directory, prefix=".capabilities-", delete=False
            ) as handle:
                handle.write(document)
                staged = Path(handle.name)
            staged.replace(directory / f"capabilities-{signature}.json")
            return directory / f"capabilities-{signature}.json"
        except OSError:
            logger.debug("probe cache not writable at %s", directory, exc_info=True)
    return None


def describe(signature: str | None, *, hit: bool) -> str:
    """One line for ``zamboni doctor``."""
    if signature is None:
        return "disabled (editable install -- answers cannot be keyed to the source)"
    where = _readable_path(signature)
    if hit and where:
        return f"hit {signature[:12]} ({where})"
    return f"miss {signature[:12]} (probed" + (f", stored in {where.parent})" if where else ")")
