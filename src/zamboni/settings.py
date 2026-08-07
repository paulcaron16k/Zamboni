"""Configuration a cron line does not have to type.

(Named settings rather than profile: profile.py is table profiling,
and Profile here is the operator-facing config file. Two different senses of
the word, one of which was already taken.)

Two files, split by whether the contents are secret:

* ``zamboni.yml`` -- catalog URI, warehouse, engine, which operations to run.
  Belongs in version control.
* ``.env`` -- credentials. Does not.

Both default to the working directory, so a cron entry is a ``cd`` and a
command. The split is not cosmetic: putting credentials in the crontab puts them
in ``crontab -l``, in every backup of ``/var/spool/cron``, and in the process
table, so there has to be somewhere else for them to live that is not the
profile people commit.

**Resolution order**, highest wins: a command-line flag, a ``ZAMBONI_*``
environment variable, ``./zamboni.yml``, ``$ZAMBONI_ROOT/zamboni.yml``, the
built-in default. A flag always wins, so a one-off run overrides the profile
without editing it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: Where per-warehouse configuration lives when nothing says otherwise.
DEFAULT_ROOT = Path.home() / ".zamboni"

PROFILE_NAME = "zamboni.yml"
ENV_NAME = ".env"

#: The runbook order. Encoded here rather than in a shell script because three
#: of the five gaps between these are load-bearing -- see docs/runbook-dev.md §1.
DEFAULT_OPERATIONS = (
    "compact",
    "apply-properties",
    "remove-dangling-deletes",
    "rewrite-manifests",
    "expire",
    "remove-orphans",
)


class ProfileError(ValueError):
    """The profile is unusable. Raised at load, never mid-run."""


@dataclass(frozen=True)
class Profile:
    """Non-secret configuration."""

    uri: str | None = None
    warehouse: str | None = None
    engine: str = "local"
    root: Path = DEFAULT_ROOT
    operations: tuple[str, ...] = DEFAULT_OPERATIONS
    tables: tuple[str, ...] = ()
    #: Where this came from, for `--help` and error messages. `None` means
    #: nothing was found and the defaults are in force.
    source: Path | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def table_config_for(self, warehouse: str) -> Path:
        """``$ZAMBONI_ROOT/configs/{warehouse}/table-config.json``.

        One file per customer in a predictable place, so provisioning a new
        tenant is writing that file and adding a cron line -- see
        docs/devops.md §5.
        """
        return self.root / "configs" / warehouse / "table-config.json"


def find_env(explicit: str | None = None, *, start: Path | None = None) -> Path | None:
    """The dotenv file to load, or None."""
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise ProfileError(f"--env {explicit}: no such file")
        return path
    candidate = (start or Path.cwd()) / ENV_NAME
    return candidate if candidate.is_file() else None


def load_env(path: Path | None) -> dict[str, str]:
    """Load ``path`` into ``os.environ`` without overwriting what is there.

    Real environment beats the file deliberately: a container or systemd unit
    that injects secrets properly should not be overridden by a stale ``.env``
    someone left in the working directory.
    """
    if path is None:
        return {}
    from dotenv import dotenv_values

    values = {k: v for k, v in dotenv_values(path).items() if v is not None}
    applied = {k: v for k, v in values.items() if k not in os.environ}
    os.environ.update(applied)
    return applied


def find_profile(explicit: str | None = None, *, start: Path | None = None) -> Path | None:
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise ProfileError(f"--profile {explicit}: no such file")
        return path
    root_env = os.environ.get("ZAMBONI_ROOT")
    for candidate in (
        (start or Path.cwd()) / PROFILE_NAME,
        (Path(root_env) if root_env else DEFAULT_ROOT) / PROFILE_NAME,
    ):
        if candidate.is_file():
            return candidate
    return None


def load_profile(path: Path | None) -> Profile:
    """Parse a profile, rejecting what it cannot honour.

    Unknown keys are an error rather than a warning, matching
    ``table-config.json``. A misspelled key that is silently ignored is a
    setting the operator believes is in force and is not, which on this tool
    means a retention or an operation list that is not what the file says.
    """
    if path is None:
        return _from_environment(source=None)

    import yaml

    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ProfileError(f"{path}: not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ProfileError(f"{path}: expected a mapping at the top level")

    known = {"uri", "warehouse", "engine", "root", "operations", "tables"}
    if unknown := sorted(set(raw) - known):
        raise ProfileError(
            f"{path}: unknown key(s) {', '.join(unknown)}. Known keys: {', '.join(sorted(known))}"
        )

    operations = tuple(raw.get("operations") or DEFAULT_OPERATIONS)
    if bad := [op for op in operations if op not in DEFAULT_OPERATIONS]:
        raise ProfileError(
            f"{path}: unknown operation(s) {', '.join(bad)}. Known: {', '.join(DEFAULT_OPERATIONS)}"
        )

    base = _from_environment(source=path)
    root = raw.get("root")
    return Profile(
        uri=raw.get("uri") or base.uri,
        warehouse=raw.get("warehouse") or base.warehouse,
        engine=raw.get("engine") or base.engine,
        root=Path(root).expanduser() if root else base.root,
        operations=operations,
        tables=tuple(raw.get("tables") or ()),
        source=path,
    )


def _from_environment(*, source: Path | None) -> Profile:
    root = os.environ.get("ZAMBONI_ROOT")
    return Profile(
        uri=os.environ.get("ZAMBONI_URI"),
        warehouse=os.environ.get("ZAMBONI_WAREHOUSE"),
        engine=os.environ.get("ZAMBONI_ENGINE", "local"),
        root=Path(root).expanduser() if root else DEFAULT_ROOT,
        source=source,
    )


def resolve(
    *,
    profile_path: str | None = None,
    env_path: str | None = None,
    start: Path | None = None,
) -> tuple[Profile, Path | None]:
    """Load the env file, then the profile. Returns both, for reporting.

    Env first, because the profile's own defaults read ``ZAMBONI_*`` -- a
    ``.env`` that sets ``ZAMBONI_WAREHOUSE`` has to be in effect before the
    profile decides whether it needs to.
    """
    env_file = find_env(env_path, start=start)
    load_env(env_file)
    return load_profile(find_profile(profile_path, start=start)), env_file
