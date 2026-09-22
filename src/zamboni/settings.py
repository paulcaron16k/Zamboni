# SPDX-License-Identifier: Apache-2.0
"""Configuration a cron line does not have to type.

(Named settings rather than profile: profile.py is table profiling,
and Profile here is the operator-facing config file. Two different senses of
the word, one of which was already taken.)

Two files, and the usual split is by whether the contents are secret:

* ``zamboni.yml`` -- catalog URI, warehouse, engine, which operations to run.
  Belongs in version control.
* ``.env`` -- credentials. Does not.

Both default to the working directory, so a cron entry is a ``cd`` and a
command. The split is not cosmetic: putting credentials in the crontab puts them
in ``crontab -l``, in every backup of ``/var/spool/cron``, and in the process
table, so there has to be somewhere else for them to live that is not the
profile people commit.

**The profile may nevertheless hold secrets**, and that is deliberate rather
than a loosening. The rule that made them illegal here assumed this file is
committed -- which is the common case and not the only one. A Kubernetes Secret
mounts as a *file*, so a profile projected from one is exactly as protected as
an environment variable and splitting the same deployment's configuration across
two mechanisms buys nothing. Forcing the split also has a cost that is easy to
miss: an operator who cannot put a credential where the rest of the
configuration lives will put the rest of the configuration where the credential
is, and ``.env`` becomes the profile.

So a secret in the profile is allowed and **treated as what it is**: the moment
one is present the file is a credential file, and
:func:`check_profile_permissions` applies the same mode rule ``.env`` has always
had -- readable by group or other is a hard error, not a warning. Precedence is
unchanged and ordinary: a flag, then a ``ZAMBONI_*`` variable, then the profile.
A deployment that injects secrets properly keeps working untouched, because the
environment still wins.

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


#: What may appear under `trino:`/`spark:` in the profile. Still no password or
#: token -- not because the file may not hold secrets (it may, see the module
#: docstring) but because neither maintainer *accepts* one. Adding a key the
#: engine cannot use would be inventing configuration.
ENGINE_SETTINGS = {
    "trino": frozenset({"host", "port", "user", "catalog", "version"}),
    "spark": frozenset({"remote", "master", "catalog"}),
}

#: What may appear under `storage:` in the profile, per provider. An allow-list
#: for the same reason the engine blocks have one: a typo should be an error at
#: load rather than a setting that silently does nothing.
STORAGE_SETTINGS = {
    "s3": frozenset(
        {"endpoint", "region", "access_key_id", "secret_access_key", "path_style_access"}
    ),
    "gcs": frozenset({"token", "project_id", "service_host", "default_location"}),
    "azure": frozenset(
        {"account_name", "account_key", "sas_token", "client_id", "client_secret", "tenant_id"}
    ),
}

#: What may appear under `ssl:` in the profile. Neither is a secret -- a CA
#: bundle is a path to a *public* certificate, and `insecure` is a policy -- so
#: this block does not make the profile a credential file.
SSL_SETTINGS = frozenset({"ca_bundle", "insecure"})

#: Keys whose presence makes the profile a credential file. Conservative on
#: purpose:
#:
#: * ``access_key_id`` is **not** here -- a key id is an identifier, which is the
#:   same judgement that keeps `--s3-access-key-id` as a flag.
#: * ``gcs.token`` **is**, even though it usually holds a key-file path or
#:   ``google_default``, neither of which is a secret. Deciding per value would
#:   mean a file that is a credential file on Tuesday and not on Wednesday, and
#:   the cost of being wrong the safe way is one `chmod`.
SECRET_PROFILE_KEYS = frozenset(
    {"credential", "token", "secret_access_key", "account_key", "sas_token", "client_secret"}
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
    #: Engine connection settings, per engine: ``{"trino": {"host": ...}}``.
    #: A host, a port, a user and a catalog name are **not secrets**, and this
    #: file is defined as everything that is not one -- so before ZMBNI-408 they
    #: had nowhere to live but a flag or `.env`, which meant either twenty
    #: characters of crontab per run or non-secret configuration sitting in the
    #: credentials file. Only the password-shaped things belong there.
    engines: dict[str, dict[str, str]] = field(default_factory=dict)
    #: Whether Zamboni reclaims storage on its own credentials rather than the
    #: catalog's. A policy, not a key.
    credential_use: str = "always"
    #: Catalog OAuth2 client credentials, ``client_id:client_secret``. A secret,
    #: and allowed here -- see the module docstring. The environment still wins.
    credential: str | None = None
    #: A catalog bearer token, as an alternative to :attr:`credential`.
    token: str | None = None
    #: Transport security for the catalog connection: ``ca_bundle`` (a path to
    #: trust) and ``insecure`` (skip verification). Catalog leg only -- the
    #: storage FileIO takes no TLS property.
    ssl: dict[str, str] = field(default_factory=dict)
    #: Object-store credentials, per provider: ``{"gcs": {"token": ...}}``.
    #: Which provider a run uses is decided by the table's own location, not by
    #: this block, so configuring two is refused rather than merged.
    storage: dict[str, dict[str, str]] = field(default_factory=dict)
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
    """The dotenv file to load, or None.

    ``--env``, then ``./.env``, then ``$ZAMBONI_ROOT/.env`` -- the same order
    :func:`find_profile` uses, and it did not used to. Only the working
    directory was searched, while docs/devops.md's multi-tenant layout puts
    ``.env`` at ``$ZAMBONI_ROOT`` and labels it "fleet-wide credentials". The
    documented layout therefore only worked when the working directory happened
    to *be* ``$ZAMBONI_ROOT``, which the cron line makes true and any other
    invocation does not -- and the symptom was not an error but a run with no
    credentials.

    **Finding nothing is not an error.** A container or systemd unit that
    injects secrets properly needs no dotenv file at all, and devops.md says so;
    failing here would break the deployment shape we recommend most.
    """
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise ProfileError(f"--env {explicit}: no such file")
        return path

    root_env = os.environ.get("ZAMBONI_ROOT")
    candidates = [
        (start or Path.cwd()) / ENV_NAME,
        (Path(root_env) if root_env else DEFAULT_ROOT) / ENV_NAME,
    ]
    # The first that exists *and is ours*. A foreign `.env` sharing the working
    # directory -- docker compose's, say -- must not mask the fleet's real one
    # two directories up.
    for candidate in candidates:
        if candidate.is_file() and env_ours(candidate):
            return candidate
    return None


#: Every variable this tool reads is prefixed. Enforced rather than assumed
#: (ZMBNI-1812): a `.env` in a working directory is very often shared with
#: docker compose, a framework, or another tool, and loading all of it would
#: mean Zamboni silently changing the environment of everything downstream of
#: it. Reading only our own keys makes the file safe to share; requiring the
#: prefix makes "is this variable ours" answerable without a list.
ENV_PREFIX = "ZAMBONI_"


def env_ours(path: Path) -> dict[str, str]:
    """The ``ZAMBONI_*`` entries of a dotenv file. Everything else is ignored."""
    from dotenv import dotenv_values

    return {
        k: v for k, v in dotenv_values(path).items() if v is not None and k.startswith(ENV_PREFIX)
    }


def check_env_permissions(path: Path) -> None:
    """Refuse to read a credential file that others can read.

    A hard error rather than a warning, and that is the point: a warning on a
    nightly cron job is a line in a log nobody opens. The file holds a catalog
    token; if the mode is wrong, the fix takes one command and the run should
    not proceed until it has been taken.

    The test is "no group or other bits", not "exactly 0600" -- 0400 is
    *stricter*, and rejecting a read-only credential file for being too safe
    would be an odd thing to do.
    """
    try:
        mode = path.stat().st_mode & 0o777
    except OSError as exc:  # pragma: no cover - raced with a delete
        raise ProfileError(f"{path}: cannot stat: {exc}") from exc
    if mode & 0o077:
        raise ProfileError(
            f"{path} is readable by group or other (mode {mode:03o}) and holds "
            f"credentials. Fix it and re-run:\n    chmod 600 {path}"
        )


def profile_holds_secrets(raw: dict[str, Any]) -> bool:
    """Whether this profile carries anything that makes it a credential file.

    Looks one level into the nested blocks as well as at the top, because
    `storage.azure.account_key` is every bit as much a secret as a top-level
    `credential` -- and a check that only saw the top level would pass the file
    that most needs the mode rule.
    """
    if SECRET_PROFILE_KEYS & set(raw):
        return True
    for block in raw.values():
        if isinstance(block, dict):
            if SECRET_PROFILE_KEYS & set(block):
                return True
            for inner in block.values():
                if isinstance(inner, dict) and SECRET_PROFILE_KEYS & set(inner):
                    return True
    return False


def check_profile_permissions(path: Path) -> None:
    """The same mode rule `.env` has, applied once the profile holds a secret.

    Deliberately identical to :func:`check_env_permissions`, including that it
    is a hard error: a warning on a nightly cron job is a line in a log nobody
    opens. What differs is when it applies -- a profile carrying no secrets is
    meant to be committed and world-readable, so checking it unconditionally
    would break every existing deployment to protect a file with nothing in it.
    """
    try:
        mode = path.stat().st_mode & 0o777
    except OSError as exc:  # pragma: no cover - raced with a delete
        raise ProfileError(f"{path}: cannot stat: {exc}") from exc
    if mode & 0o077:
        raise ProfileError(
            f"{path} holds credentials and is readable by group or other "
            f"(mode {mode:03o}). Fix it and re-run:\n    chmod 600 {path}\n"
            "Or move the secret to .env or the environment, and keep this file "
            "committable."
        )


def load_env(path: Path | None, *, explicit: bool = False) -> dict[str, str]:
    """Load the ``ZAMBONI_*`` entries of ``path`` into ``os.environ``.

    Real environment beats the file deliberately: a container or systemd unit
    that injects secrets properly should not be overridden by a stale ``.env``
    someone left in the working directory.

    A discovered file carrying no ``ZAMBONI_*`` keys is treated as **not ours**
    and ignored, permissions included -- it is somebody else's `.env` that
    happens to share a directory, and neither reading it nor complaining about
    its mode would be our business. Named explicitly with ``--env``, the same
    file is an error instead: the operator meant that file, and it is the wrong
    one.
    """
    if path is None:
        return {}

    values = env_ours(path)
    if not values:
        if explicit:
            raise ProfileError(
                f"--env {path}: no {ENV_PREFIX}* variables. Every variable this "
                f"tool reads is prefixed {ENV_PREFIX}; check the file, or drop "
                "the flag to use the environment as it stands."
            )
        return {}

    check_env_permissions(path)
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

    known = {
        "uri",
        "warehouse",
        "engine",
        "root",
        "operations",
        "tables",
        "credential_use",
        "trino",
        "spark",
        # Secrets, allowed here since they may legitimately be projected from a
        # Kubernetes Secret. Their presence turns on `.env`'s mode rule.
        "credential",
        "token",
        "storage",
        # Not a secret: a CA bundle is a path to a public certificate.
        "ssl",
    }
    if unknown := sorted(set(raw) - known):
        raise ProfileError(
            f"{path}: unknown key(s) {', '.join(unknown)}. Known keys: {', '.join(sorted(known))}"
        )

    operations = tuple(raw.get("operations") or DEFAULT_OPERATIONS)
    if bad := [op for op in operations if op not in DEFAULT_OPERATIONS]:
        raise ProfileError(
            f"{path}: unknown operation(s) {', '.join(bad)}. Known: {', '.join(DEFAULT_OPERATIONS)}"
        )

    engines: dict[str, dict[str, str]] = {}
    for engine_name in ("trino", "spark"):
        block = raw.get(engine_name)
        if block is None:
            continue
        if not isinstance(block, dict):
            raise ProfileError(f"{path}: '{engine_name}' must be a block of settings")
        allowed = ENGINE_SETTINGS[engine_name]
        if unknown := sorted(set(block) - allowed):
            raise ProfileError(
                f"{path}: {engine_name}: unknown key(s) {', '.join(unknown)}. "
                f"Known keys: {', '.join(sorted(allowed))}. Neither maintainer "
                "accepts a password or token, so there is no key for one here; "
                "catalog and object-store credentials go under `credential`, "
                "`token` or `storage`."
            )
        engines[engine_name] = {k: str(v) for k, v in block.items()}

    storage: dict[str, dict[str, str]] = {}
    storage_block = raw.get("storage") or {}
    if not isinstance(storage_block, dict):
        raise ProfileError(f"{path}: 'storage' must be a block, keyed by provider")
    if unknown := sorted(set(storage_block) - set(STORAGE_SETTINGS)):
        raise ProfileError(
            f"{path}: storage: unknown provider(s) {', '.join(unknown)}. "
            f"Known: {', '.join(sorted(STORAGE_SETTINGS))}"
        )
    for provider, settings in storage_block.items():
        if not isinstance(settings, dict):
            raise ProfileError(f"{path}: storage.{provider} must be a block of settings")
        allowed = STORAGE_SETTINGS[provider]
        if unknown := sorted(set(settings) - allowed):
            raise ProfileError(
                f"{path}: storage.{provider}: unknown key(s) {', '.join(unknown)}. "
                f"Known keys: {', '.join(sorted(allowed))}"
            )
        storage[provider] = {k: str(v) for k, v in settings.items()}
    if len(storage) > 1:
        # Refused rather than merged: a run uses one object store, and the
        # table's own location decides which credentials fit. Two blocks means
        # the operator expects something this cannot do.
        raise ProfileError(
            f"{path}: storage names more than one provider ({', '.join(sorted(storage))}). "
            "A run uses one object store; configure the one this warehouse lives in."
        )

    ssl_block = raw.get("ssl") or {}
    if not isinstance(ssl_block, dict):
        raise ProfileError(f"{path}: 'ssl' must be a block of settings")
    if unknown := sorted(set(ssl_block) - SSL_SETTINGS):
        raise ProfileError(
            f"{path}: ssl: unknown key(s) {', '.join(unknown)}. "
            f"Known keys: {', '.join(sorted(SSL_SETTINGS))}"
        )

    # Once it holds a secret it is a credential file, and gets `.env`'s mode
    # rule. Checked after parsing so a malformed profile fails on its contents
    # rather than on its permissions.
    if profile_holds_secrets(raw):
        check_profile_permissions(path)

    base = _from_environment(source=path)
    root = raw.get("root")
    return Profile(
        uri=raw.get("uri") or base.uri,
        warehouse=raw.get("warehouse") or base.warehouse,
        engine=raw.get("engine") or base.engine,
        credential_use=(
            _credential_use(raw["credential_use"])
            if raw.get("credential_use")
            else base.credential_use
        ),
        root=Path(root).expanduser() if root else base.root,
        operations=operations,
        engines=engines,
        ssl={k: str(v) for k, v in ssl_block.items()},
        credential=raw.get("credential") or base.credential,
        token=raw.get("token") or base.token,
        storage=storage,
        tables=tuple(raw.get("tables") or ()),
        source=path,
    )


def _credential_use(value: str) -> str:
    """Validated at load, like everything else here, never mid-run.

    A typo such as ``reclaim_only`` would otherwise fall through to "not always
    and not reclaim-only", i.e. silently behave as ``never`` -- turning a
    misspelling into "reclaim quietly stopped working".
    """
    from .session import CredentialUse

    allowed = [c.value for c in CredentialUse]
    if value not in allowed:
        raise ProfileError(
            f"credential_use: {value!r} is not one of {allowed}. It decides whether "
            "Zamboni reclaims storage on its own credentials or the catalog's."
        )
    return value


def _from_environment(*, source: Path | None) -> Profile:
    root = os.environ.get("ZAMBONI_ROOT")
    return Profile(
        uri=os.environ.get("ZAMBONI_URI"),
        warehouse=os.environ.get("ZAMBONI_WAREHOUSE"),
        engine=os.environ.get("ZAMBONI_ENGINE", "local"),
        credential_use=_credential_use(os.environ.get("ZAMBONI_CREDENTIAL_USE", "always")),
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
    applied = load_env(env_file, explicit=bool(env_path))
    # Report the file only when it was actually used, so `--verbose` and the
    # warnings do not name a foreign `.env` we deliberately ignored.
    used = env_file if applied else None
    return load_profile(find_profile(profile_path, start=start)), used
