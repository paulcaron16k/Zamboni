# SPDX-License-Identifier: Apache-2.0
"""The `zamboni` console script: answer the probe verbs without the engine.

`zamboni service-status --probe liveness` runs as a Kubernetes exec probe, and
Kubernetes gives an exec probe one second by default (`timeoutSeconds`). Going
through `zamboni.cli` it took 1.4-1.5 s (`/usr/bin/time`, three runs,
2026-10-02) -- and nearly all of that was importing PyIceberg, DuckDB and Arrow
for a command that reads one JSON file (ZMBNI-154). A probe that cannot answer
inside its timeout restarts the pod, forever, and never says why.

So the console script starts here. The probe verbs -- `service-status`,
`config-reload` -- and `--version` are parsed and answered by this module, which
imports nothing from the engine. Every other command is handed to
`zamboni.cli.main` unchanged.

**One definition, two parsers.** The global options and the probe verbs'
arguments are defined below and *registered* by `zamboni.cli`'s full parser too,
so `zamboni --help` lists them and the two parsers cannot disagree on a flag.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

#: Commands this module answers itself. Everything else goes to `zamboni.cli`.
PROBE_COMMANDS = frozenset({"service-status", "config-reload"})

#: Global options that take a value, so the router can skip it when looking for
#: the command. Kept beside `add_global_options`, which defines them.
_GLOBAL_OPTIONS_WITH_VALUE = frozenset({"--profile", "--env"})


def add_global_options(parser: argparse.ArgumentParser) -> None:
    """The options every `zamboni` command accepts before its name."""
    from . import version_banner

    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--profile",
        help="non-secret configuration. Default: ./zamboni.yml, then "
        "$ZAMBONI_ROOT/zamboni.yml. See docs/devops.md.",
    )
    parser.add_argument(
        "--env",
        help="dotenv file holding credentials. Default: ./.env. Cron gives a job "
        "almost no environment, and a crontab is a poor place for a secret.",
    )
    parser.add_argument("--version", action="version", version=version_banner())


def add_probe_commands(sub: argparse._SubParsersAction) -> None:
    """Register `service-status` and `config-reload` on a subparser set."""
    cr = sub.add_parser(
        "config-reload",
        help="tell a running `zamboni serve` to reload its fleet file now",
        description=(
            "Send SIGHUP to the service named by its pid file. The service polls "
            "the fleet file anyway; this is for deployments that are not "
            "containers, and loads at once rather than at the next poll. An "
            "invalid file leaves the running config in place -- check the "
            "service's log for the result. Refuses to signal a pid that is not "
            "a zamboni process, because a stale pid file and SIGHUP's default "
            "action would otherwise terminate an unrelated process."
        ),
    )
    cr.add_argument(
        "--pid-file",
        help="the pid file `zamboni serve` writes. Default: $ZAMBONI_ROOT/run/serve.pid",
    )

    ss = sub.add_parser(
        "service-status",
        help="report a running `zamboni serve`'s state, or answer a Kubernetes probe",
        description=(
            "Reads the state file `zamboni serve` writes. With --probe, exits 0 or "
            "1 for an exec probe: liveness asserts only that the scheduler loop "
            "ticked recently; readiness that it is running on a valid config; "
            "startup that the first config load and capability probe finished. "
            "None of them checks a catalog -- an outage there is not fixed by a "
            "restart."
        ),
    )
    ss.add_argument(
        "--state-file",
        help="the state file `zamboni serve` writes. Default: $ZAMBONI_ROOT/run/serve.state.json",
    )
    ss.add_argument("--probe", choices=("liveness", "readiness", "startup"))


def run_dir(args: argparse.Namespace) -> Path:
    """Where `serve` keeps its pid file, state file and per-table scratch files."""
    return Path(args.zamboni_profile.root) / "run"


def run_probe_command(args: argparse.Namespace) -> int:
    """Answer `service-status` or `config-reload`. Needs `args.zamboni_profile`."""
    if args.command == "service-status":
        return _service_status(args)
    if args.command == "config-reload":
        return _config_reload(args)
    raise ValueError(f"not a probe command: {args.command!r}")


def _service_status(args: argparse.Namespace) -> int:
    """Print the service's state, or answer one probe with 0 or 1."""
    from .service import probe, read_state

    path = Path(args.state_file) if args.state_file else run_dir(args) / "serve.state.json"
    try:
        state = read_state(path)
    except (OSError, ValueError) as exc:
        print(f"no readable service state at {path}: {exc}", file=sys.stderr)
        return 1 if args.probe else 2
    if args.probe:
        ok, why = probe(state, args.probe)
        print(f"{args.probe}: {'ok' if ok else 'FAIL'} -- {why}")
        return 0 if ok else 1
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


def _config_reload(args: argparse.Namespace) -> int:
    from .reload import PidFileError, send_reload

    try:
        pid = send_reload(args.pid_file or run_dir(args) / "serve.pid")
    except PidFileError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"sent SIGHUP to zamboni pid {pid}; the service log reports the reload")
    return 0


def _command_of(argv: list[str]) -> str | None:
    """The command name in `argv`: the first token that is not a global option."""
    skip = False
    for token in argv:
        if skip:
            skip = False
            continue
        if token in _GLOBAL_OPTIONS_WITH_VALUE:
            skip = True
            continue
        if token.startswith("-"):
            continue
        return token
    return None


def main(argv: list[str] | None = None) -> int:
    """The console script. Probe verbs and `--version` here; the rest in `zamboni.cli`."""
    args_in = list(sys.argv[1:] if argv is None else argv)
    command = _command_of(args_in)
    if command in PROBE_COMMANDS or (command is None and "--version" in args_in):
        from . import settings

        parser = argparse.ArgumentParser(prog="zamboni")
        add_global_options(parser)
        add_probe_commands(parser.add_subparsers(dest="command", required=True))
        args = parser.parse_args(args_in)  # `--version` prints and exits here
        try:
            args.zamboni_profile, _ = settings.resolve(profile_path=args.profile, env_path=args.env)
        except settings.ProfileError as exc:
            parser.error(str(exc))
        return run_probe_command(args)

    from .cli import main as full_cli

    return full_cli(args_in)
