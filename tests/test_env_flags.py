# SPDX-License-Identifier: Apache-2.0
"""A flag's ``ZAMBONI_*`` fallback, set in ``.env`` (#252).

``main`` parses before it loads ``.env`` -- where ``.env`` is, is itself a flag
-- so a flag that read its variable as an argparse default read it too early,
and a value set only in ``.env`` never arrived. Each case below is derived from
the flags `_env_argument` declared, through the same `_settle` step ``main``
runs, so a new flag is covered without being listed here.
"""

from __future__ import annotations

import argparse

import pytest

from zamboni import cli

#: A string no flag would take by accident, so its arrival proves where it came from.
REFUSED = "not-a-value-anything-accepts"


def leaves(parser: argparse.ArgumentParser, path: tuple[str, ...] = ()):
    """Each runnable command: its words, and the parser that reads its flags."""
    nested = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    if not nested:
        yield path, parser
    for action in nested:
        for name, subparser in action.choices.items():
            yield from leaves(subparser, (*path, name))


def runnable(path: tuple[str, ...], parser: argparse.ArgumentParser) -> list[str]:
    """``path`` plus a placeholder for everything the command requires."""
    argv = list(path)
    for action in parser._actions:
        if action.option_strings and action.required:
            argv += [action.option_strings[0], "x"]
        elif not action.option_strings and action.nargs != argparse.REMAINDER:
            argv.append(str(next(iter(action.choices))) if action.choices else "x")
    return argv


def declared() -> dict[str, tuple[argparse.Action, list[str]]]:
    """Every fallback variable: one flag that reads it, and a command line that has that flag."""
    found: dict[str, tuple[argparse.Action, list[str]]] = {}
    for path, parser in leaves(cli._build_parser()):
        for action, variable in cli._env_actions(parser):
            found.setdefault(variable, (action, runnable(path, parser)))
    return found


VARIABLES = declared()


def valid(action: argparse.Action) -> tuple[str, object]:
    """A value the flag accepts, as text and as the flag would hold it."""
    if isinstance(action, argparse.BooleanOptionalAction):
        return "true", True
    if action.choices is not None:
        choice = list(action.choices)[-1]
        return str(choice), choice
    if callable(action.type):
        return "7", action.type("7")
    return f"from-dotenv-{action.dest}", f"from-dotenv-{action.dest}"


@pytest.fixture
def workdir(tmp_path, monkeypatch, restore_environment):
    """A directory with nothing of the developer's in reach: no variable set,
    and a ``ZAMBONI_ROOT`` holding no ``.env`` or profile of its own."""
    monkeypatch.chdir(tmp_path)
    for variable in VARIABLES:
        monkeypatch.delenv(variable, raising=False)
    (tmp_path / "root").mkdir()
    monkeypatch.setenv("ZAMBONI_ROOT", str(tmp_path / "root"))
    return tmp_path


def dotenv(directory, **values: str) -> None:
    path = directory / ".env"
    path.write_text("".join(f"{k}={v}\n" for k, v in values.items()))
    path.chmod(0o600)


def settled(argv: list[str]) -> argparse.Namespace:
    parser = cli._build_parser()
    args = parser.parse_args(argv)
    cli._settle(parser, args)
    return args


def test_the_fallbacks_were_found():
    """The derivation below must not pass by finding nothing."""
    assert {"ZAMBONI_TRINO_HOST", "ZAMBONI_S3_ENDPOINT", "ZAMBONI_MAX_TEMP_DIRECTORY_BYTES"} <= set(
        VARIABLES
    ), "the three variables #252 was observed on"


def test_no_flag_reads_the_environment_when_the_parser_is_built(monkeypatch):
    """The cause of #252, refused for every flag: with every fallback variable
    set, the parser's defaults must be what they are with none set."""

    def defaults() -> list[tuple[str, object]]:
        return [
            (" ".join(path), action.dest, repr(action.default))
            for path, parser in leaves(cli._build_parser())
            for action in parser._actions
        ]

    for variable in VARIABLES:
        monkeypatch.delenv(variable, raising=False)
    unset = defaults()
    for variable in VARIABLES:
        monkeypatch.setenv(variable, "1")
    assert defaults() == unset


@pytest.mark.parametrize("variable", sorted(VARIABLES))
def test_a_value_only_in_dotenv_reaches_its_flag(workdir, variable):
    action, argv = VARIABLES[variable]
    text, expected = valid(action)
    dotenv(workdir, **{variable: text})

    assert getattr(settled(argv), action.dest) == expected


@pytest.mark.parametrize(
    "variable",
    sorted(v for v, (a, _) in VARIABLES.items() if callable(a.type) or a.choices is not None),
)
@pytest.mark.parametrize("source", ["dotenv", "process"])
def test_a_malformed_value_is_a_usage_error_naming_its_variable(
    workdir, monkeypatch, capsys, variable, source
):
    """Wherever it was set: the variable is what the operator has to fix."""
    _, argv = VARIABLES[variable]
    if source == "dotenv":
        dotenv(workdir, **{variable: REFUSED})
    else:
        monkeypatch.setenv(variable, REFUSED)

    with pytest.raises(SystemExit) as exc:
        settled(argv)
    assert exc.value.code == 2
    assert f"{variable}={REFUSED!r}" in capsys.readouterr().err


def test_an_empty_value_is_unset(workdir):
    """`VAR=` is how a dotenv template leaves a setting out, so it is not a
    malformed number."""
    action, argv = VARIABLES["ZAMBONI_MAX_TEMP_DIRECTORY_BYTES"]
    dotenv(workdir, ZAMBONI_MAX_TEMP_DIRECTORY_BYTES="")

    assert getattr(settled(argv), action.dest) is None


def test_a_root_set_in_dotenv_decides_where_the_profile_is_found(workdir, monkeypatch):
    """docs/devops.md §3: `ZAMBONI_ROOT` in `./.env` cannot move the search for
    `.env` itself, but the profile is looked for after the file is loaded."""
    monkeypatch.delenv("ZAMBONI_ROOT")  # the process's would beat the file's
    root = workdir / "elsewhere"
    root.mkdir()
    (root / "zamboni.yml").write_text("trino:\n  host: from-the-root-profile\n")
    dotenv(workdir, ZAMBONI_ROOT=str(root))
    action, argv = VARIABLES["ZAMBONI_TRINO_HOST"]

    args = settled(argv)
    assert args.zamboni_profile.source == root / "zamboni.yml"
    assert getattr(args, action.dest) == "from-the-root-profile"


LAYERS = ("flag", "process", "dotenv", "profile")


@pytest.mark.parametrize("highest", [*LAYERS, None])
def test_precedence_is_flag_then_process_then_dotenv_then_profile(workdir, monkeypatch, highest):
    """Each layer set along with every one below it, so each case shows that
    layer beating all of those."""
    action, argv = VARIABLES["ZAMBONI_TRINO_HOST"]
    present = LAYERS[LAYERS.index(highest) :] if highest else ()
    if "profile" in present:
        (workdir / "zamboni.yml").write_text("trino:\n  host: from-profile\n")
    if "dotenv" in present:
        dotenv(workdir, ZAMBONI_TRINO_HOST="from-dotenv")
    if "process" in present:
        monkeypatch.setenv("ZAMBONI_TRINO_HOST", "from-process")
    if "flag" in present:
        argv = [*argv, "--trino-host", "from-flag"]

    assert getattr(settled(argv), action.dest) == (f"from-{highest}" if highest else None)


def s3_profile(directory, *extra: str) -> None:
    """An S3 block complete enough to configure storage. It holds a secret, so 0600."""
    path = directory / "zamboni.yml"
    lines = ["endpoint: http://s3", "access_key_id: id", "secret_access_key: s", *extra]
    path.write_text("storage:\n  s3:\n" + "".join(f"    {line}\n" for line in lines))
    path.chmod(0o600)


def test_the_profile_region_is_used_when_nothing_else_names_one(workdir):
    """`--s3-region` used to default to us-east-1 on the flag itself, so
    `storage.s3.region` in the profile could never be reached."""
    s3_profile(workdir, "region: eu-west-3")
    _, argv = VARIABLES["ZAMBONI_S3_REGION"]

    assert cli._storage_from(settled(argv)).region == "eu-west-3"


def test_with_no_region_anywhere_it_is_still_us_east_1(workdir):
    s3_profile(workdir)
    _, argv = VARIABLES["ZAMBONI_S3_REGION"]

    assert cli._storage_from(settled(argv)).region == "us-east-1"
