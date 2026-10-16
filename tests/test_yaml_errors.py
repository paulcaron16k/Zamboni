# SPDX-License-Identifier: Apache-2.0
"""A YAML file that fails to parse is described, never quoted (ZMBNI-207).

`zamboni.yml` may hold credentials, and PyYAML's message for a parse error ends
with a snippet of the offending line: a missing closing quote after
`credential: "client:...` printed the secret on stderr. Its `problem` text
quotes content too -- an undefined alias, an unknown tag and a duplicate anchor
are each named -- so every case below puts the secret where PyYAML would echo
it. Asserted on the whole formatted traceback, because a chained cause prints
there and in any `logger.exception`.
"""

from __future__ import annotations

import traceback

import pytest

from zamboni.fleet import FleetConfig, FleetConfigError
from zamboni.settings import ProfileError, load_profile

SECRET = "zmbni207-SECRET-value"

#: Each malformed in a way PyYAML reports differently, with the secret placed
#: where that report would show it (PyYAML 6.0.3).
MALFORMED = {
    "unclosed quote": f'uri: http://x\ncredential: "client:{SECRET}\nwarehouse: w\n',
    "undefined alias": f"token: *{SECRET}\n",
    "unknown tag": f"token: !{SECRET} x\n",
    "duplicate anchor": f"a: &{SECRET} 1\nb: &{SECRET} 2\n",
    "unknown escape": f'token: "{SECRET}\\q"\n',
    "tab": f"a:\n\tb: {SECRET}\n",
    "mapping in a value": f"token: {SECRET}: x\n",
    "bad indent": f"a: 1\n b: {SECRET}\n",
}

#: The line PyYAML stops at, 1-based, so the message is still worth reading.
LINE = {
    "unclosed quote": 4,
    "undefined alias": 1,
    "unknown tag": 1,
    "duplicate anchor": 2,
    "unknown escape": 1,
    "tab": 2,
    "mapping in a value": 1,
    "bad indent": 2,
}


def load_as_profile(path):
    load_profile(path)


def load_as_fleet(path):
    FleetConfig.load(path)


LOADERS = {
    "profile": (load_as_profile, ProfileError),
    "fleet": (load_as_fleet, FleetConfigError),
}


@pytest.mark.parametrize("loader", list(LOADERS))
@pytest.mark.parametrize("case", list(MALFORMED))
def test_a_parse_error_names_the_line_and_not_the_content(tmp_path, loader, case):
    load, error = LOADERS[loader]
    path = tmp_path / "file.yml"
    path.write_text(MALFORMED[case])
    path.chmod(0o600)

    with pytest.raises(error) as caught:
        load(path)

    rendered = "".join(traceback.format_exception(caught.value))
    assert SECRET not in rendered
    assert f"line {LINE[case]}," in str(caught.value)


def test_the_cases_would_leak_without_the_description():
    """Guards the table above: each case must be one where PyYAML echoes the secret."""
    import yaml

    for case, text in MALFORMED.items():
        with pytest.raises(yaml.YAMLError) as caught:
            yaml.safe_load(text)
        assert SECRET in str(caught.value), case
