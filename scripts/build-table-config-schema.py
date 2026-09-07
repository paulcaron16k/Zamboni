#!/usr/bin/env -S uv run --script
# SPDX-License-Identifier: Apache-2.0
"""Regenerate the shipped JSON Schema for ``table-config.json``.

The schema is derived from :mod:`zamboni.tableconfig`, so it goes stale the
moment a field is added there. `test_the_shipped_schema_matches_a_fresh_generation`
fails when that happens and names this script; run it and commit the result.

Same arrangement as `scripts/build-executable.py` and `bin/`: a generated file is
committed so an installed wheel carries it without importing a generator, and a
test rather than trust keeps it current.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from zamboni.tableconfig_schema import SPEC_PATH, build_schema  # noqa: E402


def main() -> int:
    text = json.dumps(build_schema(), indent=2) + "\n"
    before = SPEC_PATH.read_text() if SPEC_PATH.exists() else None
    SPEC_PATH.write_text(text)
    rel = SPEC_PATH.relative_to(ROOT)
    if before == text:
        print(f"{rel}: already current")
    else:
        print(f"{rel}: {'updated' if before else 'written'}, {len(text)} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
