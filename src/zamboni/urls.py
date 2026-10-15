# SPDX-License-Identifier: Apache-2.0
"""Refusing credentials written into a URL.

A URL is not a place for a secret in this tool, because a URL is the thing that
gets logged, printed in a ``repr`` and shown in an error. Every secret Zamboni
accepts has its own setting -- ``nats.password``, ``nats.token``, ``credential``
-- which is kept out of reprs and makes the file that holds it subject to the
``.env`` mode rule (``settings.check_profile_permissions``). A password inside
``nats://alice:pw@broker`` was none of those things: logged at every ``serve``
start, shown in ``repr(Profile)``, and invisible to the permission check,
which matches key names (ZMBNI-194, found by the 0.6.0 security review).

So the rule is a refusal at load rather than redaction at each place a URL is
shown: redaction has to be remembered at every new log line, and a refusal
does not.
"""

from __future__ import annotations

from urllib.parse import urlsplit


def carries_userinfo(url: str) -> bool:
    """Whether ``url`` names a user, a password or a token before its host.

    A URL without a scheme is read as a host, because that is how nats-py reads
    one: `Client._parse_server_uri` prefixes ``nats://`` to anything without a
    scheme, so ``alice:pw@broker:4222`` authenticates as alice exactly as the
    full URL would. `urlsplit` alone would read ``alice`` as the scheme there
    and find no userinfo at all.
    """
    return "@" in urlsplit(url if "://" in url else f"//{url}").netloc
