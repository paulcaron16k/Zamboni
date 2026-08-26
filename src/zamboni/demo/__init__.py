# SPDX-License-Identifier: Apache-2.0
"""HIMS discharge maintenance demo.

Domain code for the demo, and the only place in the distribution that carries
hospital vocabulary. It lives *inside* `zamboni` as a subpackage rather than
beside it as a top-level `himsdemo`.

That reverses the original arrangement, whose stated reason was that a
maintenance library should not carry hospital vocabulary. The instinct was
sound and the remedy was wrong: keeping the vocabulary out of `zamboni` bought
nothing, because the same wheel shipped it either way, and it cost an
unnamespaced top-level import name for a teaching aid -- the exact objection
that renamed the `demo` console script to `zamboni-demo`. Separation of concerns
is what a subpackage boundary is for; the import namespace is not the place to
express it. ZMBNI-24.
"""
