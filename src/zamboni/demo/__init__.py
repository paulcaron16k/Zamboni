# SPDX-License-Identifier: Apache-2.0
"""HIMS discharge maintenance demo.

Domain code for the demo: the only *package* in the distribution that carries
hospital vocabulary. It lives *inside* `zamboni` as a subpackage rather than
beside it as a top-level `himsdemo`.

(Package, not distribution. `README.md` is embedded verbatim as the wheel's
`dist-info/METADATA`, and it describes the HIMS demo at length -- so the broader
claim was measurably false, which a review pointed out by grepping the built
metadata.)

That reverses the original arrangement, whose stated reason was that a
maintenance library should not carry hospital vocabulary. The instinct was
sound and the remedy was wrong: keeping the vocabulary out of `zamboni` bought
nothing, because the same wheel shipped it either way, and it cost an
unnamespaced top-level import name for a teaching aid -- the exact objection
that renamed the `demo` console script to `zamboni-demo`. Separation of concerns
is what a subpackage boundary is for; the import namespace is not the place to
express it. ZMBNI-24.
"""
