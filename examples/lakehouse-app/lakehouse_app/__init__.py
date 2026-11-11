# SPDX-License-Identifier: Apache-2.0
"""The reference Lakehouse App: docs/maintenance-runtime.md §9, running.

Two tiers from one package, sharing only a database:

- the maintenance tier, ``python -m lakehouse_app.maintenance``, runs the
  Zamboni runtime (:mod:`lakehouse_app.maintenance`);
- the web tier serves the dashboard and never imports ``zamboni.runtime``
  (§9.2, §9.5 point 3).

Modules both tiers use -- :mod:`~lakehouse_app.db`,
:mod:`~lakehouse_app.registry`, :mod:`~lakehouse_app.request_table` -- must not
import ``zamboni.runtime`` either; tests/test_lakehouse_app.py checks.

Not part of the ``iceberg-zamboni`` distribution: an example, kept in this
repository so the contract it demonstrates is tested on every change.
"""
