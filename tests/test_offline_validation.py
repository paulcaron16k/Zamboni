"""Validating a policy must not connect to anything (ZMBNI-33).

The story that asked for this diagnosed it as maintainer construction doing I/O.
It does not — and the tests below prove both halves, because the difference
decided the design: a form that took a `CatalogSession` would have kept the exact
failure the report was trying to remove.
"""

from __future__ import annotations

import socket

import pytest

from zamboni.maintainers import MaintenanceRequest, Operation, available, get
from zamboni.maintainers.trino import TrinoMaintainer
from zamboni.maintenance import validate_policy
from zamboni.tableconfig import TableConfig

TIGHT = {
    "version": 2,
    "warehouse": "acme",
    "defaults": {
        "retention": {
            "expire_snapshots": {"enabled": True, "max_snapshot_age_days": 1},
            "remove_orphan_files": {"enabled": True, "older_than_days": 1},
        }
    },
    "namespaces": {"db": {"tables": {"events": {}}}},
}


@pytest.fixture
def no_network(monkeypatch):
    """Any outbound connection raises, so an accidental one is loud.

    Asserting the *property* -- nothing connects -- rather than that a particular
    function was not called, which would pass the day the call moved.
    """
    attempts: list[object] = []

    class Blocked(socket.socket):
        def connect(self, address, *args, **kwargs):
            attempts.append(address)
            raise AssertionError(f"validation attempted a connection to {address}")

    monkeypatch.setattr(socket, "socket", Blocked)
    return attempts


def test_validate_policy_opens_no_connection(no_network):
    problems = validate_policy(TableConfig.from_dict(TIGHT), engine="trino")

    assert problems, "a 1-day retention must collide with Trino's 7-day floor"
    assert not no_network, "validation must not connect to anything"


@pytest.mark.parametrize("engine", available())
def test_every_engine_can_be_validated_offline(engine, no_network):
    """Derived from the registry, so a fourth engine is covered on arrival."""
    validate_policy(TableConfig.from_dict(TIGHT), engine=engine)
    assert not no_network, f"{engine} validation connected"


def test_it_catches_the_two_collisions_the_defaults_produce():
    """The concrete value: these are *our* defaults against *their* floors.

    Trino refuses both a 1-day expiry and a 1-day orphan interval against its
    7-day floors; Spark refuses the orphan interval against a hard-coded 24
    hours. Reported once per warehouse rather than failing every table
    identically, which is what the first integrator built by hand.
    """
    config = TableConfig.from_dict(TIGHT)

    trino = validate_policy(config, engine="trino")
    assert len(trino) == 2, trino
    assert any("expire-snapshots.min-retention" in p for p in trino)
    assert any("remove-orphan-files.min-retention" in p for p in trino)

    spark = validate_policy(config, engine="spark")
    assert len(spark) == 1, spark
    assert "under 24 hours" in spark[0]

    assert validate_policy(config, engine="local") == (), (
        "the local engine sets these floors itself, so it accepts its own defaults"
    )


def test_every_problem_names_its_table_and_operation():
    """A fleet run reports per warehouse; a problem has to say where it came from."""
    config = TableConfig.from_dict(TIGHT)
    for problem in validate_policy(config, engine="trino"):
        assert problem.startswith("db.events "), problem
        assert any(f" {op.value}:" in problem for op in Operation), problem


def test_an_unsupported_operation_is_not_reported_as_a_policy_problem():
    """Trino cannot remove dangling deletes at all.

    `maintain` skips that at exit 0, so reporting it here would make an engine
    limitation look like a misconfiguration -- and an operator would go looking
    for the setting to change.
    """
    problems = validate_policy(
        TableConfig.from_dict(TIGHT),
        engine="trino",
        operations=[Operation.REMOVE_DANGLING_DELETES],
    )
    assert problems == ()


def test_construction_was_never_the_problem(no_network, tmp_path):
    """The report's premise, tested rather than repeated.

    It said `TrinoMaintainer.__init__` "asks the server for its version". It reads
    `version` from an option. Both construction and the instance `validate()` are
    pure, so what connects is the *catalog session* a caller builds to pass in --
    which is why `validate_policy` takes none.
    """
    from zamboni import CatalogSession

    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    session = CatalogSession.for_local(warehouse_path=str(warehouse))
    maintainer = get("trino")(session, {"host": "203.0.113.1", "port": "9999"})
    request = MaintenanceRequest(
        retention=TableConfig.from_dict(TIGHT).for_table("db.events").retention
    )

    assert maintainer.validate(Operation.EXPIRE, request), "the floor collision is still caught"
    assert not no_network, "neither construction nor validate() may connect"


def test_the_classmethod_and_the_instance_agree(tmp_path):
    """One implementation, two entry points, so they cannot drift.

    `validate()` exists because the run loop holds a constructed maintainer
    already; it delegates to `validate_request()` with its own options. If the two
    ever disagreed, a fleet run and a pre-flight check would give different
    answers about the same policy, which is worse than having only one of them.
    """
    from zamboni import CatalogSession

    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    session = CatalogSession.for_local(warehouse_path=str(warehouse))
    request = MaintenanceRequest(
        retention=TableConfig.from_dict(TIGHT).for_table("db.events").retention
    )
    options = {"version": "400"}

    for engine in available():
        instance = get(engine)(session, options)
        for operation in Operation:
            if not instance.capabilities().of(operation).usable:
                continue
            assert instance.validate(operation, request) == type(instance).validate_request(
                operation, request, options=options
            ), f"{engine} {operation.value}: the two forms disagree"


def test_an_unknown_version_says_what_it_assumed():
    """The acceptance asks for this explicitly.

    `retain_last` arrived in Trino 479 and is the only version-dependent check.
    With no version the safe assumption is "absent", and the message must say so
    rather than silently dropping `min_snapshots_to_keep`.
    """
    config = {**TIGHT}
    config["defaults"] = {
        "retention": {
            "expire_snapshots": {
                "enabled": True,
                "max_snapshot_age_days": 30,
                "min_snapshots_to_keep": 5,
            }
        }
    }
    request = MaintenanceRequest(
        retention=TableConfig.from_dict(config).for_table("db.events").retention
    )

    unknown = TrinoMaintainer.validate_request(Operation.EXPIRE, request)
    assert any("of unknown version" in p for p in unknown), unknown

    known = TrinoMaintainer.validate_request(Operation.EXPIRE, request, options={"version": "483"})
    assert known == (), f"a new enough Trino has retain_last, so nothing to report: {known}"
