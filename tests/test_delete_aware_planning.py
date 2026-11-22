"""The compaction planner counts delete files, not only data files (#288).

A partition whose keys moved elsewhere by upsert keeps its one or two data
files plus a growing pile of position deletes. The planner judged it on data
files alone -- "single candidate file", "2 candidate file(s) <
min_input_files=3" -- so the deletes were never applied, and every reader paid
the merge for good.

The selection rules are Iceberg Java's (`BinPackRewriteFilePlanner`): a file is
a candidate when its *file-scoped* deletes remove at least
``delete_ratio_threshold`` of its rows, or when at least
``delete_file_threshold`` delete files apply to it; and a partition holding one
is rewritten whatever ``min_input_files`` says.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from tests.conftest import SCHEMA, batch
from zamboni import CatalogSession, CompactionConfig, TableCompactor, maintain
from zamboni.deletes import find_dangling
from zamboni.maintainers import Operation
from zamboni.profile import profile_table
from zamboni.testing import add_position_deletes

ROWS = 20


@pytest.fixture
def moving(session: CatalogSession):
    """Identity-partitioned on category; tests load `old` and delete from it."""
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
    )
    return session.catalog.create_table(
        "db.moving", schema=SCHEMA, partition_spec=spec, properties={"format-version": "2"}
    )


def load_old(session, files: int) -> list[str]:
    """`files` loads of ROWS rows into partition `old`; returns their paths."""
    tbl = session.catalog.load_table("db.moving")
    for i in range(files):
        tbl.append(batch(i * ROWS, ROWS, "old"))
    tbl = session.catalog.load_table("db.moving")
    return sorted(t.file.file_path for t in tbl.scan().plan_files())


def delete_from(session, path: str, rows: int, *, file_scoped: bool = True, files: int = 1):
    """Delete the first `rows` rows of `path`, spread over `files` delete files."""
    per_file = -(-rows // files)
    for start in range(0, rows, per_file):
        tbl = session.catalog.load_table("db.moving")
        positions = list(range(start, min(start + per_file, rows)))
        add_position_deletes(tbl, path, positions, file_scoped=file_scoped)


def plan(session, **config):
    return TableCompactor(session, "db.moving", CompactionConfig(**config)).plan()


def planned(session, **config) -> set[str]:
    return {p for g in plan(session, **config).groups for p in g.paths}


def live_rows(session) -> int:
    return session.catalog.load_table("db.moving").scan().count()


# -- the issue's reproduction ----------------------------------------------------


def test_a_lone_file_mostly_deleted_is_compacted(session, moving):
    """1 data file, 75% of it deleted: was skipped as "single candidate file"."""
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=15)

    result = TableCompactor(session, "db.moving", CompactionConfig()).execute()

    assert result.rewritten_data_files == 1, plan(session).describe()
    assert live_rows(session) == ROWS - 15
    (rewritten,) = profile_table(session.catalog.load_table("db.moving")).live_files
    assert rewritten.record_count == ROWS - 15, "the deletes were applied, not carried"
    assert find_dangling(session.catalog.load_table("db.moving")).dangling_files == 1


def test_two_files_partly_deleted_are_compacted_below_min_input_files(session, moving):
    """2 data files, ~45% deleted, min_input_files=3: was "2 < min_input_files=3"."""
    paths = load_old(session, files=2)
    for path in paths:
        delete_from(session, path, rows=9)

    assert planned(session, min_input_files=3) == set(paths)


def test_the_deletes_are_dropped_by_the_same_maintenance_run(session, moving, tmp_path):
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=15)
    config = tmp_path / "table-config.json"
    config.write_text(
        json.dumps(
            {
                "version": 2,
                "warehouse": "local",
                "namespaces": {"db": {"tables": {"moving": {}}}},
            }
        )
    )

    report = maintain(session, table_config=config, commit=True)

    assert report.exit_code == 0, report.outcomes
    removed = next(o for o in report.outcomes if o.operation is Operation.REMOVE_DANGLING_DELETES)
    assert removed.ok and removed.result.removed == 1, removed.detail
    profile = profile_table(session.catalog.load_table("db.moving"))
    assert profile.position_delete_files == 0
    assert live_rows(session) == ROWS - 15


# -- where the ratio stops -------------------------------------------------------


def test_a_light_delete_leaves_the_partition_alone(session, moving):
    """Below the ratio nothing changes: the default keeps today's plan."""
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=2)

    assert planned(session) == set()
    assert plan(session).skipped == [
        ("spec=0 partition=('old')", "1 candidate file(s) < min_input_files=2")
    ], "the reason is the one it always was"


def test_the_ratio_is_inclusive_and_configurable(session, moving):
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=ROWS // 2)

    assert planned(session, delete_ratio_threshold=0.5) == {path}, "reaching it counts"
    assert planned(session, delete_ratio_threshold=0.6) == set()


def test_a_partition_scoped_delete_does_not_count_towards_the_ratio(session, moving):
    """Which file it deletes from is unknown without reading it; Java skips it too."""
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=15, file_scoped=False)

    assert planned(session) == set()


def test_the_delete_file_threshold_counts_every_delete_that_applies(session, moving):
    """It catches partition-scoped deletes, which the ratio cannot weigh."""
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=3, file_scoped=False, files=3)

    assert planned(session, delete_file_threshold=3) == {path}
    assert planned(session, delete_file_threshold=4) == set()
    assert planned(session, delete_file_threshold=None) == set(), "None turns it off"


def test_the_delete_file_threshold_is_on_by_default(session, moving):
    """Unlike Java's: without it, partition-scoped deletes are never compacted."""
    (path,) = load_old(session, files=1)
    default = CompactionConfig().delete_file_threshold
    assert default is not None
    delete_from(session, path, rows=default - 1, file_scoped=False, files=default - 1)
    assert planned(session) == set(), "one short of the default"

    tbl = session.catalog.load_table("db.moving")
    add_position_deletes(tbl, path, [default - 1], file_scoped=False)
    assert planned(session) == {path}


def test_a_file_at_target_size_is_rewritten_for_its_deletes(session, moving):
    """The size filter alone would never offer it: it is already big enough."""
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=15)
    size = profile_table(session.catalog.load_table("db.moving")).live_files[0].size_bytes

    assert planned(session, target_file_size_bytes=size) == {path}


def test_a_heavy_file_brings_its_small_neighbours_along(session, moving):
    """The partition is rewritten as a unit, as every compaction group is."""
    paths = load_old(session, files=2)
    delete_from(session, paths[0], rows=15)

    assert planned(session, min_input_files=3) == set(paths)


def test_a_deleted_row_that_was_rewritten_since_does_not_count(session, moving):
    """A delete applies only at or below its sequence number, as the scan says."""
    (path,) = load_old(session, files=1)
    delete_from(session, path, rows=15)
    TableCompactor(session, "db.moving", CompactionConfig()).execute()

    assert planned(session) == set(), "the rewritten file carries no deletes"


def test_rows_elsewhere_are_untouched(session, moving):
    (old,) = load_old(session, files=1)
    tbl = session.catalog.load_table("db.moving")
    tbl.append(pa.concat_tables([batch(1000, ROWS, "new")]))
    delete_from(session, old, rows=15)

    assert planned(session) == {old}
