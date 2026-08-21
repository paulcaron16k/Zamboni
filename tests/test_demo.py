"""The HIMS demo, end to end.

These tests run against a temporary copy of the committed CSVs, so they never
touch the checked-in catalog or warehouse a developer may be mid-demo with.
"""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from himsdemo.cli import main
from himsdemo.state import TOTAL_DAYS, DemoState
from zamboni import CatalogSession
from zamboni.profile import profile_table

SOURCE = Path(__file__).resolve().parent.parent / "data" / "healthims"
EVS_EVENTS = {
    "bed_cleaning_required",
    "cleaning_priority_assigned",
    "bed_cleaning_started",
    "bed_cleaned",
}


def _copy_inputs(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for name in ("table_schema.json", "table-config.json"):
        shutil.copy(SOURCE / name, root / name)
    for day in range(1, TOTAL_DAYS + 1):
        shutil.copytree(SOURCE / f"day{day}", root / f"day{day}", dirs_exist_ok=True)
    return root


@pytest.fixture
def demo_root(tmp_path) -> Path:
    """A scratch copy of the demo inputs. Inputs only -- no catalog, no state."""
    return _copy_inputs(tmp_path / "healthims")


@pytest.fixture
def ingested_root(demo_root) -> Path:
    """A root with two days ingested, built fresh for this test.

    Deliberately *not* a copy of a shared template: a SQLite Iceberg catalog
    stores absolute `metadata_location` URIs and the metadata stores absolute
    file paths, so a copied catalog still points at the original warehouse.
    Tests sharing one that way would read and commit into each other's state,
    and would start failing the moment maintenance learns to delete files.

    Two days is enough to have something to compact (~24 event files) and is
    cheaper than the five-day run the progression test already covers.
    """
    run(demo_root, "clear")
    for _ in range(2):
        run(demo_root, "next-day")
    return demo_root


def run(root: Path, *argv: str) -> int:
    # --root is a top-level flag, so it precedes the subcommand.
    return main(["--root", str(root), *argv])


def open_tables(root: Path) -> tuple[CatalogSession, dict]:
    session = CatalogSession.for_local(
        warehouse_path=str(root / "iceberg_warehouse"),
        uri=f"sqlite:///{root / 'iceberg_catalog.db'}",
        name="healthims",
    )
    tables = {
        name: session.catalog.load_table(f"healthims.{name}")
        for name in ("hims_employees", "hims_discharge", "hims_events")
    }
    return session, tables


def content(tbl) -> list[str]:
    """Every row as a comparable string, order-independent."""
    arrow = tbl.scan().to_arrow()
    cols = sorted(arrow.column_names)
    return sorted(
        str(row) for row in zip(*[arrow.column(c).to_pylist() for c in cols], strict=True)
    )


# -- the source data -----------------------------------------------------


def read_all(name: str) -> list[dict]:
    rows = []
    for day in range(1, TOTAL_DAYS + 1):
        with (SOURCE / f"day{day}" / name).open(newline="") as fh:
            for row in csv.DictReader(fh):
                row["_day"] = day
                rows.append(row)
    return rows


def test_generator_output_is_deterministic(tmp_path):
    """Regenerating must not change a byte, or the committed data is not review-able.

    Generated into a temp directory rather than over the committed CSVs: a test
    that rewrites the repository's own data leaves it corrupted whenever the
    generator regresses, which is precisely when you least want that.
    """
    out = tmp_path / "regenerated"
    subprocess.run(
        [sys.executable, "scripts/generate_healthims_data.py", "--out", str(out)],
        cwd=SOURCE.parent.parent,
        check=True,
        capture_output=True,
    )
    committed = {
        p.relative_to(SOURCE).as_posix(): p.read_bytes() for p in sorted(SOURCE.glob("day*/*.csv"))
    }
    regenerated = {
        p.relative_to(out).as_posix(): p.read_bytes() for p in sorted(out.glob("day*/*.csv"))
    }
    assert regenerated == committed


def test_three_discharges_are_cancelled_and_restarted_next_day():
    rows = read_all("discharges.csv")
    cancelled = {r["process_id"] for r in rows if r["status"] == "cancelled"}
    assert len(cancelled) == 3

    for process_id in cancelled:
        versions = sorted(
            (r for r in rows if r["process_id"] == process_id), key=lambda r: r["updated_at"]
        )
        days = sorted({r["_day"] for r in versions})
        assert len(days) == 2 and days[1] == days[0] + 1, (
            f"{process_id} must restart the day after it was cancelled, got days {days}"
        )
        # Same row, not a new one, and it reaches the terminal state in the end.
        assert versions[-1]["status"] == "discharged"
        assert len({r["created_at"] for r in versions}) == 1


def test_evs_events_occur_only_after_the_room_is_empty():
    completions = {
        r["process_id"]: r["completed_at"]
        for r in read_all("discharges.csv")
        if r["status"] == "discharged"
    }
    early = [
        e
        for e in read_all("events.csv")
        if e["event_name"] in EVS_EVENTS and e["occurred_at"] <= completions[e["process_id"]]
    ]
    assert not early, f"{len(early)} EVS event(s) at or before the patient left"


def test_discharge_stays_terminal_while_evs_events_continue():
    """The row is final at `discharged`; its event stream is not."""
    events = read_all("events.csv")
    discharges = read_all("discharges.csv")
    for process_id, completed in {
        r["process_id"]: r["completed_at"] for r in discharges if r["status"] == "discharged"
    }.items():
        after = [
            e for e in events if e["process_id"] == process_id and e["occurred_at"] > completed
        ]
        assert after, f"{process_id} has no post-discharge EVS events"
        later_versions = [
            r for r in discharges if r["process_id"] == process_id and r["updated_at"] > completed
        ]
        assert not later_versions, "the discharge row changed after it was discharged"


def test_event_names_match_the_catalogue():
    # Only the second cell of a numbered catalogue row is an event name.
    # Accepting any backticked snake_case token would also swallow column
    # names, making the subset assertion nearly vacuous.
    documented = set()
    doc = (SOURCE / "HIMS_Discharge_Process_Events.md").read_text()
    for line in doc.splitlines():
        cells = [c.strip() for c in line.split("|")]
        if len(cells) > 3 and cells[1].isdigit() and cells[2].startswith("`"):
            documented.add(cells[2].strip("`"))
    assert len(documented) == 15, f"expected 15 catalogued events, parsed {sorted(documented)}"
    used = {e["event_name"] for e in read_all("events.csv")}
    assert used <= documented, f"undocumented event name(s): {sorted(used - documented)}"


# -- the demo commands ---------------------------------------------------


def test_clear_starts_from_nothing(demo_root, capsys):
    assert run(demo_root, "clear") == 0
    state = DemoState.load(demo_root)
    assert state.days_ingested == 0
    assert not state.catalog_path.exists()


def test_next_day_advances_and_stops_after_five(demo_root, capsys):
    run(demo_root, "clear")
    for day in range(1, TOTAL_DAYS + 1):
        assert run(demo_root, "next-day") == 0
        assert DemoState.load(demo_root).days_ingested == day
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    before = tables["hims_events"].metadata.current_snapshot_id
    session.close()

    assert run(demo_root, "next-day") == 0
    assert "No More Data" in capsys.readouterr().out
    assert DemoState.load(demo_root).days_ingested == TOTAL_DAYS

    session, tables = open_tables(demo_root)
    assert tables["hims_events"].metadata.current_snapshot_id == before
    session.close()


def test_mode_refuses_to_switch_once_data_is_ingested(demo_root, capsys):
    run(demo_root, "clear")
    run(demo_root, "next-day")
    capsys.readouterr()

    assert run(demo_root, "mode", "mor") == 2
    assert "refusing to switch" in capsys.readouterr().err
    assert DemoState.load(demo_root).write_mode == "cow"


def test_read_only_commands_do_not_create_a_catalog(demo_root, capsys):
    """`status` and `query` must not bring a catalog into existence.

    Comparing snapshot ids does not catch this -- they are None either way.
    The observable is the catalog file itself.
    """
    run(demo_root, "clear")
    state = DemoState.load(demo_root)
    assert not state.catalog_path.exists()

    assert run(demo_root, "status") == 0
    assert run(demo_root, "query") == 0
    out = capsys.readouterr().out

    assert not state.catalog_path.exists(), "a read-only command created the catalog"
    assert "No tables yet" in out or "nothing ingested yet" in out


def test_status_and_query_change_nothing(demo_root, capsys):
    run(demo_root, "clear")
    run(demo_root, "next-day")
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    before = {n: t.metadata.current_snapshot_id for n, t in tables.items()}
    session.close()

    assert run(demo_root, "status") == 0
    assert run(demo_root, "query") == 0
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    assert {n: t.metadata.current_snapshot_id for n, t in tables.items()} == before
    session.close()


def test_maintenance_preserves_every_row(ingested_root, capsys):
    """The property the whole demo rests on."""
    demo_root = ingested_root
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    before = {name: content(tbl) for name, tbl in tables.items()}
    before_files = {name: len(profile_table(tbl).live_files) for name, tbl in tables.items()}
    session.close()

    assert run(demo_root, "maintenance") == 0
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    after = {name: content(tbl) for name, tbl in tables.items()}
    after_files = {name: len(profile_table(tbl).live_files) for name, tbl in tables.items()}
    session.close()

    assert after == before, "maintenance changed the data"
    for name in ("hims_discharge", "hims_events"):
        assert after_files[name] < before_files[name], f"{name} was not compacted"


def test_maintenance_evolves_days_into_months(ingested_root, capsys):
    """The demo data is older than the 90-day rule, so evolution should fire."""
    demo_root = ingested_root
    run(demo_root, "maintenance")
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    try:
        for name in ("hims_discharge", "hims_events"):
            tbl = tables[name]
            live = profile_table(tbl).live_files
            specs = {f.spec_id for f in live}
            assert specs, f"{name} has no live files"
            for spec_id in specs:
                spec = tbl.metadata.specs()[spec_id]
                assert str(spec.fields[0].transform) == "month", (
                    f"{name} spec {spec_id} is {spec.fields[0].transform}, expected month"
                )
    finally:
        session.close()


def test_mor_mode_produces_delete_files(demo_root, capsys):
    run(demo_root, "clear")
    assert run(demo_root, "mode", "mor") == 0
    for _ in range(2):
        run(demo_root, "next-day")
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    try:
        profile = profile_table(tables["hims_discharge"])
        assert profile.position_delete_files > 0, "mor mode wrote no position deletes"
        # Physical rows exceed live rows -- that is what makes it merge-on-read.
        physical = sum(f.record_count for f in profile.live_files)
        assert physical > tables["hims_discharge"].scan().count()
    finally:
        session.close()


def test_mor_status_says_the_deletes_are_simulated(demo_root, capsys):
    """PyIceberg cannot write these; the demo must not imply otherwise."""
    run(demo_root, "clear")
    run(demo_root, "mode", "mor")
    run(demo_root, "next-day")
    capsys.readouterr()

    run(demo_root, "status")
    out = capsys.readouterr().out
    assert "simulate what Spark or Flink would emit" in out


def test_mor_maintenance_preserves_rows_and_reports_dangling(demo_root, capsys):
    run(demo_root, "clear")
    run(demo_root, "mode", "mor")
    for _ in range(3):
        run(demo_root, "next-day")
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    before = content(tables["hims_discharge"])
    session.close()

    run(demo_root, "maintenance")
    out = capsys.readouterr().out

    session, tables = open_tables(demo_root)
    try:
        assert content(tables["hims_discharge"]) == before
        assert "delete file(s) now dangling" in out
        # And then cleared in the same run, which is the point of doing it here
        # rather than leaving the operator to notice.
        assert "removed" in out and "delete file(s)" in out
        assert profile_table(tables["hims_discharge"]).position_delete_files == 0
    finally:
        session.close()


def test_schema_and_layout_configs_agree(demo_root):
    """Every table the layout config names must exist in the schema."""
    schema = json.loads((demo_root / "table_schema.json").read_text())
    layout = json.loads((demo_root / "table-config.json").read_text())
    namespace = schema["namespace"]
    layout_tables = {
        f"{ns}.{name}": block
        for ns, entry in layout["namespaces"].items()
        for name, block in entry["tables"].items()
    }
    declared = {f"{namespace}.{name}" for name in schema["tables"]}
    assert set(layout_tables) == declared
    assert layout["warehouse"], "the layout config must name the warehouse it describes"

    # And every column an ordering or partition names must be a real column.
    for identifier, block in layout_tables.items():
        columns = {c["name"] for c in schema["tables"][identifier.split(".")[1]]["columns"]}
        for pf in block.get("partition", []):
            assert pf["column"] in columns, f"{identifier}: no column {pf['column']}"
        ordering = block.get("ordering") or {}
        for key in (ordering.get("zorder") or {}).get("columns", []):
            assert key in columns, f"{identifier}: z-order names missing column {key}"


def test_cow_and_mor_reach_identical_data(demo_root, tmp_path, capsys):
    """The central claim of the mode switch: same rows, different physics.

    Each mode was previously only ever compared against itself, so a write-mode
    bug that corrupted both consistently would have gone unnoticed.
    """
    mor_root = _copy_inputs(tmp_path / "mor")

    run(demo_root, "clear")
    for _ in range(2):
        run(demo_root, "next-day")

    run(mor_root, "clear")
    run(mor_root, "mode", "mor")
    for _ in range(2):
        run(mor_root, "next-day")
    capsys.readouterr()

    cow_session, cow_tables = open_tables(demo_root)
    mor_session, mor_tables = open_tables(mor_root)
    try:
        for name in ("hims_employees", "hims_discharge", "hims_events"):
            assert content(cow_tables[name]) == content(mor_tables[name]), (
                f"{name} differs between cow and mor"
            )
        # ... but merge-on-read got there with delete files and cow did not.
        assert profile_table(mor_tables["hims_discharge"]).position_delete_files > 0
        assert profile_table(cow_tables["hims_discharge"]).position_delete_files == 0
    finally:
        cow_session.close()
        mor_session.close()


def test_query_reports_fewer_files_after_maintenance(ingested_root, capsys):
    """`query`'s headline number must actually move, and its rows must not.

    The command previously had no assertion beyond exit code 0, which is how
    three separate reporting defects survived earlier review rounds.
    """
    from himsdemo import queries

    session, tables = open_tables(ingested_root)
    try:
        before = queries.run_all(session.con, tables)
    finally:
        session.close()

    run(ingested_root, "maintenance")
    capsys.readouterr()

    session, tables = open_tables(ingested_root)
    try:
        after = queries.run_all(session.con, tables)
    finally:
        session.close()

    for was, now in zip(before, after, strict=True):
        assert now.files_scanned < was.files_scanned, (
            f"{was.query.title}: files scanned did not fall "
            f"({was.files_scanned} -> {now.files_scanned})"
        )
        assert now.rows.to_pylist() == was.rows.to_pylist(), (
            f"{was.query.title}: maintenance changed the answer"
        )


def test_mor_query_counts_include_delete_files(demo_root, capsys):
    """Counting only data files understates merge-on-read by roughly half."""
    from himsdemo import queries

    run(demo_root, "clear")
    run(demo_root, "mode", "mor")
    for _ in range(2):
        run(demo_root, "next-day")
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    try:
        results = queries.run_all(session.con, tables)
    finally:
        session.close()

    discharge_queries = [r for r in results if "hims_discharge" in r.query.reads]
    assert discharge_queries, "no query reads the table that has delete files"
    assert any(r.delete_files > 0 for r in discharge_queries), (
        "delete files are not counted, so the reported I/O understates merge-on-read"
    )
    for r in discharge_queries:
        assert r.files_scanned == r.data_files + r.delete_files


# -- reclaiming storage --------------------------------------------------


def parquet_on_disk(root: Path) -> dict[str, int]:
    """Parquet files physically present, per table.

    Counted straight off the filesystem rather than through `stats`, so a bug
    in the reporting cannot make the reporting's own test pass.
    """
    warehouse = root / "iceberg_warehouse" / "healthims"
    counts = {
        name: len(list((warehouse / name).rglob("*.parquet")))
        for name in ("hims_employees", "hims_discharge", "hims_events")
    }
    # A wrong path would count zero everywhere and make every assertion below
    # vacuously true -- which is exactly how this test first passed.
    assert all(counts.values()), f"no parquet under {warehouse}: {counts}"
    return counts


def test_maintenance_alone_frees_no_storage(ingested_root, capsys):
    """Compaction supersedes files; it does not delete them.

    The demo's most misleading possible outcome would be a falling `data files`
    line beside unchanged disk usage with nothing saying so.
    """
    before = parquet_on_disk(ingested_root)
    assert run(ingested_root, "maintenance") == 0
    out = capsys.readouterr().out
    after = parquet_on_disk(ingested_root)

    assert after["hims_discharge"] >= before["hims_discharge"], (
        "the default run deleted files; the age guard should have stopped it"
    )
    assert "age guard" in out, "nothing told the operator why storage did not fall"
    assert "on disk" in out and "superseded" in out


def test_reclaim_now_frees_storage_without_losing_a_row(ingested_root, capsys):
    """The other half: with the guard lifted, disk usage matches what is live."""
    run(ingested_root, "maintenance")
    capsys.readouterr()

    session, tables = open_tables(ingested_root)
    before = {name: content(tbl) for name, tbl in tables.items()}
    session.close()
    before_disk = parquet_on_disk(ingested_root)

    assert run(ingested_root, "maintenance", "--reclaim-now") == 0
    capsys.readouterr()

    after_disk = parquet_on_disk(ingested_root)
    assert after_disk["hims_discharge"] < before_disk["hims_discharge"], (
        f"--reclaim-now freed nothing: {before_disk} -> {after_disk}"
    )

    session, tables = open_tables(ingested_root)
    try:
        assert {name: content(tbl) for name, tbl in tables.items()} == before, (
            "reclaiming storage changed the data"
        )
        for name, tbl in tables.items():
            # Every live file must still be readable, not merely counted.
            assert tbl.scan().to_arrow().num_rows == len(before[name])
            # Copy-on-write writes no delete files, so on disk == live, exactly.
            live = {f.path for f in profile_table(tbl).live_files}
            assert len(live) == after_disk[name], (
                f"{name}: {after_disk[name]} parquet on disk but {len(live)} live"
            )
    finally:
        session.close()


def test_reclaim_now_keeps_live_delete_files(demo_root, capsys):
    """Merge-on-read: a live position-delete file must survive the sweep.

    It is unreferenced by the *data* file listing and lives under the same
    directory as the orphans, so treating it as one would silently resurrect
    deleted rows -- which the row comparison here would catch.
    """
    run(demo_root, "clear")
    run(demo_root, "mode", "mor")
    for _ in range(3):
        run(demo_root, "next-day")
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    before = content(tables["hims_discharge"])
    live_rows = tables["hims_discharge"].scan().count()
    session.close()

    assert run(demo_root, "maintenance", "--reclaim-now") == 0
    capsys.readouterr()

    session, tables = open_tables(demo_root)
    try:
        tbl = tables["hims_discharge"]
        assert content(tbl) == before, "reclaiming resurrected or lost rows"
        assert tbl.scan().count() == live_rows
        for path in {f.path for f in profile_table(tbl).live_files}:
            assert Path(path.replace("file://", "")).exists(), f"live file deleted: {path}"
    finally:
        session.close()


# -- the demo has to work from an install, not just a clone (ZMBNI-1809) --


def packaged_inputs() -> set[str]:
    """What `pyproject.toml` promises to ship, as repo-relative paths."""
    import tomllib

    repo = Path(__file__).resolve().parent.parent
    config = tomllib.loads((repo / "pyproject.toml").read_text())
    include = config["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]
    return set(include)


def test_every_file_the_demo_reads_is_packaged():
    """`zamboni-demo` used to ship as a console script that could not run.

    Installed, it resolved its inputs relative to the source tree and died on a
    FileNotFoundError pointing inside site-packages. The files are 212KB; the
    fix was to ship them. This keeps them shipped: add a day6, rename
    `table_schema.json`, and the wheel silently stops containing it otherwise.
    """
    promised = packaged_inputs()
    missing = []

    for name in ("table_schema.json", "table-config.json"):
        if f"data/healthims/{name}" not in promised:
            missing.append(name)
    for day in range(1, TOTAL_DAYS + 1):
        if f"data/healthims/day{day}" not in promised:
            missing.append(f"day{day}")

    assert not missing, f"the demo reads these and the wheel would not have them: {missing}"


def test_the_prose_and_the_generated_state_are_not_packaged():
    """Deliberately absent, for two different reasons.

    The requirements and the event catalogue are reference material the demo
    never opens -- a URL serves them better than a copy in everyone's
    site-packages. The warehouse and catalog are *generated*, 9MB after a run,
    and a wholesale include would ship whatever was lying in the directory when
    the wheel happened to be built.
    """
    promised = packaged_inputs()

    for path in promised:
        assert not path.endswith((".md", ".docx")), f"prose is packaged: {path}"
        assert "warehouse" not in path and not path.endswith(".db"), (
            f"generated state is packaged: {path}"
        )


def test_the_demo_finds_its_inputs_from_a_checkout():
    from himsdemo.cli import default_inputs

    assert (default_inputs() / "table_schema.json").is_file()


def test_reads_and_writes_are_separate_paths(tmp_path):
    """Installed, the inputs sit in a read-only package directory. A demo that
    writes its catalog beside them cannot be run twice, or by two users."""
    from himsdemo.state import DemoState

    state = DemoState(root=tmp_path / "work", inputs=tmp_path / "in")

    assert state.catalog_path.parent == tmp_path / "work"
    assert state.warehouse_path.parent == tmp_path / "work"
    assert state.schema_path.parent == tmp_path / "in"
    assert state.day_dir(1).parent == tmp_path / "in"


def test_the_demo_names_a_command_the_reader_can_actually_run(monkeypatch):
    """`./bin/zamboni-demo` does not exist for someone who pipx-installed this.

    Every "run X next" hint said it anyway, which sends a first-time user to a
    path that is not there -- the same class of defect as shipping data the
    installed copy could not find (ZMBNI-1809), just cheaper. ZMBNI-1811.
    """
    from himsdemo import cli

    monkeypatch.setattr(cli, "_CHECKOUT_INPUTS", Path("/nonexistent"))
    assert cli.invocation() == "zamboni-demo"

    monkeypatch.setattr(cli, "_CHECKOUT_INPUTS", Path(__file__).parent)
    assert cli.invocation() == "./bin/zamboni-demo"


def test_no_user_facing_hint_hardcodes_the_checkout_path():
    """The guard, because the literal is easy to reintroduce.

    Any `./bin/` inside a printed or raised string is wrong for an installed
    copy; it has to come from `invocation()`. Docstrings and comments are
    describing the checkout and are left alone.
    """
    import ast

    offenders = []
    for path in (Path(__file__).parent.parent / "src" / "himsdemo").glob("*.py"):
        tree = ast.parse(path.read_text())
        # `invocation()` is where the literal is *supposed* to live.
        allowed = {
            id(sub)
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "invocation"
            for sub in ast.walk(node)
        }
        docstrings = {
            id(node.body[0].value)
            for node in ast.walk(tree)
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef))
            and node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
        }
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and "./bin/" in node.value
                and id(node) not in docstrings
                and id(node) not in allowed
            ):
                offenders.append(f"{path.name}:{node.lineno}")

    assert not offenders, (
        f"{offenders} hardcode './bin/' in a runtime string; use invocation() so the "
        "hint matches how the demo was actually started"
    )
