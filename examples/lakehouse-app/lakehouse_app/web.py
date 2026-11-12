# SPDX-License-Identifier: Apache-2.0
"""The web tier: ``python -m lakehouse_app.web``.

docs/maintenance-runtime.md §9.2: the web tier never runs maintenance and
**never imports ``zamboni.runtime``** -- a test imports this module in a fresh
interpreter and checks (§9.5 point 3). It reads and writes rows:

- the warehouse registry: list, create, edit a schedule (``lakehouse_warehouse``);
- "run now": a request row (``zamboni_request``), which a maintenance replica
  turns into ``offer(reason="manual")``;
- the dashboard: what the maintenance tier wrote (``zamboni_runtime``,
  ``zamboni_run``, ``lakehouse_quarantine``) and the claims its replicas hold.

When the maintenance tier is down every page still renders, from the database,
under a "maintenance unavailable since ..." banner (:mod:`lakehouse_app.dashboard`).

**Deliberately minimal**: the standard library's WSGI server and hand-written
HTML, no framework, no JavaScript, no styling worth the name. The point is the
contract, not the UI. **No authentication either**: users and roles are the
app's to own (§9.1), and a reference app that invented them would teach the
wrong thing. Put it behind your own.

Settings, besides the database's (:mod:`lakehouse_app.db`):
``LAKEHOUSE_WEB_HOST`` (default ``0.0.0.0``) and ``LAKEHOUSE_WEB_PORT`` (8080).
"""

from __future__ import annotations

import html
import json
import logging
import os
import re
import sys
from collections.abc import Callable, Iterable
from datetime import datetime
from http import HTTPStatus
from socketserver import ThreadingMixIn
from typing import Any
from urllib.parse import parse_qs, quote
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from zamboni.tableconfig import TableConfig

from .dashboard import Availability, Dashboard, RunRow
from .db import Database
from .registry import Registry, RegistryError, WarehouseRow
from .request_table import RequestRow, RequestTable

logger = logging.getLogger("lakehouse_app.web")

StartResponse = Callable[..., Any]

STYLE = """
body { font-family: system-ui, sans-serif; margin: 1.5rem; max-width: 70rem; }
table { border-collapse: collapse; margin-bottom: 1.5rem; }
th, td { border-bottom: 1px solid #ccc; padding: .3rem .6rem; text-align: left; }
.banner { background: #fde68a; border: 1px solid #b45309; padding: .6rem 1rem; }
.failed { color: #b91c1c; }
form.inline { display: inline; }
textarea { width: 100%; font-family: monospace; }
"""


class HTTPError(Exception):
    def __init__(self, status: HTTPStatus, message: str) -> None:
        super().__init__(message)
        self.status = status


class App:
    """The WSGI application. One per process; every request opens its own
    connections, so it is safe under a threading server."""

    def __init__(self, database: Database) -> None:
        self.database = database
        self.registry = Registry(database)
        self.requests = RequestTable(database)
        self.dashboard = Dashboard(database)
        self.routes: list[tuple[str, re.Pattern[str], Callable[..., tuple[HTTPStatus, str]]]] = [
            ("GET", re.compile(r"/"), self.index),
            ("GET", re.compile(r"/healthz"), self.healthz),
            ("GET", re.compile(r"/readyz"), self.readyz),
            ("GET", re.compile(r"/warehouses/new"), self.new_warehouse),
            ("POST", re.compile(r"/warehouses"), self.create_warehouse),
            ("GET", re.compile(r"/warehouses/(?P<name>[^/]+)"), self.warehouse),
            ("POST", re.compile(r"/warehouses/(?P<name>[^/]+)/schedule"), self.edit_schedule),
            ("POST", re.compile(r"/warehouses/(?P<name>[^/]+)/run"), self.run_now),
            ("GET", re.compile(r"/requests/(?P<id>\d+)"), self.request),
        ]

    def __call__(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        method, path = environ["REQUEST_METHOD"], environ.get("PATH_INFO") or "/"
        try:
            for allowed, pattern, view in self.routes:
                if (match := pattern.fullmatch(path)) and method == allowed:
                    status, body = view(environ, **match.groupdict())
                    break
            else:
                raise HTTPError(HTTPStatus.NOT_FOUND, f"no page at {path}")
        except Redirect as redirect:
            start_response("303 See Other", [("Location", redirect.location)])
            return [b""]
        except HTTPError as exc:
            status, body = exc.status, self._page(exc.status.phrase, f"<p>{escape(exc)}</p>")
        except Exception:
            # A page that cannot reach the database says so, rather than
            # dropping the connection; the traceback goes to the log.
            logger.exception("%s %s", method, path)
            status = HTTPStatus.SERVICE_UNAVAILABLE
            body = page("Unavailable", "<p>The app's database could not be read.</p>")
        start_response(
            f"{status.value} {status.phrase}", [("Content-Type", "text/html; charset=utf-8")]
        )
        return [body.encode("utf-8")]

    # -- pages -------------------------------------------------------------------

    def healthz(self, environ: dict[str, Any]) -> tuple[HTTPStatus, str]:
        """Liveness: this process answers. Not the database, and never the
        maintenance tier -- a web pod restarted because maintenance is down
        would turn one outage into two."""
        return HTTPStatus.OK, "ok"

    def readyz(self, environ: dict[str, Any]) -> tuple[HTTPStatus, str]:
        """Readiness: the app's database answers, so a page can render. Still
        not the maintenance tier, for the reason liveness is not."""
        try:
            with self.database.connect() as conn:
                conn.execute("SELECT 1")
        except Exception as exc:
            logger.warning("not ready: %s", exc)
            return HTTPStatus.SERVICE_UNAVAILABLE, "the app's database is unreachable"
        return HTTPStatus.OK, "ok"

    def index(self, environ: dict[str, Any]) -> tuple[HTTPStatus, str]:
        d = self.dashboard
        warehouses = self.registry.warehouses()
        body = [
            "<h2>Warehouses</h2>",
            table(
                ["Warehouse", "Schedule", "Tables"],
                [
                    [
                        link(f"/warehouses/{quote(w.name)}", w.name),
                        escape(schedule_text(w.schedule)),
                        str(len(tables_of(w))),
                    ]
                    for w in warehouses
                ],
                empty="No warehouse registered yet.",
            ),
            '<p><a href="/warehouses/new">Register a warehouse</a></p>',
            "<h2>Running now</h2>",
            running_table(d.running()),
            "<h2>Maintenance replicas</h2>",
            table(
                ["Replica", "Phase", "Last tick", "Busy", "Queued", "Last error"],
                [
                    [
                        escape(r.holder),
                        escape(r.phase),
                        when(r.last_tick),
                        str(r.busy),
                        str(r.queued),
                        escape(r.last_error or ""),
                    ]
                    for r in d.runtimes()
                ],
                empty="No maintenance replica has ever reported.",
            ),
            "<h2>Quarantined</h2>",
            table(
                ["Warehouse", "Table", "Failures", "Until", "Last failure"],
                [
                    [
                        escape(q.warehouse),
                        escape(q.table),
                        str(q.failures),
                        when(q.until),
                        escape(q.last_failure or ""),
                    ]
                    for q in d.quarantined()
                ],
                empty="Nothing is quarantined.",
            ),
            "<h2>Recent runs</h2>",
            runs_table(d.runs(limit=25)),
            "<h2>Recent requests</h2>",
            requests_table(self.requests.recent(limit=15)),
        ]
        return HTTPStatus.OK, self._page("Lakehouse maintenance", "".join(body))

    def warehouse(self, environ: dict[str, Any], name: str) -> tuple[HTTPStatus, str]:
        row = self._warehouse(name)
        d = self.dashboard
        tables = tables_of(row)
        last: dict[str, RunRow] = {}
        for run in d.runs(warehouse=row.name, limit=500):
            last.setdefault(run.table, run)
        running = {c.table for c in d.running() if c.warehouse == row.name}
        body = [
            f"<p>Schedule: <code>{escape(schedule_text(row.schedule))}</code></p>",
            (
                f'<form method="post" action="/warehouses/{quote(row.name)}/schedule">'
                '<label>New schedule <input name="schedule" required></label> '
                "<button>Save</button></form>"
            ),
            (
                f'<form method="post" action="/warehouses/{quote(row.name)}/run">'
                "<button>Run now: every table</button></form>"
            ),
            "<h2>Tables</h2>",
            table(
                ["Table", "State", "Last run", "Result", ""],
                [
                    [
                        escape(t),
                        "running" if t in running else "",
                        when(last[t].finished_at) if t in last else "never",
                        result(last[t]) if t in last else "",
                        run_button(row.name, t),
                    ]
                    for t in tables
                ],
                empty="The table config names no table.",
            ),
            "<h2>Runs</h2>",
            runs_table(d.runs(warehouse=row.name, limit=50)),
        ]
        return HTTPStatus.OK, self._page(f"Warehouse {row.name}", "".join(body))

    def new_warehouse(self, environ: dict[str, Any]) -> tuple[HTTPStatus, str]:
        return HTTPStatus.OK, self._page("Register a warehouse", warehouse_form())

    def create_warehouse(self, environ: dict[str, Any]) -> tuple[HTTPStatus, str]:
        form = read_form(environ)
        try:
            row = WarehouseRow(
                name=form.get("name", "").strip(),
                schedule=parse_schedule(form.get("schedule", "")),
                table_config=parse_json(form.get("table_config", ""), "table config"),
                catalog=parse_json(form.get("catalog") or "{}", "catalog properties"),
            )
            self.registry.put(row)
        except (RegistryError, ValueError) as exc:
            return HTTPStatus.BAD_REQUEST, self._page(
                "Register a warehouse",
                f'<p class="failed">{escape(exc)}</p>' + warehouse_form(form),
            )
        raise Redirect(f"/warehouses/{quote(row.name)}")

    def edit_schedule(self, environ: dict[str, Any], name: str) -> tuple[HTTPStatus, str]:
        row = self._warehouse(name)
        try:
            self.registry.put(
                WarehouseRow(
                    row.name,
                    parse_schedule(read_form(environ).get("schedule", "")),
                    row.table_config,
                    row.catalog,
                )
            )
        except (RegistryError, ValueError) as exc:
            raise HTTPError(HTTPStatus.BAD_REQUEST, str(exc)) from None
        raise Redirect(f"/warehouses/{quote(row.name)}")

    def run_now(self, environ: dict[str, Any], name: str) -> tuple[HTTPStatus, str]:
        """Write a request row; a maintenance replica offers it. Works while
        maintenance is down too: the row waits, and the request page says so."""
        row = self._warehouse(name)
        table_name = read_form(environ).get("table")
        if table_name is not None and table_name not in tables_of(row):
            raise HTTPError(HTTPStatus.BAD_REQUEST, f"{row.name} has no table {table_name}")
        id = self.requests.submit(
            row.name, None if table_name is None else [table_name], requested_by="dashboard"
        )
        raise Redirect(f"/requests/{id}")

    def request(self, environ: dict[str, Any], id: str) -> tuple[HTTPStatus, str]:
        try:
            req = self.requests.get(id)
        except KeyError:
            raise HTTPError(HTTPStatus.NOT_FOUND, f"no request {id}") from None
        d = self.dashboard
        try:
            wanted = (
                list(req.tables)
                if req.tables is not None
                else tables_of(self.registry.warehouse(req.warehouse))
            )
        except KeyError:
            wanted = []
        running = {
            c.table: c
            for c in d.running()
            if c.warehouse == req.warehouse and c.acquired_at >= req.requested_at
        }
        finished: dict[str, Any] = {}
        for run in reversed(d.runs(warehouse=req.warehouse, since=req.requested_at, limit=500)):
            finished.setdefault(run.table, run)
        rows = []
        for t in wanted:
            if t in finished:
                state = f"finished {when(finished[t].finished_at)}: {result(finished[t])}"
            elif t in running:
                state = f"running since {when(running[t].acquired_at)}"
            elif req.handled_at is None:
                state = "waiting for a maintenance replica"
            else:
                state = "queued"
            rows.append([escape(t), state])
        body = [
            (
                f"<p>Asked for {escape(req.warehouse)} by {escape(req.requested_by)} "
                f"at {when(req.requested_at)}.</p>"
            ),
            f"<p>Outcome: {escape(req.outcome) if req.outcome else 'not yet handled'}</p>",
            table(["Table", "State"], rows, empty="No table to run."),
            "<p>"
            + link(f"/warehouses/{quote(req.warehouse)}", f"Back to {req.warehouse}")
            + "</p>",
        ]
        return HTTPStatus.OK, self._page(f"Request {req.id}", "".join(body))

    def _warehouse(self, name: str) -> WarehouseRow:
        try:
            return self.registry.warehouse(name)
        except KeyError:
            raise HTTPError(HTTPStatus.NOT_FOUND, f"no warehouse {name}") from None

    def _page(self, title: str, body: str) -> str:
        """Every page, under the banner when maintenance is unavailable: put
        here once, so no page can forget it."""
        try:
            top = banner(self.dashboard.availability())
        except Exception:  # the page is worth more than the banner
            logger.exception("cannot read the maintenance tier's state rows")
            top = ""
        return page(title, top + body)


class Redirect(Exception):
    def __init__(self, location: str) -> None:
        super().__init__(location)
        self.location = location


# -- rendering ----------------------------------------------------------------------


def escape(value: object) -> str:
    return html.escape(str(value), quote=True)


def page(title: str, body: str) -> str:
    return (
        f"<!doctype html><html><head><meta charset='utf-8'><title>{escape(title)}</title>"
        f"<style>{STYLE}</style></head><body>"
        f'<p><a href="/">Lakehouse maintenance</a></p><h1>{escape(title)}</h1>{body}'
        "</body></html>"
    )


def banner(availability: Availability) -> str:
    """Empty while a replica is ticking. Every page carries it, so a stopped
    maintenance tier is visible wherever someone is looking."""
    if availability.available:
        return ""
    if availability.since is None:
        why = ": no maintenance replica has reported yet"
    else:
        why = f" since {when(availability.since)}"
    return f'<div class="banner">Maintenance unavailable{why}.</div>'


def when(moment: datetime | None) -> str:
    return "" if moment is None else escape(moment.strftime("%Y-%m-%d %H:%M:%S %Z"))


def link(href: str, text: str) -> str:
    return f'<a href="{escape(href)}">{escape(text)}</a>'


def table(headings: list[str], rows: list[list[str]], *, empty: str) -> str:
    """Cells are HTML already: each caller escapes what it puts in one."""
    if not rows:
        return f"<p>{escape(empty)}</p>"
    head = "".join(f"<th>{escape(h)}</th>" for h in headings)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def result(run: Any) -> str:
    if run.dropped:
        return escape(f"not started: {run.dropped}")
    if run.died:
        return f'<span class="failed">{escape(run.died)}</span>'
    if run.exit_code:
        return f'<span class="failed">exit {run.exit_code}</span>'
    return "ok"


def runs_table(runs: list[Any]) -> str:
    return table(
        ["Finished", "Warehouse", "Table", "Reason", "Result"],
        [
            [when(r.finished_at), escape(r.warehouse), escape(r.table), escape(r.reason), result(r)]
            for r in runs
        ],
        empty="No run has finished yet.",
    )


def running_table(claims: list[Any]) -> str:
    return table(
        ["Warehouse", "Table", "Reason", "Since", "Replica"],
        [
            [
                escape(c.warehouse),
                escape(c.table),
                escape(c.reason),
                when(c.acquired_at),
                escape(c.holder),
            ]
            for c in claims
        ],
        empty="Nothing is running.",
    )


def requests_table(requests: list[RequestRow]) -> str:
    return table(
        ["Request", "Warehouse", "Tables", "By", "At", "Outcome"],
        [
            [
                link(f"/requests/{r.id}", str(r.id)),
                escape(r.warehouse),
                escape(", ".join(r.tables) if r.tables is not None else "every table"),
                escape(r.requested_by),
                when(r.requested_at),
                escape(r.outcome or "not yet handled"),
            ]
            for r in requests
        ],
        empty="No request yet.",
    )


EXAMPLE_FORM = {
    "name": "",
    "schedule": "0 2 * * *",
    "table_config": '{"namespaces": {"raw": {"tables": {"events": {}}}}}',
    "catalog": '{"type": "rest", "uri": "http://lakekeeper:8181/catalog", "warehouse": "zamboni"}',
}


def warehouse_form(form: dict[str, str] | None = None) -> str:
    """The registration form, refilled with what was sent when it was refused."""
    v = {key: escape(value) for key, value in {**EXAMPLE_FORM, **(form or {})}.items()}
    return (
        '<form method="post" action="/warehouses">'
        f'<p><label>Name <input name="name" required value="{v["name"]}"></label></p>'
        "<p><label>Schedule (cron, UTC, or a window block as JSON) "
        f'<input name="schedule" required value="{v["schedule"]}"></label></p>'
        "<p><label>Table config (the body of a table-config.json)<br>"
        f'<textarea name="table_config" rows="6" required>{v["table_config"]}</textarea>'
        "</label></p><p><label>Catalog properties (never a credential)<br>"
        f'<textarea name="catalog" rows="3">{v["catalog"]}</textarea>'
        "</label></p><button>Register</button></form>"
    )


def run_button(warehouse: str, table_name: str) -> str:
    return (
        f'<form class="inline" method="post" action="/warehouses/{quote(warehouse)}/run">'
        f'<input type="hidden" name="table" value="{escape(table_name)}">'
        "<button>Run now</button></form>"
    )


# -- input --------------------------------------------------------------------------


def read_form(environ: dict[str, Any]) -> dict[str, str]:
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        length = 0
    raw = environ["wsgi.input"].read(length).decode("utf-8") if length else ""
    return {k: v[-1] for k, v in parse_qs(raw, keep_blank_values=True).items()}


def parse_schedule(text: str) -> str | dict[str, Any]:
    """A cron expression, or a fleet file's ``{"window": {...}}`` block as JSON."""
    text = text.strip()
    if text.startswith("{"):
        return parse_json(text, "schedule")
    return text


def parse_json(text: str, what: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"the {what} is not JSON: {exc}") from None
    if not isinstance(value, dict):
        raise ValueError(f"the {what} must be a JSON object")
    return value


def schedule_text(schedule: str | dict[str, Any]) -> str:
    return schedule if isinstance(schedule, str) else json.dumps(schedule)


def tables_of(row: WarehouseRow) -> list[str]:
    """The fully qualified tables a warehouse's table config names, as Zamboni
    reads them -- the same list a sweep of it runs."""
    return sorted(TableConfig.from_dict({"warehouse": row.name, **row.table_config}).tables)


# -- serving -------------------------------------------------------------------------


class ThreadingServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class QuietHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s %s", self.address_string(), format % args)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    database = Database.from_environ()
    database.migrate()
    host = os.environ.get("LAKEHOUSE_WEB_HOST", "0.0.0.0")
    port = int(os.environ.get("LAKEHOUSE_WEB_PORT", "8080"))
    with make_server(
        host, port, App(database), server_class=ThreadingServer, handler_class=QuietHandler
    ) as server:
        logger.info("serving the dashboard on http://%s:%d/", host, port)
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
