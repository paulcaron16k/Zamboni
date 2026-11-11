-- The Lakehouse App's database: everything both tiers share.
--
-- The registry is the app's own. The other four tables are
-- docs/maintenance-runtime.md's reference schemas -- §6.1's claims, §9.2's
-- requests and state rows -- as printed there, except that each CREATE says
-- IF NOT EXISTS so either tier can apply this file on every start.
-- tests/test_lakehouse_app.py holds them to the document.

-- The warehouse registry: one row per warehouse, each in the shape of one
-- entry of a fleet file, so the maintenance tier builds its FleetConfig with
-- zamboni.fleet.FleetConfig.from_dict and no translation of its own.
CREATE TABLE IF NOT EXISTS lakehouse_warehouse (
    name         text        PRIMARY KEY,
    -- a cron expression as a JSON string, or {"window": {...}}
    schedule     jsonb       NOT NULL,
    -- the body of a table-config.json
    table_config jsonb       NOT NULL,
    -- PyIceberg catalog properties. Never a credential: the worker entry
    -- adds those from its own environment (lakehouse_app.catalogs).
    catalog      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Quarantined tables (§7.1), as the maintenance tier last reported them:
-- written on `quarantined`, deleted on `released`. The app's own, like the
-- registry: the reference state rows carry no quarantine. Not authoritative
-- -- each replica keeps its own quarantine in memory -- so it is what to show,
-- not what to decide by.
CREATE TABLE IF NOT EXISTS lakehouse_quarantine (
    warehouse        text        NOT NULL,
    table_identifier text        NOT NULL,
    holder           text        NOT NULL,
    failures         integer     NOT NULL,
    until            timestamptz NOT NULL,
    last_failure     text,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (warehouse, table_identifier)
);

-- §6.1: table claims
CREATE SEQUENCE IF NOT EXISTS zamboni_claim_token;

CREATE TABLE IF NOT EXISTS zamboni_claim (
    warehouse        text        NOT NULL,
    table_identifier text        NOT NULL,
    holder           text        NOT NULL,
    token            bigint      NOT NULL,
    reason           text        NOT NULL,
    acquired_at      timestamptz NOT NULL,
    expires_at       timestamptz NOT NULL,
    PRIMARY KEY (warehouse, table_identifier)
);

-- §9.2: requests from the web tier
CREATE TABLE IF NOT EXISTS zamboni_request (
    id           bigserial   PRIMARY KEY,
    warehouse    text        NOT NULL,
    tables       text[],                 -- NULL: every table of the warehouse
    reason       text        NOT NULL CHECK (reason IN ('manual', 'reclaim')),
    requested_by text        NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT now(),
    handled_at   timestamptz,
    outcome      text                    -- offered | unmapped | refused, then why
);

-- §9.2: state rows, written by the maintenance tier and read by the dashboard
CREATE TABLE IF NOT EXISTS zamboni_runtime (
    holder     text        PRIMARY KEY,  -- MaintenanceRuntime.holder
    phase      text        NOT NULL,
    last_tick  timestamptz,
    busy       integer     NOT NULL,
    queued     integer     NOT NULL,
    last_error text,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS zamboni_run (
    id               bigserial   PRIMARY KEY,
    holder           text        NOT NULL,
    warehouse        text        NOT NULL,
    table_identifier text        NOT NULL,
    reason           text        NOT NULL,
    exit_code        integer     NOT NULL,
    died             text,
    dropped          text,
    record           jsonb,                -- the run record; NULL if none came back
    finished_at      timestamptz NOT NULL DEFAULT now()
);
