-- OSB broker schema for Postgres.


CREATE TABLE IF NOT EXISTS service_instances (
    instance_id            TEXT PRIMARY KEY,
    service_id             TEXT        NOT NULL,
    plan_id                TEXT        NOT NULL,
    parameters             JSONB,
    context                JSONB,
    maintenance_info       JSONB,
    dashboard_url          TEXT,
    operation_id           TEXT,
    operation_kind         TEXT,
    operation_state        TEXT,
    operation_complete_at  TIMESTAMPTZ,
    deleted                BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS service_bindings (
    instance_id            TEXT        NOT NULL,
    binding_id             TEXT        NOT NULL,
    service_id             TEXT        NOT NULL,
    plan_id                TEXT        NOT NULL,
    parameters             JSONB,
    context                JSONB,
    credentials            JSONB,
    syslog_drain_url       TEXT,
    route_service_url      TEXT,
    operation_id           TEXT,
    operation_kind         TEXT,
    operation_state        TEXT,
    operation_complete_at  TIMESTAMPTZ,
    deleted                BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instance_id, binding_id),
    FOREIGN KEY (instance_id) REFERENCES service_instances(instance_id) ON DELETE CASCADE
);
