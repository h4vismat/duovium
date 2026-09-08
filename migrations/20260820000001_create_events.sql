-- One table holds every stream. `stream_type` namespaces the keys, so two
-- aggregates that happen to share an id value stay separate.
CREATE TABLE events (
    -- Insert order across streams, not commit order: the value is assigned at
    -- INSERT but the row becomes visible at COMMIT, and the two orders can
    -- differ. So `global_seq` alone cannot order a catch-up read; it breaks
    -- ties under `xact_id`, which can.
    global_seq     BIGSERIAL   PRIMARY KEY,
    -- The transaction that wrote the row. A reader that only takes rows below
    -- `pg_snapshot_xmin(pg_current_snapshot())` can never skip one: once xmin
    -- passes X, every transaction at or below X has finished, so no row can
    -- still appear there. 64-bit, so it never wraps around.
    xact_id        xid8        NOT NULL DEFAULT pg_current_xact_id(),
    event_id       UUID        NOT NULL,
    stream_type    TEXT        NOT NULL,
    stream_id      TEXT        NOT NULL,
    version        BIGINT      NOT NULL CHECK (version > 0),
    -- What `Event::name` and `Event::version` reported at write time.
    -- Deserialization does not read them: the payload identifies its own
    -- variant. They serve observability and later upcasting.
    event_name     TEXT        NOT NULL,
    event_version  INTEGER     NOT NULL,
    payload        JSONB       NOT NULL,
    -- Columns, not JSON fields, so one correlation is traceable across every
    -- stream with a single indexed query.
    correlation_id UUID,
    causation_id   UUID,
    metadata       JSONB       NOT NULL DEFAULT '{}',
    -- Supplied by the writer, never defaulted: one append shares one timestamp
    -- across the events it records, because they are one decision.
    recorded_at    TIMESTAMPTZ NOT NULL,
    -- Named, because the store branches on the name Postgres reports in a
    -- 23505 to tell a version collision from any other unique violation. Its
    -- index also answers the read query, which filters on the first two
    -- columns and orders by the third, so no separate read index exists.
    CONSTRAINT events_stream_version_key UNIQUE (stream_type, stream_id, version),
    CONSTRAINT events_event_id_key       UNIQUE (event_id)
);

CREATE INDEX events_correlation_idx
    ON events (correlation_id) WHERE correlation_id IS NOT NULL;

-- Answers the catch-up read. `stream_type` leads because the read filters it
-- by equality, and only an equality column can lead: put a range column first
-- and the scan loses all `stream_type` selectivity, so a projection over one
-- aggregate walks forward through every other aggregate's rows to fill each
-- LIMIT. The two range columns follow in the order the read sorts by.
CREATE INDEX events_xact_idx
    ON events (stream_type, xact_id, global_seq);
