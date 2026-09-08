-- One row per projection, keyed by `Projection::name`. Two projections that
-- return the same name share one cursor and corrupt each other; nothing here
-- can detect that, so the trait's doc comment carries the requirement.
CREATE TABLE projection_checkpoints (
    projection  TEXT        PRIMARY KEY,
    -- How far this projection has read, in the feed's order. `'0'::xid8`
    -- precedes every real transaction id, so a fresh row starts from the
    -- beginning of the table.
    cursor_xact xid8        NOT NULL DEFAULT '0'::xid8,
    cursor_seq  BIGINT      NOT NULL DEFAULT 0,
    -- Consecutive failed attempts at the same batch. Reset to 0 by a batch
    -- that commits, so this counts a run of failures, not a lifetime total.
    failures    INTEGER     NOT NULL DEFAULT 0,
    -- Set once `failures` reaches the configured maximum. While it is set the
    -- runner does no work for this projection and reports `Halted`.
    halted_at   TIMESTAMPTZ,
    last_error  TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
