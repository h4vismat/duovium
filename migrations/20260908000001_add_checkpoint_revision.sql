-- A generation changes whenever progress or operator action invalidates a
-- pending failure. Matching the cursor alone cannot distinguish resume/rebuild.
ALTER TABLE projection_checkpoints
    ADD COLUMN revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0);
