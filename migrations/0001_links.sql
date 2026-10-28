-- fillfactor 90 leaves free space on each page, so the frequent click
-- updates can be HOT updates: the new row version goes on the same page and
-- the primary key index is not touched.
CREATE TABLE links (
    code       text PRIMARY KEY,
    url        text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    clicks     bigint NOT NULL DEFAULT 0
) WITH (fillfactor = 90);

CREATE INDEX links_expires_at_idx ON links (expires_at) WHERE expires_at IS NOT NULL;
