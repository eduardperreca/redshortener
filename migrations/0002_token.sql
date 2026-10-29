-- SHA-256 of the token that can change or delete the link. Links created
-- before tokens existed have none, and cannot be changed.
ALTER TABLE links ADD COLUMN token_hash bytea;
