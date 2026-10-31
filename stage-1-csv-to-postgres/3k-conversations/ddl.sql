-- Stage 1 target table for the 3K Conversations dataset.
--
-- Grain: one row per conversation pair in Conversation.csv.
-- source_row_id is the source file's row index (the blank first
-- header in the raw CSV, normalised to this name). It is a row
-- index, not a domain identifier; kept as the primary key per the
-- project decision.

CREATE TABLE IF NOT EXISTS conversations (
    source_row_id INTEGER PRIMARY KEY,
    question      TEXT NOT NULL,
    answer        TEXT NOT NULL
);
