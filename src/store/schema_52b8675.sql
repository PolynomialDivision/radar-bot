
    CREATE TABLE IF NOT EXISTS items (
        guid             TEXT    PRIMARY KEY,
        source_name      TEXT    NOT NULL,
        title            TEXT    NOT NULL,
        link             TEXT,
        link_note        TEXT,
        score            INTEGER NOT NULL DEFAULT 0,
        max_score        INTEGER NOT NULL DEFAULT 0,
        distance_m       REAL,
        location_label   TEXT,
        discovered_at    INTEGER NOT NULL,
        state            TEXT    NOT NULL DEFAULT 'queued'
                         CHECK(state IN ('queued','candidate','processing','posted','dropped')),
        last_attempt_at  INTEGER,
        posted_at        INTEGER,
        error            TEXT
    );
    -- Secondary index makes (source_name, guid) unique as a safety net for
    -- feeds that reuse short numeric IDs across different sources.
    CREATE UNIQUE INDEX IF NOT EXISTS idx_source_guid  ON items(source_name, guid);
    CREATE        INDEX IF NOT EXISTS idx_state        ON items(state);
    CREATE TABLE IF NOT EXISTS retry_attempts (
        guid     TEXT PRIMARY KEY,
        attempts INTEGER NOT NULL,
        last_at  INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS source_poll_stats (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          INTEGER NOT NULL,
        source_name TEXT    NOT NULL,
        total       INTEGER NOT NULL,
        queued      INTEGER NOT NULL,
        candidate   INTEGER NOT NULL,
        dropped     INTEGER NOT NULL,
        retry_later INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_source_poll_stats_ts ON source_poll_stats(ts);
    CREATE TABLE IF NOT EXISTS alerts (
        id       TEXT    PRIMARY KEY,
        source   TEXT    NOT NULL,
        sent_at  INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS dwd_active_warnings (
        id             TEXT    PRIMARY KEY,
        headline       TEXT    NOT NULL,
        region         TEXT    NOT NULL,
        event          TEXT    NOT NULL,
        seen_at        INTEGER NOT NULL,
        event_ids_json TEXT,
        original_plain TEXT,
        original_html  TEXT,
        start_ms       INTEGER NOT NULL DEFAULT 0,
        end_ms         INTEGER NOT NULL DEFAULT 0
    );
    -- An alert occurrence's transaction key from its first attempt, until it
    -- is delivered everywhere or ends (Db::post_key).
    CREATE TABLE IF NOT EXISTS alert_post_keys (
        id         TEXT    PRIMARY KEY,
        source     TEXT    NOT NULL,
        key        TEXT    NOT NULL,
        start_ms   INTEGER NOT NULL,
        end_ms     INTEGER NOT NULL,
        created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS nina_active_warnings (
        id             TEXT    PRIMARY KEY,
        headline       TEXT    NOT NULL,
        region         TEXT    NOT NULL,
        seen_at        INTEGER NOT NULL,
        event_ids_json TEXT    NOT NULL,
        original_plain TEXT    NOT NULL,
        original_html  TEXT    NOT NULL
    );

