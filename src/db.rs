use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use crate::FeedItem;

// ── Public types ──────────────────────────────────────────────────────────────

/// A DWD warning that was posted and is currently tracked as active.
#[derive(Debug, Clone)]
pub struct DwdActiveWarning {
    pub id: String,
    pub headline: String,
    pub region: String,
    /// JSON-encoded map of room_id → event_id for the original warning posts.
    /// None when event IDs were not recorded (e.g. first run after upgrade).
    pub event_ids_json: Option<String>,
    pub original_plain: Option<String>,
    pub original_html: Option<String>,
}

/// A row from the items table, sufficient to format and post a digest.
#[derive(Debug, Clone)]
pub struct DbItem {
    pub guid: String,
    pub source_name: String,
    pub title: String,
    pub link: Option<String>,
    pub link_note: Option<String>,
    pub score: i32,
    pub max_score: i32,
    pub distance_meters: Option<f64>,
    pub location_label: Option<String>,
}

// ── Db handle ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Db(Arc<Mutex<Connection>>);

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).context("Failed to open items DB")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // Wait up to 5 s instead of immediately returning SQLITE_BUSY.
        conn.pragma_update(None, "busy_timeout", 5_000i64)?;
        conn.execute_batch(SCHEMA)
            .context("Failed to initialise DB schema")?;
        ensure_column(&conn, "items", "location_label", "TEXT")?;
        ensure_column(&conn, "items", "link_note", "TEXT")?;
        ensure_column(&conn, "dwd_active_warnings", "event_ids_json", "TEXT")?;
        ensure_column(&conn, "dwd_active_warnings", "original_plain", "TEXT")?;
        ensure_column(&conn, "dwd_active_warnings", "original_html", "TEXT")?;
        Ok(Db(Arc::new(Mutex::new(conn))))
    }

    // ── Poll phase ────────────────────────────────────────────────────────────

    /// Returns true if this guid has never been seen before (any state).
    pub async fn is_new(&self, guid: &str) -> Result<bool> {
        let db = self.0.clone();
        let guid = guid.to_owned();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM items WHERE guid = ?1",
                params![guid],
                |r| r.get(0),
            )?;
            Ok::<bool, anyhow::Error>(n == 0)
        })
        .await?
    }

    /// Persist a scored item that passed all filters. No-op if guid already exists.
    pub async fn insert_queued(&self, item: &FeedItem) -> Result<()> {
        let db = self.0.clone();
        let guid = item.guid.clone();
        let source_name = item.source_name.clone();
        let title = item.title.clone();
        let link = item.link.clone();
        let link_note = item.link_note.clone();
        let score = item.score;
        let max_score = item.max_score;
        let distance_m = item.distance_meters;
        let location = item.location_label.clone();
        let now = chrono::Utc::now().timestamp();

        tokio::task::spawn_blocking(move || {
            db.lock().unwrap().execute(
                "INSERT OR IGNORE INTO items
                 (guid, source_name, title, link, link_note, score, max_score, distance_m, location_label, discovered_at, state)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'queued')",
                params![guid, source_name, title, link, link_note, score, max_score, distance_m, location, now],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    /// Persist a filtered-out item as 'dropped' so it is not reprocessed next poll.
    pub async fn insert_dropped(
        &self,
        guid: &str,
        source_name: &str,
        title: &str,
        link: Option<&str>,
        reason: &str,
    ) -> Result<()> {
        let db = self.0.clone();
        let guid = guid.to_owned();
        let source_name = source_name.to_owned();
        let title = title.to_owned();
        let link = link.map(|s| s.to_owned());
        let reason = reason.to_owned();
        let now = chrono::Utc::now().timestamp();

        tokio::task::spawn_blocking(move || {
            db.lock().unwrap().execute(
                "INSERT OR IGNORE INTO items
                 (guid, source_name, title, link, score, max_score, discovered_at, state, error)
                 VALUES (?1, ?2, ?3, ?4, 0, 0, ?5, 'dropped', ?6)",
                params![guid, source_name, title, link, now, reason],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    // ── Digest phase ──────────────────────────────────────────────────────────

    /// Atomically:
    ///   1. Drop all queued items below min_score.
    ///   2. Move remaining queued items → 'processing'.
    ///   3. Return them sorted by score descending.
    ///
    /// Caller MUST call mark_posted() or requeue_failed() for the returned items.
    pub async fn take_for_digest(&self, min_score: i32) -> Result<Vec<DbItem>> {
        let db = self.0.clone();
        let now = chrono::Utc::now().timestamp();

        tokio::task::spawn_blocking(move || {
            let mut conn = db.lock().unwrap();
            let tx = conn.transaction()?;

            tx.execute(
                "UPDATE items SET state = 'dropped'
                 WHERE state = 'queued' AND score < ?1",
                params![min_score],
            )?;
            tx.execute(
                "UPDATE items SET state = 'processing', last_attempt_at = ?1
                 WHERE state = 'queued'",
                params![now],
            )?;

            let mut stmt = tx.prepare(
                "SELECT guid, source_name, title, link, link_note, score, max_score, distance_m, location_label
                 FROM items WHERE state = 'processing'
                 ORDER BY score DESC",
            )?;
            let items: Vec<DbItem> = stmt.query_map([], |row| {
                Ok(DbItem {
                    guid:            row.get(0)?,
                    source_name:     row.get(1)?,
                    title:           row.get(2)?,
                    link:            row.get(3)?,
                    link_note:       row.get(4)?,
                    score:           row.get(5)?,
                    max_score:       row.get(6)?,
                    distance_meters: row.get(7)?,
                    location_label:  row.get(8)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(stmt);

            tx.commit()?;
            Ok::<Vec<DbItem>, anyhow::Error>(items)
        })
        .await?
    }

    /// Mark items as successfully posted. Call after post_to_rooms confirms delivery.
    pub async fn mark_posted(&self, guids: &[String]) -> Result<()> {
        if guids.is_empty() {
            return Ok(());
        }
        let db = self.0.clone();
        let guids = guids.to_vec();
        let now = chrono::Utc::now().timestamp();

        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            for guid in &guids {
                conn.execute(
                    "UPDATE items SET state = 'posted', posted_at = ?1, error = NULL
                     WHERE guid = ?2 AND state = 'processing'",
                    params![now, guid],
                )?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    /// Post failed: move items back to 'queued' so the next digest retries them.
    pub async fn requeue_failed(&self, guids: &[String], error: &str) -> Result<()> {
        if guids.is_empty() {
            return Ok(());
        }
        let db = self.0.clone();
        let guids = guids.to_vec();
        let error = error.to_owned();

        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            for guid in &guids {
                conn.execute(
                    "UPDATE items SET state = 'queued', error = ?1
                     WHERE guid = ?2 AND state = 'processing'",
                    params![error, guid],
                )?;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    // ── Startup / crash recovery ──────────────────────────────────────────────

    /// Reset any 'processing' rows to 'queued'. These were in-flight when the bot
    /// last crashed. Returns the number of rows recovered.
    pub async fn recover_processing(&self) -> Result<usize> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let n = db.lock().unwrap().execute(
                "UPDATE items SET state = 'queued', error = 'recovered after crash'
                 WHERE state = 'processing'",
                [],
            )?;
            Ok::<usize, anyhow::Error>(n)
        })
        .await?
    }

    // ── Emergency alerts ──────────────────────────────────────────────────────

    pub async fn is_alert_seen(&self, id: &str) -> Result<bool> {
        let db = self.0.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM alerts WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )?;
            Ok::<bool, anyhow::Error>(n > 0)
        })
        .await?
    }

    pub async fn mark_alert_seen(&self, id: &str, source: &str) -> Result<()> {
        let db = self.0.clone();
        let id = id.to_owned();
        let source = source.to_owned();
        let now = chrono::Utc::now().timestamp();
        tokio::task::spawn_blocking(move || {
            db.lock().unwrap().execute(
                "INSERT OR IGNORE INTO alerts (id, source, sent_at) VALUES (?1, ?2, ?3)",
                params![id, source, now],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    // ── DWD active-warning tracking ───────────────────────────────────────────

    pub async fn list_active_dwd_warnings(&self) -> Result<Vec<DwdActiveWarning>> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            let mut stmt = conn.prepare(
                "SELECT id, headline, region, event_ids_json, original_plain, original_html
                 FROM dwd_active_warnings",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(DwdActiveWarning {
                        id: row.get(0)?,
                        headline: row.get(1)?,
                        region: row.get(2)?,
                        event_ids_json: row.get(3)?,
                        original_plain: row.get(4)?,
                        original_html: row.get(5)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok::<Vec<DwdActiveWarning>, anyhow::Error>(rows)
        })
        .await?
    }

    /// Upsert a currently-active DWD warning.
    ///
    /// `event_ids_json` and `original_*` are only written when `Some`; existing
    /// values are preserved via COALESCE so a "heartbeat" upsert without new post
    /// data does not overwrite previously stored event IDs.
    pub async fn upsert_dwd_active_warning(
        &self,
        id: &str,
        headline: &str,
        region: &str,
        event: &str,
        event_ids_json: Option<&str>,
        original_plain: Option<&str>,
        original_html: Option<&str>,
    ) -> Result<()> {
        let db = self.0.clone();
        let id = id.to_owned();
        let headline = headline.to_owned();
        let region = region.to_owned();
        let event = event.to_owned();
        let event_ids_json = event_ids_json.map(str::to_owned);
        let original_plain = original_plain.map(str::to_owned);
        let original_html = original_html.map(str::to_owned);
        let now = chrono::Utc::now().timestamp();
        tokio::task::spawn_blocking(move || {
            db.lock().unwrap().execute(
                "INSERT INTO dwd_active_warnings
                     (id, headline, region, event, seen_at, event_ids_json, original_plain, original_html)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                     headline       = excluded.headline,
                     region         = excluded.region,
                     event          = excluded.event,
                     seen_at        = excluded.seen_at,
                     event_ids_json = COALESCE(excluded.event_ids_json, dwd_active_warnings.event_ids_json),
                     original_plain = COALESCE(excluded.original_plain, dwd_active_warnings.original_plain),
                     original_html  = COALESCE(excluded.original_html,  dwd_active_warnings.original_html)",
                params![id, headline, region, event, now, event_ids_json, original_plain, original_html],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    pub async fn remove_dwd_active_warning(&self, id: &str) -> Result<()> {
        let db = self.0.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            db.lock().unwrap().execute(
                "DELETE FROM dwd_active_warnings WHERE id = ?1",
                params![id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    // ── Maintenance ───────────────────────────────────────────────────────────

    /// Delete posted/dropped items older than keep_days. Returns rows deleted.
    #[allow(dead_code)]
    pub async fn prune_old(&self, keep_days: u32) -> Result<u64> {
        let db = self.0.clone();
        let cutoff = chrono::Utc::now().timestamp() - (keep_days as i64 * 86_400);
        tokio::task::spawn_blocking(move || {
            let n = db.lock().unwrap().execute(
                "DELETE FROM items
                 WHERE state IN ('posted', 'dropped') AND discovered_at < ?1",
                params![cutoff],
            )?;
            Ok::<u64, anyhow::Error>(n as u64)
        })
        .await?
    }
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == column);

    if !exists {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
    }
    Ok(())
}

// ── Schema ────────────────────────────────────────────────────────────────────

const SCHEMA: &str = "
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
                         CHECK(state IN ('queued','processing','posted','dropped')),
        last_attempt_at  INTEGER,
        posted_at        INTEGER,
        error            TEXT
    );
    -- Secondary index makes (source_name, guid) unique as a safety net for
    -- feeds that reuse short numeric IDs across different sources.
    CREATE UNIQUE INDEX IF NOT EXISTS idx_source_guid  ON items(source_name, guid);
    CREATE        INDEX IF NOT EXISTS idx_state        ON items(state);
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
        original_html  TEXT
    );
";
