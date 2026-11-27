//! The bot's SQLite store (`items.db`) on mxbot-common's SQLite module: one connection used off
//! the runtime, numbered migrations ([`MIGRATIONS`]).
//!
//! Migration 1 is the schema the bot had before numbered migrations, written so that it finds
//! an existing store complete. A store from before them (an `items` table, no
//! `schema_migrations`) is first brought to that shape by the column checks the bot used to run
//! at every start ([`adopt_unversioned`]); migration 1 then changes nothing in it.

mod alerts;
mod items;
mod profile;
mod sources;

use std::path::Path;

use anyhow::{Context, Result};
use mxbot_common::schedule::{Ledger, Slot};
use mxbot_common::sqlite::{
    self,
    rusqlite::{params, Connection},
    Migrations,
};

pub use alerts::{DwdActiveWarning, PostKey};
pub use items::DbItem;
pub use sources::{SourceHealth, SourcePollStat, FAILING_AFTER};

/// The store, shared by the bot's tasks.
#[derive(Clone)]
pub struct Db(sqlite::Db);

/// Never edit a migration that has shipped: add the next.
pub(crate) const MIGRATIONS: &Migrations = &[
    (1, SCHEMA_V1),
    (2, LEDGER_V2),
    (3, SOURCE_STATE_V3),
    (4, ROUND_2_V4),
];

/// How long rows that only record "seen" or "done" are kept: past every feed's memory
/// (Google News lists results up to nine months old).
pub(crate) const KEEP_SEEN_DAYS: i64 = 400;
/// How long per-round source statistics are kept.
pub(crate) const KEEP_STATS_DAYS: i64 = 30;

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let mut conn = Connection::open(path).context("Failed to open items DB")?;
        sqlite::configure(&conn, sqlite::Options::default())?;
        adopt_unversioned(&conn)?;
        sqlite::migrate(&mut conn, MIGRATIONS)?;
        Ok(Db(sqlite::Db::from_connection(conn)))
    }

    /// The `alerts` row that recorded a handled slot before the ledger had its own table:
    /// `weather:2026-10-30`, `digest:2026-10-30 08:00`.
    fn slot_row(job: &str, slot: &Slot) -> String {
        format!("{job}:{}", slot.key())
    }

    /// Whether the job `job` has ever recorded a slot.
    pub async fn has_ledger_rows(&self, job: &str) -> Result<bool> {
        let job = job.to_owned();
        self.0
            .run(move |conn| {
                Ok(conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM schedule_ledger WHERE job = ?1)
                         OR EXISTS(SELECT 1 FROM alerts WHERE source = ?1)",
                    params![job],
                    |r| r.get(0),
                )?)
            })
            .await
    }

    /// Delete what has outlived its use: statistics after [`KEEP_STATS_DAYS`]; ledger rows,
    /// warning seen-sets and posted or dropped items after [`KEEP_SEEN_DAYS`]. Returns the
    /// number of rows deleted.
    pub async fn apply_retention(&self, now: i64) -> Result<usize> {
        let seen = now - KEEP_SEEN_DAYS * 86_400;
        let stats = now - KEEP_STATS_DAYS * 86_400;
        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                let mut n = tx.execute(
                    "DELETE FROM source_poll_stats WHERE ts < ?1",
                    params![stats],
                )?;
                n += tx.execute(
                    "DELETE FROM schedule_ledger WHERE done_at < ?1",
                    params![seen],
                )?;
                n += tx.execute("DELETE FROM alerts WHERE sent_at < ?1", params![seen])?;
                n += tx.execute(
                    "DELETE FROM items WHERE state IN ('posted', 'dropped')
                         AND COALESCE(last_seen_at, discovered_at) < ?1",
                    params![seen],
                )?;
                tx.commit()?;
                Ok(n)
            })
            .await
    }
}

/// A store from before numbered migrations: the column checks it ran at every start, so that
/// it has the shape of migration 1. Nothing for a new or a versioned store.
fn adopt_unversioned(conn: &Connection) -> Result<()> {
    let exists = |table: &str| -> Result<bool> {
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            params![table],
            |r| r.get(0),
        )?)
    };
    if exists("schema_migrations")? || !exists("items")? {
        return Ok(());
    }
    ensure_column(conn, "items", "location_label", "TEXT")?;
    ensure_column(conn, "items", "link_note", "TEXT")?;
    ensure_items_state_allows_candidates(conn)?;
    ensure_column(
        conn,
        "items",
        "deliver_always",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    if exists("dwd_active_warnings")? {
        ensure_column(conn, "dwd_active_warnings", "event_ids_json", "TEXT")?;
        ensure_column(conn, "dwd_active_warnings", "original_plain", "TEXT")?;
        ensure_column(conn, "dwd_active_warnings", "original_html", "TEXT")?;
        ensure_column(
            conn,
            "dwd_active_warnings",
            "start_ms",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            conn,
            "dwd_active_warnings",
            "end_ms",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
    }
    if exists("nina_active_warnings")? {
        ensure_column(conn, "nina_active_warnings", "event_ids_json", "TEXT")?;
        ensure_column(conn, "nina_active_warnings", "original_plain", "TEXT")?;
        ensure_column(conn, "nina_active_warnings", "original_html", "TEXT")?;
    }
    Ok(())
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<mxbot_common::sqlite::rusqlite::Result<Vec<_>>>()?
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

fn ensure_items_state_allows_candidates(conn: &Connection) -> Result<()> {
    let sql: String = conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'items'",
        [],
        |row| row.get(0),
    )?;
    if sql.contains("'candidate'") {
        return Ok(());
    }

    conn.execute_batch(
        "
        PRAGMA foreign_keys = OFF;
        BEGIN TRANSACTION;
        ALTER TABLE items RENAME TO items_old;
        CREATE TABLE items (
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
        INSERT INTO items
            (guid, source_name, title, link, link_note, score, max_score, distance_m,
             location_label, discovered_at, state, last_attempt_at, posted_at, error)
        SELECT
            guid, source_name, title, link, link_note, score, max_score, distance_m,
            location_label, discovered_at, state, last_attempt_at, posted_at, error
        FROM items_old;
        DROP TABLE items_old;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_source_guid ON items(source_name, guid);
        CREATE INDEX IF NOT EXISTS idx_state ON items(state);
        COMMIT;
        PRAGMA foreign_keys = ON;
        ",
    )
    .context("Failed to migrate items table for candidate state")?;
    Ok(())
}

/// The schedule's ledger (digest, forecast). It is read from and written to its own table and
/// the `alerts` rows it used before, so that the version before this one, started again over
/// this store, sees the same slots as done.
impl Ledger for Db {
    async fn is_done(&self, job: &str, slot: &Slot) -> Result<bool> {
        let (job, key, row) = (job.to_owned(), slot.key(), Db::slot_row(job, slot));
        self.0
            .run(move |conn| {
                Ok(conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM schedule_ledger WHERE job = ?1 AND slot = ?2)
                         OR EXISTS(SELECT 1 FROM alerts WHERE id = ?3)",
                    params![job, key, row],
                    |r| r.get(0),
                )?)
            })
            .await
    }

    async fn mark_done(&self, job: &str, slot: &Slot) -> Result<()> {
        let (job, key, row) = (job.to_owned(), slot.key(), Db::slot_row(job, slot));
        let now = chrono::Utc::now().timestamp();
        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT OR IGNORE INTO schedule_ledger (job, slot, done_at) VALUES (?1, ?2, ?3)",
                    params![job, key, now],
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO alerts (id, source, sent_at) VALUES (?1, ?2, ?3)",
                    params![row, job, now],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
    }
}

// ── Schema ────────────────────────────────────────────────────────────────────

/// Migration 1: the schema before numbered migrations, including every column the bot used to
/// add at start, and the reference point (`home_profile`).
const SCHEMA_V1: &str = "
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
        error            TEXT,
        deliver_always   INTEGER NOT NULL DEFAULT 0
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
    -- The reference point and what follows from it, resolved once (profile.rs).
    CREATE TABLE IF NOT EXISTS home_profile (
        id           INTEGER PRIMARY KEY CHECK (id = 1),
        address      TEXT    NOT NULL,
        lat          REAL    NOT NULL,
        lon          REAL    NOT NULL,
        city         TEXT,
        borough      TEXT,
        postcode     TEXT,
        country_code TEXT,
        nina_ags     TEXT,
        resolved_at  INTEGER NOT NULL
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
";

/// Migration 2: the schedule's ledger in a table of its own, with the slots recorded so far.
const LEDGER_V2: &str = "
    CREATE TABLE schedule_ledger (
        job      TEXT    NOT NULL,
        slot     TEXT    NOT NULL,
        done_at  INTEGER NOT NULL,
        PRIMARY KEY (job, slot)
    );
    INSERT OR IGNORE INTO schedule_ledger (job, slot, done_at)
        SELECT source, substr(id, length(source) + 2), sent_at
        FROM alerts
        WHERE source IN ('digest', 'weather') AND id LIKE source || ':%';
";

/// Migration 3: how each source is doing, started from the items it delivered so far, so that
/// a source that went quiet is seen as such at once.
const SOURCE_STATE_V3: &str = "
    CREATE TABLE source_state (
        source_name          TEXT    PRIMARY KEY,
        first_polled_at      INTEGER NOT NULL,
        last_success_at      INTEGER,
        last_item_at         INTEGER,
        last_error           TEXT,
        last_error_at        INTEGER,
        consecutive_failures INTEGER NOT NULL DEFAULT 0
    );
    INSERT INTO source_state (source_name, first_polled_at, last_item_at)
        SELECT source_name, MIN(discovered_at), MAX(discovered_at)
        FROM items
        GROUP BY source_name;
";

/// Migration 4: the severity of a posted NINA warning (to see it rise), the newest entry a
/// source lists and the state last reported to the admins, and when a seen item was last
/// listed by its feed (retention prunes on that).
const ROUND_2_V4: &str = "
    ALTER TABLE nina_active_warnings ADD COLUMN severity TEXT;
    ALTER TABLE source_state ADD COLUMN newest_entry_at INTEGER;
    ALTER TABLE source_state ADD COLUMN reported TEXT;
    ALTER TABLE items ADD COLUMN last_seen_at INTEGER;
    UPDATE items SET last_seen_at = discovered_at;
";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::FeedItem;
    use crate::testing::test_db;

    fn item(guid: &str, source: &str, title: &str, score: i32) -> FeedItem {
        FeedItem {
            guid: guid.to_owned(),
            source_name: source.to_owned(),
            title: title.to_owned(),
            link: Some(format!("https://{guid}.test/")),
            score,
            max_score: 5,
            distance_meters: Some(433.0),
            location_label: Some("Perleberger Straße".to_owned()),
            ..Default::default()
        }
    }

    fn guids(items: &[DbItem]) -> Vec<&str> {
        let mut guids: Vec<&str> = items.iter().map(|i| i.guid.as_str()).collect();
        guids.sort_unstable();
        guids
    }

    /// The life of an item today: queued or candidate, promoted by a second source, taken for
    /// a digest, requeued after a failed post, recovered after a crash, posted.
    #[tokio::test]
    async fn items_move_through_queue_candidate_digest_and_recovery() {
        let (db, _dir) = test_db();
        db.insert_queued(&item("a", "Source A", "Brand in der Perleberger Straße", 4))
            .await
            .unwrap();
        db.insert_candidate(
            &item(
                "b",
                "Source B",
                "Feuerwehreinsatz Perleberger Straße nach Brand",
                2,
            ),
            "score 2 below digest threshold 3",
        )
        .await
        .unwrap();
        db.insert_candidate(
            &item("c", "Source C", "Neue Bänke im Park", 1),
            "score 1 below digest threshold 3",
        )
        .await
        .unwrap();
        db.insert_dropped("d", "Source A", "Bundestag debattiert", None, "keyword")
            .await
            .unwrap();

        // Every state is part of the seen-set.
        for guid in ["a", "b", "c", "d"] {
            assert!(!db.is_new(guid).await.unwrap(), "{guid}");
        }
        assert!(db.is_new("e").await.unwrap());

        // A second source corroborates b; c stays a candidate.
        let promoted = crate::digest::promote_candidate_clusters(&db, 3, 2, 24)
            .await
            .unwrap();
        assert_eq!(promoted, 1);

        let taken = db.take_for_digest(3).await.unwrap();
        assert_eq!(guids(&taken), vec!["a", "b"]);
        assert!(taken.iter().all(|i| i.score >= 3));

        // The post of a failed, b got through.
        db.requeue_failed(&["a".to_owned()], "post_to_rooms failed")
            .await
            .unwrap();
        db.mark_posted(&["b".to_owned()]).await.unwrap();
        assert_eq!(db.recover_processing().await.unwrap(), 0);

        // A crash while a is being posted: it comes back as queued.
        assert_eq!(guids(&db.take_for_digest(3).await.unwrap()), vec!["a"]);
        assert_eq!(db.recover_processing().await.unwrap(), 1);
        assert_eq!(guids(&db.take_for_digest(3).await.unwrap()), vec!["a"]);
        db.mark_posted(&["a".to_owned()]).await.unwrap();

        assert_eq!(
            guids(&db.list_recent_posted(3_600).await.unwrap()),
            vec!["a", "b"]
        );
        assert!(db.take_for_digest(3).await.unwrap().is_empty());
    }

    use mxbot_common::sqlite::rusqlite::types::Value;

    /// The schema of the version before numbered migrations (52b8675), as its `Db::open`
    /// created it.
    const SCHEMA_52B8675: &str = include_str!("schema_52b8675.sql");

    fn raw(path: &Path) -> Connection {
        Connection::open(path).unwrap()
    }

    fn tables(conn: &Connection) -> Vec<String> {
        conn.prepare(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')
               AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    }

    /// (name, type, not null, default, primary key) of each column, by name.
    fn columns(conn: &Connection, table: &str) -> Vec<(String, String, bool, Option<String>, i64)> {
        let mut columns: Vec<_> = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |r| {
                Ok((r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        columns.sort();
        columns
    }

    fn rows(conn: &Connection, table: &str, cols: &[String]) -> Vec<Vec<Value>> {
        conn.prepare(&format!(
            "SELECT {} FROM {table} ORDER BY rowid",
            cols.join(", ")
        ))
        .unwrap()
        .query_map([], |r| {
            (0..cols.len()).map(|i| r.get::<_, Value>(i)).collect()
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    }

    fn versions(conn: &Connection) -> Vec<i64> {
        conn.prepare("SELECT version FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn a_store_from_before_numbered_migrations_is_adopted_without_losing_anything() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old.db");
        let conn = raw(&old);
        conn.execute_batch(SCHEMA_52B8675).unwrap();
        conn.execute_batch(
            "INSERT INTO items (guid, source_name, title, link, score, max_score, distance_m,
                                location_label, discovered_at, state, posted_at)
                 VALUES ('g1', 'Local Police', 'Brand', 'https://example.com/1', 4, 5, 433.0,
                         'Perleberger Straße', 1795100000, 'posted', 1795110000),
                        ('g2', 'Local Police', 'Raub', NULL, 0, 0, NULL, NULL, 1795100100,
                         'dropped', NULL);
             INSERT INTO alerts (id, source, sent_at) VALUES
                 ('digest:2026-10-30 08:00', 'digest', 1795000000),
                 ('weather:2026-10-30', 'weather', 1795000001),
                 ('nina:mow.DE-XX-1:2026-10-30', 'nina', 1795000002);
             INSERT INTO dwd_active_warnings (id, headline, region, event, seen_at, event_ids_json,
                                              original_plain, original_html, start_ms, end_ms)
                 VALUES ('dwd:warnings:Yourcity:FROST:2', 'FROST', 'Yourcity', 'FROST', 1,
                         '{}', 'p', 'h', 10, 20);
             INSERT INTO source_poll_stats (ts, source_name, total, queued, candidate, dropped,
                                            retry_later)
                 VALUES (1795100000, 'Local Police', 3, 1, 0, 2, 0);",
        )
        .unwrap();
        let before: Vec<(String, Vec<String>)> = tables(&conn)
            .into_iter()
            .filter(|t| !t.starts_with("idx_"))
            .map(|t| {
                let cols = columns(&conn, &t).into_iter().map(|c| c.0).collect();
                (t, cols)
            })
            .collect();
        let old_rows: Vec<_> = before.iter().map(|(t, c)| rows(&conn, t, c)).collect();
        drop(conn);

        drop(Db::open(&old).unwrap());
        drop(Db::open(&old).unwrap()); // a second start changes nothing
        let fresh = dir.path().join("fresh.db");
        drop(Db::open(&fresh).unwrap());

        let (old, fresh) = (raw(&old), raw(&fresh));
        let all: Vec<i64> = MIGRATIONS.iter().map(|(version, _)| *version).collect();
        assert_eq!(versions(&old), all);
        assert_eq!(tables(&old), tables(&fresh));
        for table in tables(&fresh).iter().filter(|t| !t.starts_with("idx_")) {
            assert_eq!(columns(&old, table), columns(&fresh, table), "{table}");
        }
        for ((table, cols), expected) in before.iter().zip(&old_rows) {
            assert_eq!(&rows(&old, table, cols), expected, "{table}");
        }
        let ledger: Vec<(String, String)> = old
            .prepare("SELECT job, slot FROM schedule_ledger ORDER BY job")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            ledger,
            vec![
                ("digest".to_owned(), "2026-10-30 08:00".to_owned()),
                ("weather".to_owned(), "2026-10-30".to_owned())
            ]
        );
    }

    #[tokio::test]
    async fn the_ledger_is_kept_in_both_tables() {
        let job = crate::weather::weather_job(
            mxbot_common::clock::Zone::DEFAULT,
            mxbot_common::config::TimeOfDay::new(8, 0),
        );
        let slot = job.latest("2026-11-20T08:00:00Z".parse().unwrap()).unwrap();
        let (db, _dir) = test_db();
        assert!(!db.has_ledger_rows("weather").await.unwrap());

        db.mark_done("weather", &slot).await.unwrap();

        assert!(db.is_alert_seen("weather:2026-11-20").await.unwrap());
        db.0.with(|conn| Ok(conn.execute("DELETE FROM alerts", [])?))
            .unwrap();
        assert!(db.is_done("weather", &slot).await.unwrap());
        assert!(db.has_ledger_rows("weather").await.unwrap());
    }

    #[tokio::test]
    async fn an_item_its_feed_still_lists_stays_in_the_seen_set() {
        let (db, _dir) = test_db();
        let now = chrono::Utc::now().timestamp();
        let long_ago = now - 500 * 86_400;
        db.0.with(|conn| {
            conn.execute_batch(&format!(
                "INSERT INTO items (guid, source_name, title, discovered_at, state) VALUES
                     ('listed', 's', 't', {long_ago}, 'dropped'),
                     ('gone', 's', 't', {long_ago}, 'dropped');"
            ))?;
            Ok(())
        })
        .unwrap();

        let seen = db.seen_guids(vec!["listed".to_owned()]).await.unwrap();
        let deleted = db.apply_retention(now).await.unwrap();

        assert!(seen.contains("listed"));
        assert_eq!(deleted, 1);
        assert!(!db.is_new("listed").await.unwrap());
        assert!(db.is_new("gone").await.unwrap());
    }

    #[tokio::test]
    async fn retention_keeps_the_seen_set_for_400_days() {
        let (db, _dir) = test_db();
        let now = 1_800_000_000;
        let days = |d: i64| now - d * 86_400;
        db.0.with(|conn| {
            conn.execute_batch(&format!(
                "INSERT INTO items (guid, source_name, title, discovered_at, state) VALUES
                     ('old', 's', 't', {}, 'dropped'), ('kept', 's', 't', {}, 'dropped'),
                     ('queued', 's', 't', {}, 'queued');
                 INSERT INTO alerts (id, source, sent_at) VALUES
                     ('usgs:old', 'usgs', {}), ('usgs:kept', 'usgs', {});
                 INSERT INTO schedule_ledger (job, slot, done_at) VALUES ('digest', 'old', {});
                 INSERT INTO source_poll_stats (ts, source_name, total, queued, candidate,
                                                dropped, retry_later) VALUES
                     ({}, 's', 1, 0, 0, 1, 0), ({}, 's', 1, 0, 0, 1, 0);",
                days(401),
                days(399),
                days(500),
                days(401),
                days(399),
                days(401),
                days(31),
                days(29)
            ))?;
            Ok(())
        })
        .unwrap();

        let deleted = db.apply_retention(now).await.unwrap();

        assert_eq!(deleted, 4);
        assert!(!db.is_new("kept").await.unwrap());
        assert!(
            !db.is_new("queued").await.unwrap(),
            "only posted and dropped items go"
        );
        assert!(db.is_new("old").await.unwrap());
        assert!(db.is_alert_seen("usgs:kept").await.unwrap());
        assert!(!db.is_alert_seen("usgs:old").await.unwrap());
    }
}
