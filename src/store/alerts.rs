//! Official warnings: the seen-set, post keys, the active NINA and DWD warnings.

use std::collections::HashSet;

use anyhow::Result;
use mxbot_common::sqlite::rusqlite::{self, params};

use super::Db;

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
    /// DWD start/end timestamps (ms) as last seen. Used to detect time updates.
    pub start_ms: i64,
    pub end_ms: i64,
    /// When the message was posted (or last replaced), seconds.
    pub seen_at: i64,
}

/// The transaction key of an alert occurrence's post, fixed at its first
/// attempt, with the warning's time window as it was then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostKey {
    pub key: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Clone)]
pub struct NinaActiveWarning {
    pub headline: String,
    pub event_ids_json: String,
    pub original_plain: String,
    pub original_html: String,
    /// The severity of the posted version; unknown for rows from before migration 4.
    pub severity: Option<String>,
    /// When it was posted or last updated, seconds.
    pub seen_at: i64,
}

impl Db {
    pub async fn is_alert_seen(&self, id: &str) -> Result<bool> {
        let id = id.to_owned();
        self.0
            .run(move |conn| {
                let n: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM alerts WHERE id = ?1",
                    params![id],
                    |r| r.get(0),
                )?;
                Ok(n > 0)
            })
            .await
    }

    pub async fn mark_alert_seen(&self, id: &str, source: &str) -> Result<()> {
        let id = id.to_owned();
        let source = source.to_owned();
        let now = chrono::Utc::now().timestamp();
        self.0
            .run(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO alerts (id, source, sent_at) VALUES (?1, ?2, ?3)",
                    params![id, source, now],
                )?;
                Ok(())
            })
            .await
    }

    /// The transaction key for posting the occurrence `id` (a DWD stable key,
    /// a NINA warning without its version): the one stored at its first
    /// attempt, else `key` (stored now with `start_ms`/`end_ms`). A retry
    /// after some rooms failed thus sends with the same transaction IDs even
    /// when the warning changed in between, and rooms that have it get no
    /// second copy. The row goes when the occurrence is stored as delivered
    /// or ended (`mark_*_posted`, `promote_*`, `mark_*_allclear_sent`,
    /// [`Db::forget_post_keys_except`]), so a warning that comes back later
    /// gets a key of its own.
    pub async fn post_key(
        &self,
        id: &str,
        source: &str,
        key: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<PostKey> {
        let id = id.to_owned();
        let source = source.to_owned();
        let key = key.to_owned();
        let now = chrono::Utc::now().timestamp();
        self.0
            .run(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO alert_post_keys
                     (id, source, key, start_ms, end_ms, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![id, source, key, start_ms, end_ms, now],
                )?;
                let stored = conn.query_row(
                    "SELECT key, start_ms, end_ms FROM alert_post_keys WHERE id = ?1",
                    params![id],
                    |r| {
                        Ok(PostKey {
                            key: r.get(0)?,
                            start_ms: r.get(1)?,
                            end_ms: r.get(2)?,
                        })
                    },
                )?;
                Ok(stored)
            })
            .await
    }

    /// Drop the post keys of `source` whose occurrence is no longer in its
    /// feed (`current`): it ended without being delivered everywhere, and if
    /// it comes back it is a new occurrence. Returns how many went.
    pub async fn forget_post_keys_except(
        &self,
        source: &str,
        current: &HashSet<String>,
    ) -> Result<usize> {
        let source = source.to_owned();
        let current = current.clone();
        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                let ids = tx
                    .prepare("SELECT id FROM alert_post_keys WHERE source = ?1")?
                    .query_map(params![source], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let mut gone = 0;
                for id in ids.iter().filter(|id| !current.contains(*id)) {
                    gone += tx.execute("DELETE FROM alert_post_keys WHERE id = ?1", params![id])?;
                }
                tx.commit()?;
                Ok(gone)
            })
            .await
    }

    pub async fn get_active_nina_warning(&self, id: &str) -> Result<Option<NinaActiveWarning>> {
        let id = id.to_owned();
        self.0
            .run(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT headline, event_ids_json, original_plain, original_html, severity,
                        seen_at
                 FROM nina_active_warnings
                 WHERE id = ?1",
                )?;
                let mut rows = stmt.query(params![id])?;
                let Some(row) = rows.next()? else {
                    return Ok(None);
                };
                Ok(Some(NinaActiveWarning {
                    headline: row.get(0)?,
                    event_ids_json: row.get(1)?,
                    original_plain: row.get(2)?,
                    original_html: row.get(3)?,
                    severity: row.get(4)?,
                    seen_at: row.get(5)?,
                }))
            })
            .await
    }

    /// The severity of the version of NINA warning `id` that the rooms show.
    pub async fn set_nina_severity(&self, id: &str, severity: Option<&str>) -> Result<()> {
        let id = id.to_owned();
        let severity = severity.map(str::to_owned);
        self.0
            .run(move |conn| {
                conn.execute(
                    "UPDATE nina_active_warnings SET severity = ?2 WHERE id = ?1",
                    params![id, severity],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn mark_nina_alert_posted(
        &self,
        id: &str,
        headline: &str,
        region: &str,
        event_ids_json: &str,
        original_plain: &str,
        original_html: &str,
    ) -> Result<()> {
        let id = id.to_owned();
        let headline = headline.to_owned();
        let region = region.to_owned();
        let event_ids_json = event_ids_json.to_owned();
        let original_plain = original_plain.to_owned();
        let original_html = original_html.to_owned();
        let now = chrono::Utc::now().timestamp();
        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO nina_active_warnings
                     (id, headline, region, seen_at, event_ids_json, original_plain, original_html)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(id) DO UPDATE SET
                     headline       = excluded.headline,
                     region         = excluded.region,
                     seen_at        = excluded.seen_at,
                     event_ids_json = excluded.event_ids_json,
                     original_plain = excluded.original_plain,
                     original_html  = excluded.original_html",
                    params![
                        id,
                        headline,
                        region,
                        now,
                        event_ids_json,
                        original_plain,
                        original_html
                    ],
                )?;
                tx.execute("DELETE FROM alert_post_keys WHERE id = ?1", params![id])?;
                tx.commit()?;
                Ok(())
            })
            .await
    }

    pub async fn mark_nina_allclear_sent(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.0
            .run(move |conn| {
                conn.execute(
                    "DELETE FROM nina_active_warnings WHERE id = ?1",
                    params![id],
                )?;
                conn.execute("DELETE FROM alert_post_keys WHERE id = ?1", params![id])?;
                Ok(())
            })
            .await
    }

    pub async fn list_active_dwd_warnings(&self) -> Result<Vec<DwdActiveWarning>> {
        self.0
            .run(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, headline, region, event_ids_json, original_plain, original_html,
                        start_ms, end_ms, seen_at
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
                            start_ms: row.get(6)?,
                            end_ms: row.get(7)?,
                            seen_at: row.get(8)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }

    /// Atomically insert a fresh DWD warning into `dwd_active_warnings`.
    ///
    /// The stable key (event+level+region, no timestamps) is used as the primary key,
    /// so this is also the correct upsert when the same warning reappears after an all-clear.
    /// No `alerts` row is written; presence in `dwd_active_warnings` IS the "seen" check.
    #[allow(clippy::too_many_arguments)]
    pub async fn mark_dwd_alert_posted(
        &self,
        id: &str,
        headline: &str,
        region: &str,
        event: &str,
        event_ids_json: &str,
        start_ms: i64,
        end_ms: i64,
        original_plain: &str,
        original_html: &str,
    ) -> Result<()> {
        let id = id.to_owned();
        let headline = headline.to_owned();
        let region = region.to_owned();
        let event = event.to_owned();
        let event_ids_json = event_ids_json.to_owned();
        let original_plain = original_plain.to_owned();
        let original_html = original_html.to_owned();
        let now = chrono::Utc::now().timestamp();
        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO dwd_active_warnings
                     (id, headline, region, event, seen_at,
                      event_ids_json, start_ms, end_ms, original_plain, original_html)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(id) DO UPDATE SET
                     headline       = excluded.headline,
                     region         = excluded.region,
                     event          = excluded.event,
                     seen_at        = excluded.seen_at,
                     event_ids_json = excluded.event_ids_json,
                     start_ms       = excluded.start_ms,
                     end_ms         = excluded.end_ms,
                     original_plain = excluded.original_plain,
                     original_html  = excluded.original_html",
                    params![
                        id,
                        headline,
                        region,
                        event,
                        now,
                        event_ids_json,
                        start_ms,
                        end_ms,
                        original_plain,
                        original_html
                    ],
                )?;
                tx.execute("DELETE FROM alert_post_keys WHERE id = ?1", params![id])?;
                tx.commit()?;
                Ok(())
            })
            .await
    }

    /// Update the stored time window and message content after a time-change edit.
    ///
    /// `plain` / `html` use COALESCE so passing `None` preserves the stored content
    /// (used when we acknowledge a time change but have no event IDs to edit with).
    pub async fn update_dwd_active_warning(
        &self,
        id: &str,
        start_ms: i64,
        end_ms: i64,
        plain: Option<&str>,
        html: Option<&str>,
    ) -> Result<()> {
        let id = id.to_owned();
        let plain = plain.map(str::to_owned);
        let html = html.map(str::to_owned);
        self.0
            .run(move |conn| {
                conn.execute(
                    "UPDATE dwd_active_warnings
                 SET start_ms       = ?2,
                     end_ms         = ?3,
                     original_plain = COALESCE(?4, original_plain),
                     original_html  = COALESCE(?5, original_html)
                 WHERE id = ?1",
                    params![id, start_ms, end_ms, plain, html],
                )?;
                Ok(())
            })
            .await
    }

    /// Atomically replace an active-warning row with a new one for a different level,
    /// transferring the Matrix event IDs so the same message can continue to be edited.
    /// Used when a warning escalates or de-escalates: the old-level row is deleted and
    /// a new-level row is inserted with the same event_ids_json.
    #[allow(clippy::too_many_arguments)]
    pub async fn promote_dwd_active_warning(
        &self,
        old_id: &str,
        new_id: &str,
        headline: &str,
        region: &str,
        event: &str,
        event_ids_json: &str,
        start_ms: i64,
        end_ms: i64,
        original_plain: &str,
        original_html: &str,
    ) -> Result<()> {
        let old_id = old_id.to_owned();
        let new_id = new_id.to_owned();
        let headline = headline.to_owned();
        let region = region.to_owned();
        let event = event.to_owned();
        let event_ids_json = event_ids_json.to_owned();
        let original_plain = original_plain.to_owned();
        let original_html = original_html.to_owned();
        let now = chrono::Utc::now().timestamp();
        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "DELETE FROM dwd_active_warnings WHERE id = ?1",
                    params![old_id],
                )?;
                tx.execute(
                    "INSERT INTO dwd_active_warnings
                     (id, headline, region, event, seen_at,
                      event_ids_json, start_ms, end_ms, original_plain, original_html)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        new_id,
                        headline,
                        region,
                        event,
                        now,
                        event_ids_json,
                        start_ms,
                        end_ms,
                        original_plain,
                        original_html
                    ],
                )?;
                tx.execute("DELETE FROM alert_post_keys WHERE id = ?1", params![new_id])?;
                tx.commit()?;
                Ok(())
            })
            .await
    }

    /// Remove the active-warning row once an all-clear has been delivered.
    /// The next occurrence of the same warning type is then treated as fresh.
    pub async fn mark_dwd_allclear_sent(&self, warning_id: &str) -> Result<()> {
        let warning_id = warning_id.to_owned();
        self.0
            .run(move |conn| {
                conn.execute(
                    "DELETE FROM dwd_active_warnings WHERE id = ?1",
                    params![warning_id],
                )?;
                conn.execute(
                    "DELETE FROM alert_post_keys WHERE id = ?1",
                    params![warning_id],
                )?;
                Ok(())
            })
            .await
    }
}
