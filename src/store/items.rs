//! Feed items: the seen-set, the queue, candidates, the digest's take and its bookkeeping.

use anyhow::Result;
use mxbot_common::sqlite::rusqlite::{self, params};

use super::Db;
use crate::feed::FeedItem;

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

/// A digest candidate or queued item used for pre-digest clustering.
#[derive(Debug, Clone)]
pub struct PendingItem {
    pub item: DbItem,
    pub state: String,
}

impl Db {
    /// The guids of `guids` that were seen before (any state): one query per feed.
    pub async fn seen_guids(
        &self,
        guids: Vec<String>,
    ) -> Result<std::collections::HashSet<String>> {
        self.0
            .run(move |conn| {
                let mut seen = std::collections::HashSet::new();
                for chunk in guids.chunks(500) {
                    let marks = vec!["?"; chunk.len()].join(", ");
                    let mut stmt =
                        conn.prepare(&format!("SELECT guid FROM items WHERE guid IN ({marks})"))?;
                    let rows = stmt.query_map(rusqlite::params_from_iter(chunk), |r| r.get(0))?;
                    for guid in rows {
                        seen.insert(guid?);
                    }
                }
                Ok(seen)
            })
            .await
    }

    /// Returns true if this guid has never been seen before (any state).
    #[cfg(test)]
    pub async fn is_new(&self, guid: &str) -> Result<bool> {
        let guid = guid.to_owned();
        self.0
            .run(move |conn| {
                let n: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM items WHERE guid = ?1",
                    params![guid],
                    |r| r.get(0),
                )?;
                Ok(n == 0)
            })
            .await
    }

    /// Persist a scored item that passed all filters. No-op if guid already exists.
    pub async fn insert_queued(&self, item: &FeedItem) -> Result<()> {
        self.insert_item_with_state(item, "queued", None).await
    }

    /// Persist a weak-but-local item so duplicate clustering can promote it later.
    pub async fn insert_candidate(&self, item: &FeedItem, reason: &str) -> Result<()> {
        self.insert_item_with_state(item, "candidate", Some(reason))
            .await
    }

    async fn insert_item_with_state(
        &self,
        item: &FeedItem,
        state: &'static str,
        reason: Option<&str>,
    ) -> Result<()> {
        let guid = item.guid.clone();
        let source_name = item.source_name.clone();
        let title = item.title.clone();
        let link = item.link.clone();
        let link_note = item.link_note.clone();
        let score = item.score;
        let max_score = item.max_score;
        let distance_m = item.distance_meters;
        let location = item.location_label.clone();
        let deliver_always = item.deliver_always;
        let reason = reason.map(str::to_owned);
        let now = chrono::Utc::now().timestamp();

        self.0.run(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO items
                 (guid, source_name, title, link, link_note, score, max_score, distance_m, location_label, discovered_at, state, error, deliver_always)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![guid, source_name, title, link, link_note, score, max_score, distance_m, location, now, state, reason, deliver_always],
            )?;
            conn.execute("DELETE FROM retry_attempts WHERE guid = ?1", params![guid])?;
            Ok(())
        })
        .await
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
        let guid = guid.to_owned();
        let source_name = source_name.to_owned();
        let title = title.to_owned();
        let link = link.map(|s| s.to_owned());
        let reason = reason.to_owned();
        let now = chrono::Utc::now().timestamp();

        self.0
            .run(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO items
                 (guid, source_name, title, link, score, max_score, discovered_at, state, error)
                 VALUES (?1, ?2, ?3, ?4, 0, 0, ?5, 'dropped', ?6)",
                    params![guid, source_name, title, link, now, reason],
                )?;
                conn.execute("DELETE FROM retry_attempts WHERE guid = ?1", params![guid])?;
                Ok(())
            })
            .await
    }

    /// Return queued and candidate rows recent enough to participate in pre-digest clustering.
    pub async fn list_pending_for_clustering(
        &self,
        candidate_retention_secs: i64,
    ) -> Result<Vec<PendingItem>> {
        let cutoff = chrono::Utc::now().timestamp() - candidate_retention_secs;

        self.0.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT guid, source_name, title, link, link_note, score, max_score, distance_m, location_label, state
                 FROM items
                 WHERE state = 'queued'
                    OR (state = 'candidate' AND discovered_at >= ?1)
                 ORDER BY score DESC",
            )?;
            let rows = stmt
                .query_map(params![cutoff], |row| {
                    Ok(PendingItem {
                        item: DbItem {
                            guid:            row.get(0)?,
                            source_name:     row.get(1)?,
                            title:           row.get(2)?,
                            link:            row.get(3)?,
                            link_note:       row.get(4)?,
                            score:           row.get(5)?,
                            max_score:       row.get(6)?,
                            distance_meters: row.get(7)?,
                            location_label:  row.get(8)?,
                        },
                        state: row.get(9)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn promote_candidates(
        &self,
        guids: &[String],
        min_score: i32,
        reason: &str,
    ) -> Result<usize> {
        if guids.is_empty() {
            return Ok(0);
        }
        let guids = guids.to_vec();
        let reason = reason.to_owned();

        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                let mut n = 0;
                for guid in &guids {
                    n += tx.execute(
                        "UPDATE items
                     SET state = 'queued',
                         score = MAX(score, ?1),
                         error = ?2
                     WHERE guid = ?3 AND state = 'candidate'",
                        params![min_score, reason, guid],
                    )?;
                }
                tx.commit()?;
                Ok(n)
            })
            .await
    }

    pub async fn prune_old_candidates(&self, candidate_retention_secs: i64) -> Result<usize> {
        let cutoff = chrono::Utc::now().timestamp() - candidate_retention_secs;
        self.0
            .run(move |conn| {
                let n = conn.execute(
                    "UPDATE items
                 SET state = 'dropped', error = 'candidate expired before corroboration'
                 WHERE state = 'candidate' AND discovered_at < ?1",
                    params![cutoff],
                )?;
                Ok(n)
            })
            .await
    }

    /// Atomically:
    ///   1. Move queued items → 'processing'.
    ///   2. Return them sorted by score descending.
    ///
    /// Caller MUST call mark_posted() or requeue_failed() for the returned items.
    pub async fn take_for_digest(&self, min_score: i32) -> Result<Vec<DbItem>> {
        let now = chrono::Utc::now().timestamp();

        self.0.run(move |conn| {
            let tx = conn.transaction()?;

            tx.execute(
                "UPDATE items SET state = 'dropped', error = 'queued item below digest threshold'
                 WHERE state = 'queued' AND score < ?1 AND deliver_always = 0",
                params![min_score],
            )?;
            tx.execute(
                "UPDATE items SET state = 'processing', last_attempt_at = ?1
                 WHERE state = 'queued' AND (score >= ?2 OR deliver_always = 1)",
                params![now, min_score],
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
            Ok(items)
        })
        .await
    }

    /// Mark items as successfully posted. Call after post_to_rooms confirms delivery.
    pub async fn mark_posted(&self, guids: &[String]) -> Result<()> {
        if guids.is_empty() {
            return Ok(());
        }
        let guids = guids.to_vec();
        let now = chrono::Utc::now().timestamp();

        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                for guid in &guids {
                    tx.execute(
                        "UPDATE items SET state = 'posted', posted_at = ?1, error = NULL
                     WHERE guid = ?2 AND state = 'processing'",
                        params![now, guid],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await
    }

    /// Post failed: move items back to 'queued' so the next digest retries them.
    pub async fn requeue_failed(&self, guids: &[String], error: &str) -> Result<()> {
        if guids.is_empty() {
            return Ok(());
        }
        let guids = guids.to_vec();
        let error = error.to_owned();

        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                for guid in &guids {
                    tx.execute(
                        "UPDATE items SET state = 'queued', error = ?1
                     WHERE guid = ?2 AND state = 'processing'",
                        params![error, guid],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await
    }

    /// Items posted within the last `window_secs`, to recognise re-reports.
    pub async fn list_recent_posted(&self, window_secs: i64) -> Result<Vec<DbItem>> {
        let cutoff = chrono::Utc::now().timestamp() - window_secs;

        self.0.run(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT guid, source_name, title, link, link_note, score, max_score, distance_m, location_label
                 FROM items WHERE state = 'posted' AND posted_at >= ?1",
            )?;
            let items = stmt
                .query_map(params![cutoff], |row| {
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
            Ok(items)
        })
        .await
    }

    /// Drop items taken for a digest without posting them.
    pub async fn drop_processing(&self, guids: &[String], reason: &str) -> Result<()> {
        if guids.is_empty() {
            return Ok(());
        }
        let guids = guids.to_vec();
        let reason = reason.to_owned();

        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                for guid in &guids {
                    tx.execute(
                        "UPDATE items SET state = 'dropped', error = ?1
                     WHERE guid = ?2 AND state = 'processing'",
                        params![reason, guid],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await
    }

    /// How many polls ended in "retry later" for this guid.
    pub async fn retry_attempts(&self, guid: &str) -> Result<u32> {
        let guid = guid.to_owned();
        self.0
            .run(move |conn| {
                let n: Option<u32> = conn
                    .query_row(
                        "SELECT attempts FROM retry_attempts WHERE guid = ?1",
                        params![guid],
                        |r| r.get(0),
                    )
                    .ok();
                Ok(n.unwrap_or(0))
            })
            .await
    }

    pub async fn record_retry(&self, guid: &str) -> Result<()> {
        let guid = guid.to_owned();
        let now = chrono::Utc::now().timestamp();
        self.0
            .run(move |conn| {
                conn.execute(
                    "INSERT INTO retry_attempts (guid, attempts, last_at) VALUES (?1, 1, ?2)
                 ON CONFLICT(guid) DO UPDATE SET attempts = attempts + 1, last_at = ?2",
                    params![guid, now],
                )?;
                // Items that left their feed while still retrying.
                conn.execute(
                    "DELETE FROM retry_attempts WHERE last_at < ?1",
                    params![now - 7 * 86_400],
                )?;
                Ok(())
            })
            .await
    }

    /// Reset any 'processing' rows to 'queued'. These were in-flight when the bot
    /// last crashed. Returns the number of rows recovered.
    pub async fn recover_processing(&self) -> Result<usize> {
        self.0
            .run(move |conn| {
                let n = conn.execute(
                    "UPDATE items SET state = 'queued', error = 'recovered after crash'
                 WHERE state = 'processing'",
                    [],
                )?;
                Ok(n)
            })
            .await
    }
}
