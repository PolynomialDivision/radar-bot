//! Per-source statistics of each poll round (`!source-stats`).

use anyhow::Result;
use mxbot_common::sqlite::rusqlite::{self, params};

use super::Db;

#[derive(Debug, Clone)]
pub struct SourcePollStat {
    pub source_name: String,
    pub total: usize,
    pub queued: usize,
    pub candidate: usize,
    pub dropped: usize,
    pub retry_later: usize,
}

#[derive(Debug, Clone)]
pub struct SourceStatSummary {
    pub source_name: String,
    pub total: i64,
    pub queued: i64,
    pub candidate: i64,
    pub dropped: i64,
    pub retry_later: i64,
}

impl Db {
    pub async fn record_source_stats(&self, stats: Vec<SourcePollStat>) -> Result<()> {
        if stats.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp();
        let cutoff = now - 30 * 86_400;

        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                for stat in stats {
                    tx.execute(
                        "INSERT INTO source_poll_stats
                     (ts, source_name, total, queued, candidate, dropped, retry_later)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            now,
                            stat.source_name,
                            stat.total as i64,
                            stat.queued as i64,
                            stat.candidate as i64,
                            stat.dropped as i64,
                            stat.retry_later as i64
                        ],
                    )?;
                }
                tx.execute(
                    "DELETE FROM source_poll_stats WHERE ts < ?1",
                    params![cutoff],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
    }

    pub async fn source_stats_summary(&self, hours: u32) -> Result<Vec<SourceStatSummary>> {
        let hours = hours.clamp(1, 24 * 30);
        let cutoff = chrono::Utc::now().timestamp() - hours as i64 * 3600;

        self.0
            .run(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT source_name,
                        COALESCE(SUM(total), 0),
                        COALESCE(SUM(queued), 0),
                        COALESCE(SUM(candidate), 0),
                        COALESCE(SUM(dropped), 0),
                        COALESCE(SUM(retry_later), 0)
                 FROM source_poll_stats
                 WHERE ts >= ?1
                 GROUP BY source_name
                 ORDER BY SUM(queued) DESC, SUM(candidate) DESC, SUM(total) DESC
                 LIMIT 25",
                )?;
                let rows = stmt
                    .query_map(params![cutoff], |row| {
                        Ok(SourceStatSummary {
                            source_name: row.get(0)?,
                            total: row.get(1)?,
                            queued: row.get(2)?,
                            candidate: row.get(3)?,
                            dropped: row.get(4)?,
                            retry_later: row.get(5)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }

    /// Record one poll of `source_name`: `Ok(new items)` or `Err(why)`.
    pub async fn record_source_round(
        &self,
        source_name: &str,
        outcome: std::result::Result<usize, String>,
        now: i64,
    ) -> Result<()> {
        let name = source_name.to_owned();
        self.0
            .run(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT INTO source_state (source_name, first_polled_at) VALUES (?1, ?2)
                     ON CONFLICT(source_name) DO NOTHING",
                    params![name, now],
                )?;
                match outcome {
                    Ok(new_items) => tx.execute(
                        "UPDATE source_state
                         SET last_success_at = ?2, consecutive_failures = 0,
                             last_item_at = CASE WHEN ?3 > 0 THEN ?2 ELSE last_item_at END
                         WHERE source_name = ?1",
                        params![name, now, new_items as i64],
                    )?,
                    Err(why) => tx.execute(
                        "UPDATE source_state
                         SET consecutive_failures = consecutive_failures + 1,
                             last_error = ?3, last_error_at = ?2
                         WHERE source_name = ?1",
                        params![name, now, why],
                    )?,
                };
                tx.commit()?;
                Ok(())
            })
            .await
    }

    /// How every source that was ever polled is doing.
    pub async fn source_health(&self) -> Result<Vec<SourceHealth>> {
        self.0
            .run(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT source_name, first_polled_at, last_success_at, last_item_at,
                            last_error, consecutive_failures
                     FROM source_state ORDER BY source_name",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok(SourceHealth {
                            source_name: row.get(0)?,
                            first_polled_at: row.get(1)?,
                            last_success_at: row.get(2)?,
                            last_item_at: row.get(3)?,
                            last_error: row.get(4)?,
                            consecutive_failures: row.get(5)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }
}

/// Polls in a row that failed before a source counts as failing.
pub const FAILING_AFTER: i64 = 6;
/// How long a source may answer without a new item before it counts as silent.
pub const SILENT_AFTER_SECS: i64 = 30 * 86_400;

/// How a source is doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceHealth {
    pub source_name: String,
    pub first_polled_at: i64,
    pub last_success_at: Option<i64>,
    /// When it last delivered an item not seen before.
    pub last_item_at: Option<i64>,
    pub last_error: Option<String>,
    pub consecutive_failures: i64,
}

impl SourceHealth {
    /// Failed [`FAILING_AFTER`] polls or more in a row.
    pub fn is_failing(&self) -> bool {
        self.consecutive_failures >= FAILING_AFTER
    }

    /// Answers, but has had no new item for [`SILENT_AFTER_SECS`] (counted from its first poll
    /// when it never had one).
    pub fn is_silent(&self, now: i64) -> bool {
        !self.is_failing()
            && self.last_item_at.unwrap_or(self.first_polled_at) < now - SILENT_AFTER_SECS
    }
}
