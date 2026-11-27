//! Observations: every report a source gave, with its text, evidence and the decision the new
//! policy takes on it (in S2 next to the digest, which still reads `items`).

use anyhow::Result;
use mxbot_common::sqlite::rusqlite::{self, params};

use super::Db;

/// One report as stored.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Observation {
    pub source_id: String,
    pub ext_id: String,
    pub first_seen_at: i64,
    pub url: Option<String>,
    pub linked_url: Option<String>,
    pub social: bool,
    pub title: String,
    pub teaser: Option<String>,
    pub body: Option<String>,
    pub published_at: Option<i64>,
    pub category: String,
    pub noise: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub distance_m: Option<f64>,
    pub precision: String,
    pub place_key: Option<String>,
    pub ring: i32,
    pub label: Option<String>,
    /// JSON: the evidence the best one was chosen from.
    pub evidence: String,
    pub class: String,
    /// JSON: the policy's record.
    pub decision: String,
}

/// An observation next to what the digest did with the same item.
#[derive(Debug, Clone, PartialEq)]
pub struct Compared {
    pub title: String,
    pub class: String,
    pub rule: String,
    /// The digest posted it, or is posting it (`items.state` `posted` or `processing`; the
    /// dry run takes items for its digest without posting).
    pub delivered: bool,
}

/// Body text kept per observation, at most (characters).
pub const MAX_BODY_CHARS: usize = 20_000;
/// After this many days body and evidence are cleared; the row stays.
pub const KEEP_TEXT_DAYS: i64 = 14;

impl Db {
    /// Stores a decided observation; `false` when the source already had it.
    pub async fn insert_observation(&self, o: Observation) -> Result<bool> {
        self.0
            .run(move |conn| {
                let body: Option<String> = o.body.map(|b| b.chars().take(MAX_BODY_CHARS).collect());
                let n = conn.execute(
                    "INSERT OR IGNORE INTO observation
                       (source_id, ext_id, first_seen_at, state, attempts, url, linked_url,
                        social, title, teaser, body, published_at, category, noise, lat, lon,
                        distance_m, precision, place_key, ring, label, evidence, class, decision)
                     VALUES (?1, ?2, ?3, 'decided', 0, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                             ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
                    params![
                        o.source_id,
                        o.ext_id,
                        o.first_seen_at,
                        o.url,
                        o.linked_url,
                        o.social,
                        o.title,
                        o.teaser,
                        body,
                        o.published_at,
                        o.category,
                        o.noise,
                        o.lat,
                        o.lon,
                        o.distance_m,
                        o.precision,
                        o.place_key,
                        o.ring,
                        o.label,
                        o.evidence,
                        o.class,
                        o.decision,
                    ],
                )?;
                Ok(n > 0)
            })
            .await
    }

    /// The guids of `guids` that were seen before: in the old seen-set (`items`, any source) or
    /// as an observation of `source_id`. The items found in `items` are marked as listed now.
    pub async fn seen_by_source(
        &self,
        source_id: &str,
        guids: Vec<String>,
    ) -> Result<std::collections::HashSet<String>> {
        let mut seen = self.seen_guids(guids.clone()).await?;
        let source_id = source_id.to_owned();
        let observed = self
            .0
            .run(move |conn| {
                let mut found = std::collections::HashSet::new();
                for chunk in guids.chunks(500) {
                    let marks = vec!["?"; chunk.len()].join(", ");
                    let mut stmt = conn.prepare(&format!(
                        "SELECT ext_id FROM observation WHERE source_id = ?1 AND ext_id IN ({marks})"
                    ))?;
                    let mut values: Vec<&dyn rusqlite::ToSql> = vec![&source_id];
                    values.extend(chunk.iter().map(|g| g as &dyn rusqlite::ToSql));
                    let rows =
                        stmt.query_map(rusqlite::params_from_iter(values), |r| r.get(0))?;
                    for ext_id in rows {
                        found.insert(ext_id?);
                    }
                }
                Ok(found)
            })
            .await?;
        seen.extend(observed);
        Ok(seen)
    }

    /// The observations first seen since `since`, with whether the digest posted the item.
    pub async fn compare_since(&self, since: i64) -> Result<Vec<Compared>> {
        self.0
            .run(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT o.title, o.class, json_extract(o.decision, '$.rule'),
                            COALESCE(i.state IN ('posted', 'processing'), 0)
                     FROM observation o LEFT JOIN items i ON i.guid = o.ext_id
                     WHERE o.first_seen_at >= ?1
                     ORDER BY o.first_seen_at DESC, o.id DESC",
                )?;
                let rows = stmt
                    .query_map(params![since], |r| {
                        Ok(Compared {
                            title: r.get(0)?,
                            class: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            rule: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                            delivered: r.get(3)?,
                        })
                    })?
                    .collect::<rusqlite::Result<_>>()?;
                Ok(rows)
            })
            .await
    }

    /// The newest observation whose title contains every one of `words` (case-insensitive),
    /// as a JSON object for the corpus.
    pub async fn newest_observation_titled(
        &self,
        words: Vec<String>,
    ) -> Result<Option<serde_json::Value>> {
        self.0
            .run(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT source_id, social, title, COALESCE(teaser, ''), COALESCE(body, ''),
                            ring, category, class
                     FROM observation ORDER BY first_seen_at DESC, id DESC LIMIT 2000",
                )?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let title: String = row.get(2)?;
                    let lower = title.to_lowercase();
                    if !words.iter().all(|w| lower.contains(&w.to_lowercase())) {
                        continue;
                    }
                    return Ok(Some(serde_json::json!({
                        "source": row.get::<_, String>(0)?,
                        "social": row.get::<_, bool>(1)?,
                        "title": title,
                        "teaser": row.get::<_, String>(3)?,
                        "body": row.get::<_, String>(4)?,
                        "ring_today": row.get::<_, Option<i32>>(5)?,
                        "category_today": row.get::<_, Option<String>>(6)?,
                        "class_today": row.get::<_, Option<String>>(7)?,
                        "class": "",
                    })));
                }
                Ok(None)
            })
            .await
    }

    /// Every observation's source, id, category, class and decision record (tests).
    #[cfg(test)]
    pub async fn observation_rows(&self) -> Result<Vec<(String, String, String, String, String)>> {
        self.0
            .run(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT source_id, ext_id, category, class, decision FROM observation ORDER BY id",
                )?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?
                    .collect::<rusqlite::Result<_>>()?;
                Ok(rows)
            })
            .await
    }
}
