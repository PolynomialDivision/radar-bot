use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use matrix_sdk::{
    ruma::{
        events::room::message::{ReplacementMetadata, RoomMessageEventContent},
        OwnedEventId,
    },
    Client,
};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::db::Db;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DwdEditResult {
    Ok,
    MissingOriginal,
    Failed,
}

const DWD_ACTIVE_RECONCILE_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub struct AlertConfig {
    /// AGS resolved at startup from ref_point. None = outside Germany, skip NINA.
    pub nina_ags: Option<String>,
    pub ref_point: Option<(f64, f64)>,
    /// DWD warning region keywords, e.g. ["Berlin"]. Empty = skip direct DWD warnings.
    pub dwd_region_keywords: Vec<String>,
    /// Drop USGS events farther than this from ref_point. None = send all.
    pub max_distance_km: Option<f64>,
    pub poll_interval_secs: u64,
}

// ── AGS lookup ────────────────────────────────────────────────────────────────

/// Derive the NINA AGS (Amtlicher Gemeindeschlüssel) from coordinates:
///   1. Nominatim reverse geocode → city name + country check
///   2. NINA completion search → AGS string
/// Returns None if the location is outside Germany or any lookup fails.
pub async fn lookup_nina_ags(http: &reqwest::Client, ref_point: (f64, f64)) -> Option<String> {
    let (lat, lon) = ref_point;

    let rev_url =
        format!("https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json");
    let body = tokio::time::timeout(Duration::from_secs(15), http.get(&rev_url).send())
        .await
        .ok()?
        .ok()?
        .text()
        .await
        .ok()?;

    let json: serde_json::Value = serde_json::from_str(&body).ok()?;

    let country_code = json["address"]["country_code"].as_str()?;
    if country_code != "de" {
        info!("Reference address is outside Germany — NINA disabled");
        return None;
    }

    let city = json["address"]["city"]
        .as_str()
        .or_else(|| json["address"]["town"].as_str())
        .or_else(|| json["address"]["village"].as_str())?;

    info!("Looking up NINA AGS for '{city}'");

    let nina_url = format!(
        "https://nina.api.bund.de/api31/completion/search?q={}",
        crate::url_encode(city)
    );
    let body = tokio::time::timeout(Duration::from_secs(15), http.get(&nina_url).send())
        .await
        .ok()?
        .ok()?
        .text()
        .await
        .ok()?;

    let results: serde_json::Value = serde_json::from_str(&body).ok()?;
    let ags = results[0]["id"].as_str()?.to_owned();
    info!("NINA AGS resolved: {ags} ({city})");
    Some(ags)
}

// ── Main loop ─────────────────────────────────────────────────────────────────

pub async fn alert_loop(client: Client, http: reqwest::Client, db: Db, config: AlertConfig) {
    let mut dwd_area_cache: HashMap<String, bool> = HashMap::new();
    let mut last_dwd_reconcile: Option<Instant> = None;

    loop {
        if let Some(ref ags) = config.nina_ags {
            if let Err(e) = check_nina(&client, &http, &db, ags).await {
                warn!("NINA check failed: {e:#}");
            }
        }
        if !config.dwd_region_keywords.is_empty() {
            let reconcile_active_messages = last_dwd_reconcile
                .map_or(true, |last| last.elapsed() >= DWD_ACTIVE_RECONCILE_INTERVAL);
            if let Err(e) = check_dwd_weather_warnings(
                &client,
                &http,
                &db,
                &config.dwd_region_keywords,
                config.ref_point,
                &mut dwd_area_cache,
                reconcile_active_messages,
            )
            .await
            {
                warn!("DWD weather-warning check failed: {e:#}");
            } else if reconcile_active_messages {
                last_dwd_reconcile = Some(Instant::now());
            }
        }
        if let Err(e) = check_usgs(
            &client,
            &http,
            &db,
            config.ref_point,
            config.max_distance_km,
        )
        .await
        {
            warn!("USGS check failed: {e:#}");
        }
        if let Err(e) = check_gdacs(
            &client,
            &http,
            &db,
            config.ref_point,
            config.max_distance_km,
        )
        .await
        {
            warn!("GDACS check failed: {e:#}");
        }
        sleep(Duration::from_secs(config.poll_interval_secs)).await;
    }
}

// ── NINA/warnung.bund.de ──────────────────────────────────────────────────────

async fn check_nina(client: &Client, http: &reqwest::Client, db: &Db, ags: &str) -> Result<()> {
    let url = format!("https://nina.api.bund.de/api31/dashboard/{ags}");

    let body = tokio::time::timeout(Duration::from_secs(15), http.get(&url).send())
        .await
        .context("NINA fetch timed out")?
        .context("NINA request failed")?
        .text()
        .await
        .context("NINA response read failed")?;

    let json: serde_json::Value = serde_json::from_str(&body).context("NINA JSON parse failed")?;
    let warnings = json.as_array().context("NINA response is not an array")?;

    for w in warnings {
        let id = match w["id"].as_str() {
            Some(id) => id,
            None => continue,
        };

        let data = &w["payload"]["data"];
        if data["msgType"].as_str() == Some("Cancel") {
            continue;
        }

        let headline = data["headline"].as_str().unwrap_or("Warnung");
        let alert_key = nina_alert_key(id, data);
        if db.is_alert_seen(&alert_key).await? {
            continue;
        }

        let desc = format_nina_description(data);
        let link = format!("https://warnung.bund.de/meldung/{id}");
        let (plain, html) = format_alert("NINA/warnung.bund.de", headline, &desc, Some(&link));
        if crate::post_to_rooms(client, &plain, &html).await {
            db.mark_alert_seen(&alert_key, "nina").await?;
            info!("ALERT sent [nina]: {headline}");
        } else {
            warn!("ALERT post failed [nina], will retry next poll: {headline}");
        }
    }

    Ok(())
}

fn nina_alert_key(id: &str, data: &serde_json::Value) -> String {
    let sent = data["sent"].as_str().unwrap_or("");
    let effective = data["effective"].as_str().unwrap_or("");
    let expires = data["expires"].as_str().unwrap_or("");
    let severity = data["severity"].as_str().unwrap_or("");
    format!("nina:{id}:{sent}:{effective}:{expires}:{severity}")
}

fn format_nina_description(data: &serde_json::Value) -> String {
    let mut lines = Vec::new();

    if let Some(area) = data["area"]
        .as_array()
        .and_then(|areas| areas.first())
        .and_then(|area| area["areaDesc"].as_str())
    {
        lines.push(format!("📍 {area}"));
    }

    if let Some(valid) =
        format_rfc3339_validity(data["effective"].as_str(), data["expires"].as_str())
    {
        lines.push(format!("🕒 {valid}"));
    }

    if let Some(details) = concise_sentences(data["description"].as_str().unwrap_or(""), 2) {
        lines.push(format!("ℹ️ {details}"));
    }

    if let Some(advice) = concise_sentences(data["instruction"].as_str().unwrap_or(""), 2) {
        lines.push(format!("✅ {advice}"));
    }

    lines.join("\n")
}

// ── DWD weather warnings ─────────────────────────────────────────────────────

async fn check_dwd_weather_warnings(
    client: &Client,
    http: &reqwest::Client,
    db: &Db,
    region_keywords: &[String],
    ref_point: Option<(f64, f64)>,
    area_cache: &mut HashMap<String, bool>,
    reconcile_active_messages: bool,
) -> Result<()> {
    let body = tokio::time::timeout(
        Duration::from_secs(15),
        http.get("https://www.dwd.de/DWD/warnungen/warnapp/json/warnings.json")
            .send(),
    )
    .await
    .context("DWD warnings fetch timed out")?
    .context("DWD warnings request failed")?
    .text()
    .await
    .context("DWD warnings response read failed")?;

    let json = parse_dwd_jsonp(&body).context("DWD warnings JSON parse failed")?;
    let now_ms = chrono::Utc::now().timestamp_millis();

    // Load the previously-posted warnings before scanning the feed.
    // This map is the source of truth for "already active": if a stable key is
    // present here, the warning was posted and must not be posted again.
    // When all-clear fires, the row is deleted, so the next occurrence of the same
    // warning type is treated as fresh without needing any extra "un-seen" logic.
    let active_map: HashMap<String, crate::db::DwdActiveWarning> = db
        .list_active_dwd_warnings()
        .await?
        .into_iter()
        .map(|w| (w.id.clone(), w))
        .collect();

    // Phase 1: collect all warnings that pass region/time/geometry filters.
    // We collect first so we can group simultaneous warnings for the same event
    // and present them as one message (primary + continuation notes).
    struct ValidWarning {
        bucket: String,
        region: String,
        event_key: String,
        headline: String,
        start_ms: i64,
        end_ms: i64,
        level: i64,
        w: serde_json::Value,
    }
    let mut valid: Vec<ValidWarning> = Vec::new();

    for bucket in ["warnings", "vorabInformation"] {
        let Some(regions) = json[bucket].as_object() else {
            continue;
        };
        for (warncell_id, warnings) in regions {
            let Some(warnings) = warnings.as_array() else {
                continue;
            };
            for w in warnings {
                let region = w["regionName"].as_str().unwrap_or("");
                if !matches_region(region, region_keywords) {
                    continue;
                }
                let start_ms = w["start"].as_i64().unwrap_or(0);
                let end_ms = w["end"].as_i64().unwrap_or(i64::MAX);
                if start_ms > now_ms || end_ms < now_ms {
                    continue;
                }
                if let Some(point) = ref_point {
                    if !dwd_warning_area_contains_point(http, warncell_id, point, area_cache)
                        .await?
                    {
                        info!(
                            "DWD warning skipped outside configured location: {} ({region}, cell {warncell_id})",
                            w["headline"].as_str().unwrap_or("Wetterwarnung")
                        );
                        continue;
                    }
                }
                let event = w["event"]
                    .as_str()
                    .or_else(|| w["headline"].as_str())
                    .unwrap_or("Wetterwarnung")
                    .to_string();
                let event_key = dwd_event_key(&event);
                let headline = w["headline"].as_str().unwrap_or(&event).to_string();
                let level = w["level"].as_i64().unwrap_or(0);
                valid.push(ValidWarning {
                    bucket: bucket.to_string(),
                    region: region.to_string(),
                    event_key,
                    headline,
                    start_ms,
                    end_ms,
                    level,
                    w: w.clone(),
                });
            }
        }
    }

    // Phase 2: group by (bucket, region, event family) so simultaneous warnings for
    // the same event (e.g. Level 4 HITZE 11-19h + Level 2 HITZE 19h-next) are
    // shown as one message with a continuation note rather than two messages.
    // Within each group: highest level = primary; the rest = continuations.
    let mut groups: HashMap<(String, String, String), Vec<usize>> = HashMap::new();
    for (i, vw) in valid.iter().enumerate() {
        groups
            .entry((vw.bucket.clone(), vw.region.clone(), vw.event_key.clone()))
            .or_default()
            .push(i);
    }

    let mut current_active: HashSet<String> = HashSet::new();
    let mut active_level_by_event: HashMap<(String, String, String), i64> = HashMap::new();
    // Keys from active_map that were promoted (level change) this poll.
    // The all-clear loop skips these so it doesn't fire a spurious all-clear
    // for the old-level row that was just replaced.
    let mut promoted_keys: HashSet<String> = HashSet::new();

    for ((bucket, region, event_key), indices) in &groups {
        // Sort: highest level first; within same level, earliest start first.
        let mut group: Vec<&ValidWarning> = indices.iter().map(|&i| &valid[i]).collect();
        group.sort_by(|a, b| b.level.cmp(&a.level).then(a.start_ms.cmp(&b.start_ms)));

        let primary = group[0];
        let continuations = &group[1..];

        let primary_id = dwd_warning_stable_key(bucket, region, &primary.w);

        // Register all warnings in this group for tracking.
        for vw in &group {
            let id = dwd_warning_stable_key(bucket, region, &vw.w);
            current_active.insert(id);
            active_level_by_event
                .entry((bucket.clone(), region.clone(), event_key.clone()))
                .and_modify(|e| *e = (*e).max(vw.level))
                .or_insert(vw.level);
        }

        // Build secondary notes sorted by start time. The primary remains the worst
        // active warning; lower overlapping/follow-up warnings are shown underneath.
        let mut secondary: Vec<&ValidWarning> = continuations.to_vec();
        secondary.sort_by(|a, b| a.start_ms.cmp(&b.start_ms).then(b.level.cmp(&a.level)));
        let cont_notes: Vec<String> = secondary
            .iter()
            .map(|vw| format_dwd_secondary_warning(&primary.w, &vw.w))
            .collect();

        if let Some(active) = active_map.get(&primary_id) {
            // Already posted. Edit in-place if the primary time window changed.
            if active.start_ms != primary.start_ms || active.end_ms != primary.end_ms {
                let (new_plain, new_html, edit_result) = match active.event_ids_json.as_deref() {
                    Some(ids_json) => {
                        let desc = format_dwd_description_with_continuations(
                            region,
                            &primary.w,
                            &cont_notes,
                        );
                        let (p, h) = format_alert(
                            "DWD Weather Warnings",
                            &primary.headline,
                            &desc,
                            Some("https://www.dwd.de/DE/wetter/warnungen_gemeinden/warnWetter_node.html"),
                        );
                        let edit_result = edit_dwd_warning_in_rooms(client, ids_json, &p, &h).await;
                        (p, h, edit_result)
                    }
                    // No event IDs stored (pre-upgrade row): can't edit.
                    _ => (String::new(), String::new(), DwdEditResult::Failed),
                };
                let edited = matches!(edit_result, DwdEditResult::Ok);
                let plain_opt = if edited {
                    Some(new_plain.as_str())
                } else {
                    None
                };
                let html_opt = if edited {
                    Some(new_html.as_str())
                } else {
                    None
                };
                db.update_dwd_active_warning(
                    &primary_id,
                    primary.start_ms,
                    primary.end_ms,
                    plain_opt,
                    html_opt,
                )
                .await?;
                if edited {
                    info!("ALERT updated [dwd]: {} ({region})", primary.headline);
                } else if matches!(edit_result, DwdEditResult::MissingOriginal) {
                    if repost_dwd_active_warning(
                        client,
                        db,
                        &primary_id,
                        &primary.headline,
                        region,
                        event_key,
                        primary.start_ms,
                        primary.end_ms,
                        &new_plain,
                        &new_html,
                    )
                    .await?
                    {
                        info!(
                            "ALERT reposted [dwd] after missing original: {} ({region})",
                            primary.headline
                        );
                    } else {
                        warn!(
                            "ALERT repost failed [dwd], will retry: {} ({region})",
                            primary.headline
                        );
                    }
                }
            } else if reconcile_active_messages {
                if let Some(ids_json) = active.event_ids_json.as_deref() {
                    let desc =
                        format_dwd_description_with_continuations(region, &primary.w, &cont_notes);
                    let (new_plain, new_html) = format_alert(
                        "DWD Weather Warnings",
                        &primary.headline,
                        &desc,
                        Some(
                            "https://www.dwd.de/DE/wetter/warnungen_gemeinden/warnWetter_node.html",
                        ),
                    );
                    match edit_dwd_warning_in_rooms(client, ids_json, &new_plain, &new_html).await {
                        DwdEditResult::Ok => {
                            db.update_dwd_active_warning(
                                &primary_id,
                                primary.start_ms,
                                primary.end_ms,
                                Some(&new_plain),
                                Some(&new_html),
                            )
                            .await?;
                            info!("ALERT reconciled [dwd]: {} ({region})", primary.headline);
                        }
                        DwdEditResult::MissingOriginal => {
                            if repost_dwd_active_warning(
                                client,
                                db,
                                &primary_id,
                                &primary.headline,
                                region,
                                event_key,
                                primary.start_ms,
                                primary.end_ms,
                                &new_plain,
                                &new_html,
                            )
                            .await?
                            {
                                info!(
                                    "ALERT reposted [dwd] after missing original during reconcile: {} ({region})",
                                    primary.headline
                                );
                            } else {
                                warn!(
                                    "ALERT reconcile repost failed [dwd], will retry: {} ({region})",
                                    primary.headline
                                );
                            }
                        }
                        DwdEditResult::Failed => {
                            warn!(
                                "ALERT reconcile edit failed [dwd], will retry later: {} ({region})",
                                primary.headline
                            );
                        }
                    }
                }
            }
            continue;
        }

        // New group: post the primary with continuation notes appended.
        // Check if the same event is already active at a different level.
        // If so, edit the existing message to show the new level instead of
        // posting a second message. The old DB row is atomically replaced so
        // the Matrix event IDs (pointing to the same message) are preserved.
        let old_same_event = active_map.iter().find(|(k, _)| {
            parse_stable_key(k)
                .map(|(kb, kr, ke, ol)| {
                    kb == bucket.as_str()
                        && kr == region.as_str()
                        && dwd_event_key(ke) == event_key.as_str()
                        && ol != primary.level
                })
                .unwrap_or(false)
        });

        if let Some((old_id, old_warning)) = old_same_event {
            let old_level = parse_stable_key(old_id).map(|(_, _, _, l)| l).unwrap_or(0);
            let desc = format_dwd_description_with_continuations(region, &primary.w, &cont_notes);
            let (new_plain, new_html) = format_alert(
                "DWD Weather Warnings",
                &primary.headline,
                &desc,
                Some("https://www.dwd.de/DE/wetter/warnungen_gemeinden/warnWetter_node.html"),
            );

            match old_warning.event_ids_json.as_deref() {
                Some(ids_json) => {
                    // Edit the existing message: replace content with new level + append notice.
                    let (edit_plain, edit_html) = append_level_change_to_warning(
                        &new_plain,
                        &new_html,
                        old_level,
                        primary.level,
                        event_key,
                    );
                    match edit_dwd_warning_in_rooms(client, ids_json, &edit_plain, &edit_html).await
                    {
                        DwdEditResult::Ok => {
                            // Store new_plain (without the footer) so future all-clear appends cleanly.
                            db.promote_dwd_active_warning(
                                old_id,
                                &primary_id,
                                &primary.headline,
                                region,
                                event_key,
                                ids_json,
                                primary.start_ms,
                                primary.end_ms,
                                &new_plain,
                                &new_html,
                            )
                            .await?;
                            promoted_keys.insert(old_id.clone());
                            let dir = if primary.level > old_level {
                                "⬆"
                            } else {
                                "⬇"
                            };
                            info!(
                                "LEVEL CHANGE {dir} [dwd]: {} ({region}) L{old_level} → L{}",
                                primary.headline, primary.level
                            );
                        }
                        DwdEditResult::MissingOriginal => {
                            if repost_dwd_active_warning(
                                client,
                                db,
                                &primary_id,
                                &primary.headline,
                                region,
                                event_key,
                                primary.start_ms,
                                primary.end_ms,
                                &new_plain,
                                &new_html,
                            )
                            .await?
                            {
                                db.mark_dwd_allclear_sent(old_id).await?;
                                promoted_keys.insert(old_id.clone());
                                info!(
                                    "LEVEL CHANGE reposted [dwd] after missing original: {} ({region})",
                                    primary.headline
                                );
                            } else {
                                warn!(
                                    "LEVEL CHANGE repost failed [dwd], will retry: {} ({region})",
                                    primary.headline
                                );
                            }
                        }
                        DwdEditResult::Failed => {
                            warn!(
                                "LEVEL CHANGE failed [dwd], will retry: {} ({region})",
                                primary.headline
                            );
                        }
                    }
                }
                None => {
                    // No event IDs (pre-upgrade row) — can't edit. Post fresh and
                    // clean up the stale row so no spurious all-clear fires.
                    let (all_ok, room_event_ids) =
                        post_dwd_warning_to_rooms(client, &new_plain, &new_html).await;
                    if all_ok {
                        let ids_json = serde_json::to_string(&room_event_ids)
                            .unwrap_or_else(|_| String::from("{}"));
                        db.promote_dwd_active_warning(
                            old_id,
                            &primary_id,
                            &primary.headline,
                            region,
                            event_key,
                            &ids_json,
                            primary.start_ms,
                            primary.end_ms,
                            &new_plain,
                            &new_html,
                        )
                        .await?;
                        promoted_keys.insert(old_id.clone());
                        info!(
                            "LEVEL CHANGE (post fallback) [dwd]: {} ({region})",
                            primary.headline
                        );
                    } else {
                        warn!(
                            "LEVEL CHANGE post failed [dwd], will retry: {} ({region})",
                            primary.headline
                        );
                    }
                }
            }
        } else {
            // Genuinely new warning — no prior message for this event type.
            let desc = format_dwd_description_with_continuations(region, &primary.w, &cont_notes);
            let (plain, html) = format_alert(
                "DWD Weather Warnings",
                &primary.headline,
                &desc,
                Some("https://www.dwd.de/DE/wetter/warnungen_gemeinden/warnWetter_node.html"),
            );
            let (all_ok, room_event_ids) = post_dwd_warning_to_rooms(client, &plain, &html).await;
            if all_ok {
                let ids_json =
                    serde_json::to_string(&room_event_ids).unwrap_or_else(|_| String::from("{}"));
                db.mark_dwd_alert_posted(
                    &primary_id,
                    &primary.headline,
                    region,
                    event_key,
                    &ids_json,
                    primary.start_ms,
                    primary.end_ms,
                    &plain,
                    &html,
                )
                .await?;
                info!("ALERT sent [dwd]: {} ({region})", primary.headline);
            } else {
                warn!(
                    "ALERT post failed [dwd], will retry next poll: {} ({region})",
                    primary.headline
                );
            }
        }
    }

    // Genuine all-clears: warnings that disappeared from DWD with no replacement
    // at any level. Level changes are handled above in Phase 2 (promoted_keys).
    for (id, prev) in &active_map {
        if current_active.contains(id) || promoted_keys.contains(id) {
            continue;
        }
        // Also skip if a level-change is in progress but the edit failed last poll:
        // Phase 2 will retry it; firing an all-clear here would be wrong.
        if let Some((b, r, e, _)) = parse_stable_key(id) {
            if active_level_by_event.contains_key(&(b.to_owned(), r.to_owned(), dwd_event_key(e))) {
                continue;
            }
        }
        let sent = match (
            prev.event_ids_json.as_deref(),
            prev.original_plain.as_deref(),
            prev.original_html.as_deref(),
        ) {
            (Some(ids_json), Some(orig_plain), Some(orig_html)) => {
                let (new_plain, new_html) = append_allclear_to_warning(orig_plain, orig_html);
                edit_dwd_warning_in_rooms(client, ids_json, &new_plain, &new_html).await
            }
            _ => {
                let (plain, html) = format_dwd_allclear(&prev.headline, &prev.region);
                if crate::post_to_rooms(client, &plain, &html).await {
                    DwdEditResult::Ok
                } else {
                    DwdEditResult::Failed
                }
            }
        };
        match sent {
            DwdEditResult::Ok => {
                db.mark_dwd_allclear_sent(id).await?;
                info!("ALL-CLEAR sent [dwd]: {} ({})", prev.headline, prev.region);
            }
            DwdEditResult::MissingOriginal => {
                db.mark_dwd_allclear_sent(id).await?;
                info!(
                    "ALL-CLEAR skipped [dwd], original message is gone: {} ({})",
                    prev.headline, prev.region
                );
            }
            DwdEditResult::Failed => {
                warn!(
                    "ALL-CLEAR send failed [dwd], will retry next poll: {} ({})",
                    prev.headline, prev.region
                );
            }
        }
    }

    Ok(())
}

fn parse_dwd_jsonp(body: &str) -> Option<serde_json::Value> {
    let trimmed = body.trim();
    let json = if trimmed.starts_with("warnWetter.loadWarnings(") {
        trimmed
            .strip_prefix("warnWetter.loadWarnings(")?
            .trim_end_matches(");")
            .trim_end_matches(')')
    } else {
        trimmed
    };
    serde_json::from_str(json).ok()
}

fn matches_region(region: &str, keywords: &[String]) -> bool {
    let region = region.to_lowercase();
    keywords.iter().any(|kw| {
        let kw = kw.trim().to_lowercase();
        !kw.is_empty() && region.contains(&kw)
    })
}

fn dwd_warning_stable_key(bucket: &str, region: &str, w: &serde_json::Value) -> String {
    let event = w["event"].as_str().unwrap_or("warning");
    let event_key = dwd_event_key(event);
    let level = w["level"].as_i64().unwrap_or(0);
    // Excludes start/end: DWD refreshes `start` to the request time on every poll
    // and `end` may be extended as the forecast evolves. Including them would treat
    // each refresh as a new warning and cause duplicate posts.
    format!("dwd:{bucket}:{region}:{event_key}:{level}")
}

fn dwd_event_key(event: &str) -> String {
    let event_upper = event.trim().to_uppercase();
    let family = if contains_any(&event_upper, &["HITZE"]) {
        Some("HITZE")
    } else if contains_any(&event_upper, &["GEWITTER"]) {
        Some("GEWITTER")
    } else if contains_any(&event_upper, &["WIND", "STURM", "ORKAN", "BÖEN", "BOEEN"]) {
        Some("WIND")
    } else if contains_any(&event_upper, &["REGEN", "NIEDERSCHLAG"]) {
        Some("REGEN")
    } else if contains_any(
        &event_upper,
        &["SCHNEE", "GLÄTTE", "GLAETTE", "GLATTEIS", "EISREGEN"],
    ) {
        Some("SCHNEE_GLAETTE")
    } else if contains_any(&event_upper, &["FROST"]) {
        Some("FROST")
    } else if contains_any(&event_upper, &["NEBEL"]) {
        Some("NEBEL")
    } else if contains_any(&event_upper, &["TAUWETTER"]) {
        Some("TAUWETTER")
    } else if contains_any(&event_upper, &["UV"]) {
        Some("UV")
    } else if contains_any(&event_upper, &["WALDBRAND", "GRASLAND", "FEUERINDEX"]) {
        Some("FIRE_RISK")
    } else {
        None
    };
    family.map(str::to_owned).unwrap_or(event_upper)
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

async fn dwd_warning_area_contains_point(
    http: &reqwest::Client,
    warncell_id: &str,
    point: (f64, f64),
    cache: &mut HashMap<String, bool>,
) -> Result<bool> {
    if let Some(inside) = cache.get(warncell_id) {
        return Ok(*inside);
    }

    match fetch_dwd_warning_area(http, warncell_id).await {
        Ok(Some(geometry)) => {
            let inside = geometry_contains_point(&geometry, point);
            cache.insert(warncell_id.to_owned(), inside);
            Ok(inside)
        }
        Ok(None) => {
            warn!(
                "DWD warning-area geometry not found for cell {warncell_id}; falling back to region-name match"
            );
            cache.insert(warncell_id.to_owned(), true);
            Ok(true)
        }
        Err(e) => {
            warn!(
                "DWD warning-area geometry lookup failed for cell {warncell_id}: {e:#}; falling back to region-name match"
            );
            cache.insert(warncell_id.to_owned(), true);
            Ok(true)
        }
    }
}

async fn fetch_dwd_warning_area(
    http: &reqwest::Client,
    warncell_id: &str,
) -> Result<Option<serde_json::Value>> {
    const DWD_GEOMETRY_LAYERS: &[&str] = &[
        "Warngebiete_Kreise",
        "Warngebiete_Gemeinden",
        "Warngebiete_Bundeslaender",
        "Warngebiete_Binnenseen",
        "Warngebiete_Kueste",
    ];

    for layer in DWD_GEOMETRY_LAYERS {
        let url = format!(
            "https://maps.dwd.de/geoserver/dwd/ows?service=WFS&version=1.1.0&request=GetFeature&typeName=dwd:{layer}&outputFormat=application/json&CQL_FILTER=WARNCELLID%3D{}",
            crate::url_encode(warncell_id)
        );
        let body = tokio::time::timeout(Duration::from_secs(15), http.get(&url).send())
            .await
            .context("DWD warning-area fetch timed out")?
            .context("DWD warning-area request failed")?
            .text()
            .await
            .context("DWD warning-area response read failed")?;
        let json: serde_json::Value =
            serde_json::from_str(&body).context("DWD warning-area JSON parse failed")?;
        if let Some(geometry) = json["features"]
            .as_array()
            .and_then(|features| features.first())
            .and_then(|feature| feature.get("geometry"))
        {
            return Ok(Some(geometry.clone()));
        }
    }

    Ok(None)
}

fn geometry_contains_point(geometry: &serde_json::Value, point: (f64, f64)) -> bool {
    match geometry["type"].as_str() {
        Some("Polygon") => polygon_contains_point(&geometry["coordinates"], point),
        Some("MultiPolygon") => geometry["coordinates"]
            .as_array()
            .map(|polygons| {
                polygons
                    .iter()
                    .any(|polygon| polygon_contains_point(polygon, point))
            })
            .unwrap_or(false),
        _ => false,
    }
}

fn polygon_contains_point(coordinates: &serde_json::Value, point: (f64, f64)) -> bool {
    let Some(rings) = coordinates.as_array() else {
        return false;
    };
    let Some(outer) = rings.first() else {
        return false;
    };
    if !ring_contains_point(outer, point) {
        return false;
    }
    !rings
        .iter()
        .skip(1)
        .any(|hole| ring_contains_point(hole, point))
}

fn ring_contains_point(ring: &serde_json::Value, point: (f64, f64)) -> bool {
    let Some(points) = ring.as_array() else {
        return false;
    };
    if points.len() < 3 {
        return false;
    }

    let (lat, lon) = point;
    let mut inside = false;
    let mut prev = points.len() - 1;
    for current in 0..points.len() {
        let Some((x1, y1)) = geojson_position(points[current].as_array()) else {
            prev = current;
            continue;
        };
        let Some((x2, y2)) = geojson_position(points[prev].as_array()) else {
            prev = current;
            continue;
        };
        let intersects =
            ((y1 > lat) != (y2 > lat)) && (lon < (x2 - x1) * (lat - y1) / (y2 - y1) + x1);
        if intersects {
            inside = !inside;
        }
        prev = current;
    }

    inside
}

fn geojson_position(position: Option<&Vec<serde_json::Value>>) -> Option<(f64, f64)> {
    let position = position?;
    let lon = position.first()?.as_f64()?;
    let lat = position.get(1)?.as_f64()?;
    Some((lon, lat))
}

fn format_dwd_description_with_continuations(
    region: &str,
    w: &serde_json::Value,
    continuations: &[String],
) -> String {
    let level = w["level"].as_i64().unwrap_or(0);
    let description = w["description"].as_str().unwrap_or("").trim();
    let instruction = w["instruction"].as_str().unwrap_or("").trim();

    let mut parts = vec![
        format!(
            "{} {}",
            dwd_level_emoji(level),
            dwd_level_label_for_event(w["event"].as_str().unwrap_or(""), level)
        ),
        format!("📍 {region}"),
    ];
    if let Some(valid) = format_dwd_validity(w) {
        parts.push(format!("🕒 {valid}"));
    }
    if let Some(details) = concise_sentences(description, 2) {
        parts.push(format!("ℹ️ {details}"));
    }
    if let Some(advice) = concise_sentences(instruction, 1) {
        parts.push(format!("✅ {advice}"));
    }
    for note in continuations {
        parts.push(note.clone());
    }
    parts.join("\n")
}

/// One-line note for a lower-severity warning in the same event family.
/// Overlapping warnings are shown as simultaneous; later warnings as follow-ups.
fn format_dwd_secondary_warning(primary: &serde_json::Value, w: &serde_json::Value) -> String {
    let level = w["level"].as_i64().unwrap_or(0);
    let emoji = dwd_level_emoji(level);
    let label = dwd_level_label_for_event(w["event"].as_str().unwrap_or(""), level);
    let primary_start = primary["start"].as_i64().unwrap_or(0);
    let primary_end = primary["end"].as_i64().unwrap_or(0);
    let secondary_start = w["start"].as_i64().unwrap_or(0);
    let secondary_end = w["end"].as_i64().unwrap_or(0);
    let prefix = if secondary_end <= primary_start {
        "davor"
    } else if secondary_start < primary_end {
        "gleichzeitig"
    } else {
        "danach"
    };
    let end_str = w["end"]
        .as_i64()
        .and_then(|ms| chrono::DateTime::from_timestamp_millis(ms))
        .map(|dt| {
            let local = dt.with_timezone(&chrono::Local);
            let today = chrono::Local::now().date_naive();
            if local.date_naive() == today {
                local.format("%H:%M").to_string()
            } else {
                local.format("%a %H:%M").to_string()
            }
        });
    match end_str {
        Some(t) => format!("({prefix}: {emoji} {label} bis {t})"),
        None => format!("({prefix}: {emoji} {label})"),
    }
}

fn dwd_level_emoji(level: i64) -> &'static str {
    match level {
        1 => "🟡",
        2 => "⚠️",
        3 => "🟠",
        50 => "🟠",
        51 => "🔴",
        _ => "🔴", // 4, 5, or anything higher
    }
}

fn dwd_level_label(level: i64) -> &'static str {
    match level {
        1 => "Vorinformation",
        2 => "Warnung",
        3 => "Starke Warnung",
        4 => "Unwetterwarnung",
        5 => "Extreme Unwetterwarnung",
        50 => "Starke Hitzewarnung",
        51 => "Extreme Hitzewarnung",
        _ => "Extremwarnung",
    }
}

fn dwd_level_label_for_event(event: &str, level: i64) -> &'static str {
    match dwd_event_key(event).as_str() {
        "HITZE" => match level {
            50 => "Starke Hitzewarnung",
            51 => "Extreme Hitzewarnung",
            _ => dwd_level_label(level),
        },
        _ => dwd_level_label(level),
    }
}

/// Parse a stable key back into its components.
/// Format: `dwd:{bucket}:{region}:{event}:{level}`
fn parse_stable_key(id: &str) -> Option<(&str, &str, &str, i64)> {
    let rest = id.strip_prefix("dwd:")?;
    let mut parts = rest.splitn(4, ':');
    let bucket = parts.next()?;
    let region = parts.next()?;
    let event = parts.next()?;
    let level: i64 = parts.next()?.parse().ok()?;
    Some((bucket, region, event, level))
}

fn format_dwd_validity(w: &serde_json::Value) -> Option<String> {
    let start = w["start"].as_i64()?;
    let end = w["end"].as_i64()?;
    let start = chrono::DateTime::from_timestamp_millis(start)?.with_timezone(&chrono::Local);
    let end = chrono::DateTime::from_timestamp_millis(end)?.with_timezone(&chrono::Local);

    if start.date_naive() == end.date_naive() {
        Some(format!(
            "{}-{}",
            start.format("%a, %d %b %H:%M"),
            end.format("%H:%M")
        ))
    } else {
        Some(format!(
            "{} - {}",
            start.format("%a, %d %b %H:%M"),
            end.format("%a, %d %b %H:%M")
        ))
    }
}

fn format_rfc3339_validity(start: Option<&str>, end: Option<&str>) -> Option<String> {
    let start = start
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Local));
    let end = end
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Local));

    match (start, end) {
        (Some(start), Some(end)) if start.date_naive() == end.date_naive() => Some(format!(
            "{}-{}",
            start.format("%a, %d %b %H:%M"),
            end.format("%H:%M")
        )),
        (Some(start), Some(end)) => Some(format!(
            "{} - {}",
            start.format("%a, %d %b %H:%M"),
            end.format("%a, %d %b %H:%M")
        )),
        (Some(start), None) => Some(format!("from {}", start.format("%a, %d %b %H:%M"))),
        (None, Some(end)) => Some(format!("until {}", end.format("%a, %d %b %H:%M"))),
        (None, None) => None,
    }
}

fn format_usgs_description(f: &serde_json::Value) -> String {
    let props = &f["properties"];
    let mut lines = Vec::new();

    if let Some(place) = props["place"].as_str().filter(|s| !s.trim().is_empty()) {
        lines.push(format!("📍 {place}"));
    }

    if let Some(time_ms) = props["time"].as_i64() {
        if let Some(dt) = chrono::DateTime::from_timestamp_millis(time_ms) {
            let local = dt.with_timezone(&chrono::Local);
            lines.push(format!("🕒 {}", local.format("%a, %d %b %H:%M")));
        }
    }

    let mut detail = Vec::new();
    if let Some(mag) = props["mag"].as_f64() {
        detail.push(format!("Magnitude {mag:.1}"));
    }
    if let Some(alert) = props["alert"].as_str().filter(|s| !s.trim().is_empty()) {
        detail.push(format!("USGS alert: {alert}"));
    }
    if !detail.is_empty() {
        lines.push(format!("ℹ️ {}", detail.join(" · ")));
    }

    lines.join("\n")
}

fn format_gdacs_description(item: &crate::FeedItem, coords: Option<(f64, f64)>) -> String {
    let mut lines = Vec::new();

    if let Some((lat, lon)) = coords {
        lines.push(format!("📍 {:.2}, {:.2}", lat, lon));
    }
    if let Some(ts) = item.published_at {
        if let Some(dt) = chrono::DateTime::from_timestamp(ts, 0) {
            let local = dt.with_timezone(&chrono::Local);
            lines.push(format!("🕒 {}", local.format("%a, %d %b %H:%M")));
        }
    }
    if let Some(details) = concise_sentences(item.description.as_deref().unwrap_or(""), 2) {
        lines.push(format!("ℹ️ {details}"));
    }

    lines.join("\n")
}

fn concise_sentences(text: &str, max_sentences: usize) -> Option<String> {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return None;
    }

    let mut sentences = Vec::new();
    let mut start = 0usize;
    for (idx, ch) in text.char_indices() {
        if matches!(ch, '.' | '!' | '?') {
            let end = idx + ch.len_utf8();
            let sentence = text[start..end].trim();
            if !sentence.is_empty() && !sentences.contains(&sentence) {
                sentences.push(sentence);
            }
            start = end;
            if sentences.len() >= max_sentences {
                break;
            }
        }
    }

    if sentences.is_empty() {
        Some(text)
    } else {
        Some(sentences.join(" "))
    }
}

// ── USGS significant earthquakes ──────────────────────────────────────────────

async fn check_usgs(
    client: &Client,
    http: &reqwest::Client,
    db: &Db,
    ref_point: Option<(f64, f64)>,
    max_distance_km: Option<f64>,
) -> Result<()> {
    let body = tokio::time::timeout(
        Duration::from_secs(15),
        http.get(
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson",
        )
        .send(),
    )
    .await
    .context("USGS fetch timed out")?
    .context("USGS request failed")?
    .text()
    .await
    .context("USGS response read failed")?;

    let json: serde_json::Value = serde_json::from_str(&body).context("USGS JSON parse failed")?;
    let empty = vec![];
    let features = json["features"].as_array().unwrap_or(&empty);
    let cutoff_ms = (chrono::Utc::now().timestamp() - 48 * 3600) * 1000;

    for f in features {
        let id = match f["id"].as_str() {
            Some(id) => id,
            None => continue,
        };

        // USGS time is milliseconds since epoch.
        if f["properties"]["time"]
            .as_i64()
            .map_or(false, |t| t < cutoff_ms)
        {
            continue;
        }

        if db.is_alert_seen(id).await? {
            continue;
        }

        // GeoJSON coordinates are [lon, lat, depth].
        let lon = f["geometry"]["coordinates"][0].as_f64();
        let lat = f["geometry"]["coordinates"][1].as_f64();

        if let (Some(max_km), Some((ref_lat, ref_lon))) = (max_distance_km, ref_point) {
            match (lat, lon) {
                (Some(lat), Some(lon)) if haversine_km(ref_lat, ref_lon, lat, lon) <= max_km => {}
                (Some(_), Some(_)) => continue, // too far
                _ => continue,                  // no coordinates — can't verify proximity, skip
            }
        }

        let props = &f["properties"];
        let title = props["title"].as_str().unwrap_or("Earthquake");
        let url = props["url"].as_str();

        let desc = format_usgs_description(f);

        let (plain, html) = format_alert("USGS Earthquakes", title, &desc, url);
        if crate::post_to_rooms(client, &plain, &html).await {
            db.mark_alert_seen(id, "usgs").await?;
            info!("ALERT sent [usgs]: {title}");
        } else {
            warn!("ALERT post failed [usgs], will retry next poll: {title}");
        }
    }

    Ok(())
}

// ── GDACS global disasters ────────────────────────────────────────────────────

async fn check_gdacs(
    client: &Client,
    http: &reqwest::Client,
    db: &Db,
    ref_point: Option<(f64, f64)>,
    max_distance_km: Option<f64>,
) -> Result<()> {
    let xml = tokio::time::timeout(
        Duration::from_secs(15),
        http.get("https://www.gdacs.org/xml/rss.xml").send(),
    )
    .await
    .context("GDACS fetch timed out")?
    .context("GDACS request failed")?
    .text()
    .await
    .context("GDACS response read failed")?;

    let coords = gdacs_coords(&xml);
    let cutoff = chrono::Utc::now().timestamp() - 48 * 3600;

    for item in crate::parse_feed(&xml, "GDACS") {
        // Green = routine monitoring, not actionable. Only send Orange and Red.
        if item.title.to_lowercase().starts_with("green") {
            continue;
        }

        // Skip events older than 48 hours to avoid backlog spam on first run.
        if item.published_at.map_or(false, |t| t < cutoff) {
            continue;
        }

        // Distance filter using <geo:lat>/<geo:long> from the RSS item.
        if let (Some(max_km), Some((ref_lat, ref_lon))) = (max_distance_km, ref_point) {
            match coords.get(&item.guid) {
                Some(&(lat, lon)) if haversine_km(ref_lat, ref_lon, lat, lon) <= max_km => {}
                Some(_) => continue, // too far
                None => continue,    // no coordinates — can't verify proximity, skip
            }
        }

        if db.is_alert_seen(&item.guid).await? {
            continue;
        }

        let desc = format_gdacs_description(&item, coords.get(&item.guid).copied());
        let (plain, html) = format_alert("GDACS", &item.title, &desc, item.link.as_deref());
        if crate::post_to_rooms(client, &plain, &html).await {
            db.mark_alert_seen(&item.guid, "gdacs").await?;
            info!("ALERT sent [gdacs]: {}", item.title);
        } else {
            warn!(
                "ALERT post failed [gdacs], will retry next poll: {}",
                item.title
            );
        }
    }

    Ok(())
}

/// Extract a map of guid → (lat, lon) from GDACS RSS XML.
/// GDACS includes <geo:lat> and <geo:long> per item.
fn gdacs_coords(xml: &str) -> std::collections::HashMap<String, (f64, f64)> {
    let mut map = std::collections::HashMap::new();
    for block in xml.split("<item>").skip(1) {
        let item = &block[..block.find("</item>").unwrap_or(block.len())];
        let guid = xml_tag(item, "guid");
        let lat = xml_tag(item, "geo:lat").and_then(|s| s.parse::<f64>().ok());
        let lon = xml_tag(item, "geo:long").and_then(|s| s.parse::<f64>().ok());
        if let (Some(guid), Some(lat), Some(lon)) = (guid, lat, lon) {
            map.insert(guid, (lat, lon));
        }
    }
    map
}

fn xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_owned())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `(all_ok, room_id_str → event_id_str)`.
/// Collects event IDs from all rooms so they can be stored for future edits.
async fn post_dwd_warning_to_rooms(
    client: &Client,
    plain: &str,
    html: &str,
) -> (bool, HashMap<String, String>) {
    let mut event_ids: HashMap<String, String> = HashMap::new();
    let mut all_ok = true;
    for room in client.joined_rooms() {
        match room
            .send(RoomMessageEventContent::text_html(plain, html))
            .await
        {
            Ok(resp) => {
                event_ids.insert(
                    room.room_id().to_string(),
                    resp.response.event_id.to_string(),
                );
            }
            Err(e) => {
                error!("Failed to post DWD warning to {}: {e}", room.room_id());
                all_ok = false;
            }
        }
    }
    (all_ok, event_ids)
}

#[allow(clippy::too_many_arguments)]
async fn repost_dwd_active_warning(
    client: &Client,
    db: &Db,
    id: &str,
    headline: &str,
    region: &str,
    event: &str,
    start_ms: i64,
    end_ms: i64,
    plain: &str,
    html: &str,
) -> Result<bool> {
    let (all_ok, room_event_ids) = post_dwd_warning_to_rooms(client, plain, html).await;
    if !all_ok {
        return Ok(false);
    }
    let ids_json = serde_json::to_string(&room_event_ids).unwrap_or_else(|_| String::from("{}"));
    db.mark_dwd_alert_posted(
        id, headline, region, event, &ids_json, start_ms, end_ms, plain, html,
    )
    .await?;
    Ok(true)
}

/// Edit the original DWD warning message in each room to append the all-clear notice.
/// `event_ids_json` is a JSON object mapping room_id strings to event_id strings.
/// Returns whether all edits succeeded, failed transiently, or likely targeted
/// messages that are gone from the room.
async fn edit_dwd_warning_in_rooms(
    client: &Client,
    event_ids_json: &str,
    new_plain: &str,
    new_html: &str,
) -> DwdEditResult {
    let map: HashMap<String, String> = match serde_json::from_str(event_ids_json) {
        Ok(m) => m,
        Err(e) => {
            warn!("Could not parse DWD event_ids_json for edit: {e}");
            return DwdEditResult::Failed;
        }
    };

    let mut all_ok = true;
    let mut missing_original = false;
    for room in client.joined_rooms() {
        let room_id = room.room_id().to_string();
        let Some(event_id_str) = map.get(&room_id) else {
            continue;
        };
        let event_id = match event_id_str.parse::<OwnedEventId>() {
            Ok(id) => id,
            Err(e) => {
                warn!("Invalid event ID {event_id_str} for DWD edit: {e}");
                all_ok = false;
                continue;
            }
        };
        let edited = RoomMessageEventContent::text_html(new_plain, new_html)
            .make_replacement(ReplacementMetadata::new(event_id, None));
        if let Err(e) = room.send(edited).await {
            error!("Failed to edit DWD warning in {room_id}: {e}");
            all_ok = false;
            if looks_like_missing_matrix_event(&e.to_string()) {
                missing_original = true;
            }
        }
    }
    if all_ok {
        DwdEditResult::Ok
    } else if missing_original {
        DwdEditResult::MissingOriginal
    } else {
        DwdEditResult::Failed
    }
}

fn looks_like_missing_matrix_event(error: &str) -> bool {
    let e = error.to_lowercase();
    [
        "not found",
        "unknown event",
        "event not found",
        "m_not_found",
        "redacted",
        "deleted",
        "cannot replace",
    ]
    .iter()
    .any(|needle| e.contains(needle))
}

/// Append an all-clear notice to the original warning content.
/// The `<hr>` separator is well-supported in Matrix HTML clients.
fn append_allclear_to_warning(original_plain: &str, original_html: &str) -> (String, String) {
    let time = chrono::Local::now().format("%H:%M").to_string();
    let new_plain =
        format!("{original_plain}\n\n---\n✅ Entwarnung ({time}): Diese Warnung gilt nicht mehr.");
    let new_html = format!(
        "{original_html}<hr><em>✅ Entwarnung ({time}): Diese Warnung gilt nicht mehr.</em>"
    );
    (new_plain, new_html)
}

/// Append a level-change notice (escalation or de-escalation) to the original warning.
fn append_level_change_to_warning(
    original_plain: &str,
    original_html: &str,
    old_level: i64,
    new_level: i64,
    event: &str,
) -> (String, String) {
    let time = chrono::Local::now().format("%H:%M").to_string();
    let emoji = dwd_level_emoji(new_level);
    let label = dwd_level_label_for_event(event, new_level);
    let (arrow, verb) = if new_level > old_level {
        ("⬆️", "erhöht")
    } else {
        ("⬇️", "reduziert")
    };
    let new_plain =
        format!("{original_plain}\n\n---\n{arrow} Warnstufe {verb} auf {emoji} {label} ({time}).");
    let new_html = format!(
        "{original_html}<hr><em>{arrow} Warnstufe {verb} auf {emoji} {label} ({time}).</em>"
    );
    (new_plain, new_html)
}

/// Standalone all-clear fallback for when event IDs were not stored.
fn format_dwd_allclear(headline: &str, region: &str) -> (String, String) {
    let body = format!("📍 {region}\nDie Warnung ist nicht mehr aktiv.");
    let headline_html = alert_html_escape(headline);
    let body_html = alert_html_escape(&body).replace('\n', "<br>");
    let plain = format!("✅ Entwarnung: {headline}\n{body}\nSource: DWD Weather Warnings");
    let html = format!(
        "<b>✅ Entwarnung: {headline_html}</b><br>{body_html}<br><em>Source: DWD Weather Warnings</em>"
    );
    (plain, html)
}

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

fn format_alert(
    source: &str,
    event: &str,
    description: &str,
    link: Option<&str>,
) -> (String, String) {
    let link_plain = link.map(|l| format!("\n🔗 {l}")).unwrap_or_default();
    let link_html = link
        .map(|l| {
            let escaped = alert_html_escape(l);
            format!("<br>🔗 <a href=\"{escaped}\">{escaped}</a>")
        })
        .unwrap_or_default();
    let event_html = alert_html_escape(event);
    let description_html = alert_html_escape(description).replace('\n', "<br>");
    let source_html = alert_html_escape(source);

    let plain = format!("🚨 {event}\n{description}{link_plain}\nSource: {source}");
    let html = format!(
        "<b>🚨 {event_html}</b><br>{description_html}{link_html}<br><em>Source: {source_html}</em>"
    );
    (plain, html)
}

fn alert_html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dwd_description_is_structured_and_concise() {
        let warning = json!({
            "start": 1781946000000i64,
            "end": 1781974800000i64,
            "event": "EXTREME HITZE",
            "level": 51,
            "description": "\nAm Samstag wird eine extreme Wärmebelastung erwartet.\nMit einer zusätzlichen Belastung aufgrund verringerter nächtlicher Abkühlung ist insbesondere im dicht bebauten Stadtgebiet von Berlin zu rechnen.\nNoch ein Satz, der nicht in die kurze Nachricht soll.\n",
            "instruction": "Hitzebelastung kann für den menschlichen Körper gefährlich werden. Vermeiden Sie nach Möglichkeit die Hitze, trinken Sie ausreichend Wasser und halten Sie die Innenräume kühl."
        });

        let desc = format_dwd_description_with_continuations("Berlin", &warning, &[]);

        assert!(desc.contains("📍 Berlin"));
        assert!(desc.contains("🕒"));
        assert!(desc.contains("ℹ️ Am Samstag wird eine extreme Wärmebelastung erwartet."));
        assert!(
            desc.contains("✅ Hitzebelastung kann für den menschlichen Körper gefährlich werden.")
        );
        assert!(!desc.contains("Noch ein Satz"));
    }

    #[test]
    fn dwd_heat_levels_are_not_labeled_as_unwetter() {
        assert_eq!(
            dwd_level_label_for_event("STARKE HITZE", 50),
            "Starke Hitzewarnung"
        );
        assert_eq!(
            dwd_level_label_for_event("EXTREME HITZE", 51),
            "Extreme Hitzewarnung"
        );

        let warning = json!({
            "event": "STARKE HITZE",
            "level": 50,
            "start": 1781974800000i64,
            "end": 1782061200000i64,
            "description": "Am Samstag wird eine starke Wärmebelastung erwartet.",
            "instruction": "Hitzebelastung kann gefährlich werden."
        });
        let desc = format_dwd_description_with_continuations("Berlin", &warning, &[]);

        assert!(desc.contains("🟠 Starke Hitzewarnung"));
        assert!(!desc.contains("Unwetter"));
    }

    #[test]
    fn dwd_heat_events_share_lifecycle_key() {
        assert_eq!(dwd_event_key("EXTREME HITZE"), "HITZE");
        assert_eq!(dwd_event_key("STARKE HITZE"), "HITZE");

        let extreme = json!({"event": "EXTREME HITZE", "level": 51});
        let strong = json!({"event": "STARKE HITZE", "level": 50});

        assert_eq!(
            dwd_warning_stable_key("warnings", "Berlin", &extreme),
            "dwd:warnings:Berlin:HITZE:51"
        );
        assert_eq!(
            dwd_warning_stable_key("warnings", "Berlin", &strong),
            "dwd:warnings:Berlin:HITZE:50"
        );
    }

    #[test]
    fn dwd_related_weather_events_share_lifecycle_families() {
        assert_eq!(dwd_event_key("STURMBÖEN"), "WIND");
        assert_eq!(dwd_event_key("SCHWERE STURMBÖEN"), "WIND");
        assert_eq!(dwd_event_key("ORKANBÖEN"), "WIND");
        assert_eq!(dwd_event_key("STARKREGEN"), "REGEN");
        assert_eq!(dwd_event_key("DAUERREGEN"), "REGEN");
        assert_eq!(dwd_event_key("SCHNEEFALL"), "SCHNEE_GLAETTE");
        assert_eq!(dwd_event_key("GLATTEIS"), "SCHNEE_GLAETTE");
        assert_eq!(dwd_event_key("FROST"), "FROST");
        assert_eq!(dwd_event_key("NEBEL"), "NEBEL");
        assert_eq!(dwd_event_key("TAUWETTER"), "TAUWETTER");
        assert_eq!(dwd_event_key("UV-STRAHLUNG"), "UV");
        assert_eq!(dwd_event_key("WALDBRANDGEFAHR"), "FIRE_RISK");
        assert_eq!(dwd_event_key("UNBEKANNTE WARNART"), "UNBEKANNTE WARNART");
    }

    #[test]
    fn dwd_edit_error_classifies_missing_original_events() {
        assert!(looks_like_missing_matrix_event(
            "M_NOT_FOUND: event not found"
        ));
        assert!(looks_like_missing_matrix_event("unknown event id"));
        assert!(!looks_like_missing_matrix_event(
            "temporary network timeout"
        ));
    }

    #[test]
    fn dwd_secondary_note_distinguishes_overlap_and_followup() {
        let primary = json!({
            "event": "EXTREME HITZE",
            "level": 51,
            "start": 1781946000000i64,
            "end": 1781974800000i64
        });
        let before = json!({
            "event": "STARKE HITZE",
            "level": 50,
            "start": 1781938800000i64,
            "end": 1781946000000i64
        });
        let overlapping = json!({
            "event": "STARKE HITZE",
            "level": 50,
            "start": 1781953200000i64,
            "end": 1782061200000i64
        });
        let followup = json!({
            "event": "STARKE HITZE",
            "level": 50,
            "start": 1781974800000i64,
            "end": 1782061200000i64
        });

        assert!(format_dwd_secondary_warning(&primary, &before).contains("(davor:"));
        assert!(format_dwd_secondary_warning(&primary, &overlapping).contains("(gleichzeitig:"));
        assert!(format_dwd_secondary_warning(&primary, &followup).contains("(danach:"));
    }

    #[test]
    fn dwd_polygon_geometry_contains_point() {
        let geometry = json!({
            "type": "Polygon",
            "coordinates": [[
                [13.0, 52.0],
                [14.0, 52.0],
                [14.0, 53.0],
                [13.0, 53.0],
                [13.0, 52.0]
            ]]
        });

        assert!(geometry_contains_point(&geometry, (52.5, 13.5)));
        assert!(!geometry_contains_point(&geometry, (51.5, 13.5)));
    }

    #[test]
    fn dwd_polygon_hole_excludes_point() {
        let geometry = json!({
            "type": "Polygon",
            "coordinates": [
                [
                    [13.0, 52.0],
                    [14.0, 52.0],
                    [14.0, 53.0],
                    [13.0, 53.0],
                    [13.0, 52.0]
                ],
                [
                    [13.4, 52.4],
                    [13.6, 52.4],
                    [13.6, 52.6],
                    [13.4, 52.6],
                    [13.4, 52.4]
                ]
            ]
        });

        assert!(!geometry_contains_point(&geometry, (52.5, 13.5)));
        assert!(geometry_contains_point(&geometry, (52.2, 13.2)));
    }

    #[test]
    fn alert_html_preserves_lines_and_adds_link() {
        let (plain, html) = format_alert(
            "DWD Weather Warnings",
            "Amtliche WARNUNG vor HITZE",
            "📍 Berlin\n✅ Drink water",
            Some("https://www.dwd.de/"),
        );

        assert!(plain.contains("🚨 Amtliche WARNUNG vor HITZE"));
        assert!(plain.contains("📍 Berlin\n✅ Drink water\n🔗 https://www.dwd.de/"));
        assert!(html.contains("📍 Berlin<br>✅ Drink water"));
        assert!(html.contains("🔗 <a href=\"https://www.dwd.de/\">https://www.dwd.de/</a>"));
        assert!(!html.contains("DWD level"));
    }

    #[test]
    fn dwd_allclear_edit_appends_below_hr() {
        let orig_plain = "🚨 Amtliche WARNUNG vor HITZE\n📍 Berlin\nSource: DWD Weather Warnings";
        let orig_html =
            "<b>🚨 Amtliche WARNUNG vor HITZE</b><br>📍 Berlin<br><em>Source: DWD Weather Warnings</em>";

        let (plain, html) = append_allclear_to_warning(orig_plain, orig_html);

        // Original content preserved
        assert!(plain.starts_with(orig_plain));
        assert!(html.starts_with(orig_html));
        // Separator present
        assert!(plain.contains("\n\n---\n"));
        assert!(html.contains("<hr>"));
        // All-clear marker present
        assert!(plain.contains("✅ Entwarnung"));
        assert!(html.contains("✅ Entwarnung"));
        assert!(plain.contains("gilt nicht mehr"));
    }

    #[test]
    fn dwd_allclear_references_headline_and_region() {
        let (plain, html) = format_dwd_allclear("Amtliche WARNUNG vor HITZE", "Berlin, Stadt");

        assert!(plain.contains("✅ Entwarnung"));
        assert!(plain.contains("Amtliche WARNUNG vor HITZE"));
        assert!(plain.contains("Berlin, Stadt"));
        assert!(plain.contains("nicht mehr aktiv"));
        assert!(plain.contains("DWD Weather Warnings"));

        assert!(html.contains("✅ Entwarnung"));
        assert!(html.contains("Amtliche WARNUNG vor HITZE"));
        assert!(html.contains("Berlin, Stadt"));
        assert!(html.contains("nicht mehr aktiv"));
    }

    #[test]
    fn dwd_allclear_escapes_html() {
        let (_, html) = format_dwd_allclear("WARNUNG <test>", "Region & Co.");

        assert!(!html.contains("<test>"));
        assert!(html.contains("&lt;test&gt;"));
        assert!(html.contains("Region &amp; Co."));
    }

    #[test]
    fn nina_key_changes_when_warning_is_updated() {
        let first = json!({
            "sent": "2026-06-20T10:00:00+02:00",
            "effective": "2026-06-20T11:00:00+02:00",
            "expires": "2026-06-20T19:00:00+02:00",
            "severity": "Severe"
        });
        let updated = json!({
            "sent": "2026-06-20T12:00:00+02:00",
            "effective": "2026-06-20T11:00:00+02:00",
            "expires": "2026-06-20T20:00:00+02:00",
            "severity": "Severe"
        });

        assert_ne!(
            nina_alert_key("warn-1", &first),
            nina_alert_key("warn-1", &updated)
        );
    }
}
