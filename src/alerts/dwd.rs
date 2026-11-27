//! DWD weather warnings: one message per event family, edited on time and level changes,
//! all-clear as an edit, the warning cells' geometry.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{Context, Result};
use mxbot_common::Bot;
use tracing::{info, warn};

use super::post::{
    alert_html_escape, append_allclear_to_warning, concise_sentences, edit_alert_message_in_rooms,
    format_alert, post_alert_to_rooms, DwdEditResult,
};
use crate::store::{Db, PostKey};

#[allow(clippy::too_many_arguments)]
pub(super) async fn check_dwd_weather_warnings(
    client: &Bot,
    http: &reqwest::Client,
    db: &Db,
    region_keywords: &[String],
    lead: Duration,
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
    let active_map: HashMap<String, crate::store::DwdActiveWarning> = db
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
                let lead_ms = i64::try_from(lead.as_millis()).unwrap_or(i64::MAX);
                if !dwd_warning_is_relevant_now(bucket, start_ms, end_ms, now_ms, lead_ms) {
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
                let mut warning_value = w.clone();
                warning_value["_radar_bucket"] = serde_json::Value::String(bucket.to_string());
                valid.push(ValidWarning {
                    bucket: bucket.to_string(),
                    region: region.to_string(),
                    event_key,
                    headline,
                    start_ms,
                    end_ms,
                    level,
                    w: warning_value,
                });
            }
        }
    }

    // Phase 2: group by (region, event family) so active warnings and
    // Vorabinformation for the same event are shown as one lifecycle message.
    // This prevents "active thunderstorm now" + "pre-warning later today" from
    // becoming two separate posts for the same weather situation.
    //
    // Previous versions grouped by (bucket, region, event family). That kept
    // `warnings` and `vorabInformation` separate, which was too noisy during
    // thunderstorm situations.
    //
    // For warnings in the same event family:
    //   - active `warnings` bucket entries are preferred as primary
    //   - otherwise active entries are preferred over future Vorabinformation
    //   - then highest level wins
    //
    // Simultaneous warnings for
    // the same event (e.g. Level 4 HITZE 11-19h + Level 2 HITZE 19h-next) are
    // shown as one message with a continuation note rather than two messages.
    // Within each group: highest level = primary; the rest = continuations.
    let mut groups: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (i, vw) in valid.iter().enumerate() {
        groups
            .entry((vw.region.clone(), vw.event_key.clone()))
            .or_default()
            .push(i);
    }

    let mut current_active: HashSet<String> = HashSet::new();
    let mut active_level_by_event: HashMap<(String, String), i64> = HashMap::new();
    // Keys from active_map that were promoted (level change) this poll.
    // The all-clear loop skips these so it doesn't fire a spurious all-clear
    // for the old-level row that was just replaced.
    let mut promoted_keys: HashSet<String> = HashSet::new();

    for ((region, event_key), indices) in &groups {
        let mut group: Vec<&ValidWarning> = indices.iter().map(|&i| &valid[i]).collect();
        group.sort_by(|a, b| {
            dwd_primary_rank(&b.bucket, b.start_ms, b.end_ms, b.level, now_ms)
                .cmp(&dwd_primary_rank(
                    &a.bucket, a.start_ms, a.end_ms, a.level, now_ms,
                ))
                .then(a.start_ms.cmp(&b.start_ms))
        });

        let primary = group[0];
        let continuations = &group[1..];

        let primary_id = dwd_warning_stable_key(&primary.bucket, region, &primary.w);

        // Register all warnings in this group for tracking.
        for vw in &group {
            let id = dwd_warning_stable_key(&vw.bucket, region, &vw.w);
            current_active.insert(id);
            active_level_by_event
                .entry((region.clone(), event_key.clone()))
                .and_modify(|e| *e = (*e).max(vw.level))
                .or_insert(vw.level);
        }

        // Build secondary notes sorted by start time. The primary remains the worst
        // active warning; lower overlapping/follow-up warnings are shown underneath.
        let mut secondary: Vec<&ValidWarning> = continuations.to_vec();
        secondary.sort_by(|a, b| a.start_ms.cmp(&b.start_ms).then(b.level.cmp(&a.level)));
        let cont_notes: Vec<String> = secondary
            .iter()
            .filter_map(|vw| format_dwd_secondary_warning(&primary.w, &vw.w))
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
                        let edit_result =
                            edit_alert_message_in_rooms(client, ids_json, &p, &h).await;
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
                        active.event_ids_json.as_deref().unwrap_or_default(),
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
                    match edit_alert_message_in_rooms(client, ids_json, &new_plain, &new_html).await
                    {
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
                                ids_json,
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
                .map(|(_kb, kr, ke, _ol)| {
                    kr == region.as_str()
                        && dwd_event_key(ke) == event_key.as_str()
                        && k.as_str() != primary_id.as_str()
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
                    let (edit_plain, edit_html) = if old_level == primary.level {
                        (new_plain.clone(), new_html.clone())
                    } else {
                        append_level_change_to_warning(
                            &new_plain,
                            &new_html,
                            old_level,
                            primary.level,
                            primary
                                .w
                                .get("event")
                                .and_then(|v| v.as_str())
                                .unwrap_or(event_key),
                        )
                    };
                    match edit_alert_message_in_rooms(client, ids_json, &edit_plain, &edit_html)
                        .await
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
                            if old_level == primary.level {
                                info!(
                                    "ALERT replaced [dwd]: {} ({region}) same level L{}",
                                    primary.headline, primary.level
                                );
                            } else {
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
                        }
                        DwdEditResult::MissingOriginal => {
                            if repost_dwd_active_warning(
                                client,
                                db,
                                &primary_id,
                                ids_json,
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
                    // No event IDs (pre-upgrade row): can't edit. Post fresh and
                    // clean up the stale row so no spurious all-clear fires.
                    let first =
                        dwd_occurrence_key(db, &primary_id, primary.start_ms, primary.end_ms, None)
                            .await?;
                    let (all_ok, room_event_ids) =
                        post_alert_to_rooms(client, &first.key, &new_plain, &new_html).await;
                    if all_ok {
                        let ids_json = serde_json::to_string(&room_event_ids)
                            .unwrap_or_else(|_| String::from("{}"));
                        // The window of the first attempt: if it moved since,
                        // the next poll edits every room's message to the new one.
                        db.promote_dwd_active_warning(
                            old_id,
                            &primary_id,
                            &primary.headline,
                            region,
                            event_key,
                            &ids_json,
                            first.start_ms,
                            first.end_ms,
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
            // Genuinely new warning. No prior message for this event type.
            let desc = format_dwd_description_with_continuations(region, &primary.w, &cont_notes);
            let (plain, html) = format_alert(
                "DWD Weather Warnings",
                &primary.headline,
                &desc,
                Some("https://www.dwd.de/DE/wetter/warnungen_gemeinden/warnWetter_node.html"),
            );
            let first =
                dwd_occurrence_key(db, &primary_id, primary.start_ms, primary.end_ms, None).await?;
            let (all_ok, room_event_ids) =
                post_alert_to_rooms(client, &first.key, &plain, &html).await;
            if all_ok {
                let ids_json =
                    serde_json::to_string(&room_event_ids).unwrap_or_else(|_| String::from("{}"));
                // The window of the first attempt: if it moved since, the
                // next poll edits every room's message to the new one.
                db.mark_dwd_alert_posted(
                    &primary_id,
                    &primary.headline,
                    region,
                    event_key,
                    &ids_json,
                    first.start_ms,
                    first.end_ms,
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

    // Warnings that left the feed before every room had them: if they come
    // back, that is a new occurrence with a key of its own.
    db.forget_post_keys_except(DWD_POST_KEYS, &current_active)
        .await?;

    // Genuine all-clears: warnings that disappeared from DWD with no replacement
    // at any level. Level changes are handled above in Phase 2 (promoted_keys).
    for (id, prev) in &active_map {
        if current_active.contains(id) || promoted_keys.contains(id) {
            continue;
        }
        // Also skip if a level-change is in progress but the edit failed last poll:
        // Phase 2 will retry it; firing an all-clear here would be wrong.
        if let Some((b, r, e, _)) = parse_stable_key(id) {
            let _ = b;
            if active_level_by_event.contains_key(&(r.to_owned(), dwd_event_key(e))) {
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
                edit_alert_message_in_rooms(client, ids_json, &new_plain, &new_html).await
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

pub(super) fn parse_dwd_jsonp(body: &str) -> Option<serde_json::Value> {
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

pub(super) fn matches_region(region: &str, keywords: &[String]) -> bool {
    let region = region.to_lowercase();
    keywords.iter().any(|kw| {
        let kw = kw.trim().to_lowercase();
        !kw.is_empty() && region.contains(&kw)
    })
}

/// Active warnings, warnings starting within `lead_ms`, and every
/// Vorabinformation that has not ended yet.
pub(super) fn dwd_warning_is_relevant_now(
    bucket: &str,
    start_ms: i64,
    end_ms: i64,
    now_ms: i64,
    lead_ms: i64,
) -> bool {
    if end_ms < now_ms {
        return false;
    }
    if bucket == "vorabInformation" {
        return true;
    }
    start_ms <= now_ms.saturating_add(lead_ms)
}

pub(super) fn dwd_primary_rank(
    bucket: &str,
    start_ms: i64,
    end_ms: i64,
    level: i64,
    now_ms: i64,
) -> (i32, i32, i64) {
    let active = start_ms <= now_ms && end_ms >= now_ms;
    let active_warning = active && bucket == "warnings";
    (active_warning as i32, active as i32, level)
}

pub(super) fn dwd_warning_stable_key(bucket: &str, region: &str, w: &serde_json::Value) -> String {
    let event = w["event"].as_str().unwrap_or("warning");
    let event_key = dwd_event_key(event);
    let level = w["level"].as_i64().unwrap_or(0);
    // Excludes start/end: DWD refreshes `start` to the request time on every poll
    // and `end` may be extended as the forecast evolves. Including them would treat
    // each refresh as a new warning and cause duplicate posts.
    format!("dwd:{bucket}:{region}:{event_key}:{level}")
}

pub(super) fn dwd_event_key(event: &str) -> String {
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

pub(super) fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

pub(super) async fn dwd_warning_area_contains_point(
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
            // Not cached: a network hiccup must not decide this cell for the
            // rest of the process lifetime. The next poll asks again.
            warn!(
                "DWD warning-area geometry lookup failed for cell {warncell_id}: {e:#}; falling back to region-name match"
            );
            Ok(true)
        }
    }
}

pub(super) async fn fetch_dwd_warning_area(
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

pub(super) fn geometry_contains_point(geometry: &serde_json::Value, point: (f64, f64)) -> bool {
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

pub(super) fn polygon_contains_point(coordinates: &serde_json::Value, point: (f64, f64)) -> bool {
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

pub(super) fn ring_contains_point(ring: &serde_json::Value, point: (f64, f64)) -> bool {
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

pub(super) fn geojson_position(position: Option<&Vec<serde_json::Value>>) -> Option<(f64, f64)> {
    let position = position?;
    let lon = position.first()?.as_f64()?;
    let lat = position.get(1)?.as_f64()?;
    Some((lon, lat))
}

pub(super) fn format_dwd_description_with_continuations(
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
            dwd_level_label_for_warning(w)
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
        parts.push(format!("💡 {advice}"));
    }
    for note in continuations {
        parts.push(note.clone());
    }
    parts.join("\n")
}

/// One-line note for a secondary warning in the same event family.
/// Lower-level warnings fully covered by the primary are omitted. If a lower
/// warning overlaps but continues after the primary, only the remaining
/// follow-up part is shown as `danach`.
pub(super) fn format_dwd_secondary_warning(
    primary: &serde_json::Value,
    w: &serde_json::Value,
) -> Option<String> {
    let level = w["level"].as_i64().unwrap_or(0);
    let emoji = dwd_level_emoji(level);
    let label = dwd_secondary_label_for_warning(w);
    let primary_start = primary["start"].as_i64().unwrap_or(0);
    let primary_end = primary["end"].as_i64().unwrap_or(0);
    let secondary_start = w["start"].as_i64().unwrap_or(0);
    let secondary_end = w["end"].as_i64().unwrap_or(0);

    let prefix = if secondary_start < primary_end && secondary_end <= primary_end {
        return None;
    } else if secondary_start < primary_end && secondary_end > primary_end {
        "danach"
    } else if secondary_end <= primary_start {
        return None;
    } else if secondary_start < primary_end {
        "gleichzeitig"
    } else {
        "danach"
    };
    let end_str = w["end"]
        .as_i64()
        .and_then(chrono::DateTime::from_timestamp_millis)
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
        Some(t) => Some(format!("({prefix}: {emoji} {label} bis {t})")),
        None => Some(format!("({prefix}: {emoji} {label})")),
    }
}

pub(super) fn dwd_level_emoji(level: i64) -> &'static str {
    match level {
        1 => "🟡",
        2 => "⚠️",
        3 => "🟠",
        50 => "🟠",
        51 => "🔴",
        _ => "🔴", // 4, 5, or anything higher
    }
}

pub(super) fn dwd_level_label(level: i64) -> &'static str {
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

pub(super) fn dwd_level_label_for_event(event: &str, level: i64) -> &'static str {
    match dwd_event_key(event).as_str() {
        "HITZE" => match level {
            50 => "Starke Hitzewarnung",
            51 => "Extreme Hitzewarnung",
            _ => dwd_level_label(level),
        },
        _ => dwd_level_label(level),
    }
}

pub(super) fn dwd_level_label_for_warning(w: &serde_json::Value) -> &'static str {
    if w["_radar_bucket"].as_str() == Some("vorabInformation") {
        return match w["level"].as_i64().unwrap_or(0) {
            4 | 5 => "Vorabinformation Unwetter",
            _ => "Vorabinformation",
        };
    }
    dwd_level_label_for_event(
        w["event"].as_str().unwrap_or(""),
        w["level"].as_i64().unwrap_or(0),
    )
}

pub(super) fn dwd_secondary_label_for_warning(w: &serde_json::Value) -> &'static str {
    let event = w["event"].as_str().unwrap_or("");
    let level = w["level"].as_i64().unwrap_or(0);
    match dwd_event_key(event).as_str() {
        "GEWITTER" => match level {
            3 => "Starke Gewitterwarnung",
            4 => "Unwetterwarnung Gewitter",
            5 => "Extreme Gewitterwarnung",
            _ => dwd_level_label_for_warning(w),
        },
        "REGEN" => match level {
            3 => "Starkregenwarnung",
            4 => "Unwetterwarnung Starkregen",
            5 => "Extreme Starkregenwarnung",
            _ => dwd_level_label_for_warning(w),
        },
        _ => dwd_level_label_for_warning(w),
    }
}

/// Parse a stable key back into its components.
/// Format: `dwd:{bucket}:{region}:{event}:{level}`
pub(super) fn parse_stable_key(id: &str) -> Option<(&str, &str, &str, i64)> {
    let rest = id.strip_prefix("dwd:")?;
    let mut parts = rest.splitn(4, ':');
    let bucket = parts.next()?;
    let region = parts.next()?;
    let event = parts.next()?;
    let level: i64 = parts.next()?.parse().ok()?;
    Some((bucket, region, event, level))
}

pub(super) fn format_dwd_validity(w: &serde_json::Value) -> Option<String> {
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

/// The key of a DWD warning post's transaction IDs. `stable` alone names
/// the warning, and the same warning comes back (next week's storm at the
/// same level): the homeserver remembers transaction IDs for good and would
/// answer with the old message instead of posting the new one. So the key
/// also names this occurrence: by the warning's end at its first attempt
/// (DWD gives no ID of its own, and its start moves with every poll), and
/// for a repost by the messages it replaces. [`dwd_occurrence_key`] keeps
/// it for the retries of that post (some rooms failed), also when DWD moves
/// the end in between, so rooms that have it get no second copy.
pub(super) fn dwd_post_key(stable: &str, end_ms: i64, replaces: Option<&str>) -> String {
    match replaces {
        Some(replaced) => format!("{stable}|end={end_ms}|replaces={replaced}"),
        None => format!("{stable}|end={end_ms}"),
    }
}

/// The `source` of DWD rows in `alert_post_keys`.
pub(super) const DWD_POST_KEYS: &str = "dwd";

/// The key (and window) for posting the DWD warning `id` now: the one of its
/// first attempt while that post has not reached every room, else a new one
/// ([`dwd_post_key`]) from this window. Once stored as delivered, or gone
/// from the feed, the next post of `id` gets a new key.
pub(super) async fn dwd_occurrence_key(
    db: &Db,
    id: &str,
    start_ms: i64,
    end_ms: i64,
    replaces: Option<&str>,
) -> Result<PostKey> {
    let key = dwd_post_key(id, end_ms, replaces);
    db.post_key(id, DWD_POST_KEYS, &key, start_ms, end_ms).await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn repost_dwd_active_warning(
    client: &Bot,
    db: &Db,
    id: &str,
    replaces: &str,
    headline: &str,
    region: &str,
    event: &str,
    start_ms: i64,
    end_ms: i64,
    plain: &str,
    html: &str,
) -> Result<bool> {
    let first = dwd_occurrence_key(db, id, start_ms, end_ms, Some(replaces)).await?;
    let (all_ok, room_event_ids) = post_alert_to_rooms(client, &first.key, plain, html).await;
    if !all_ok {
        return Ok(false);
    }
    let ids_json = serde_json::to_string(&room_event_ids).unwrap_or_else(|_| String::from("{}"));
    // The window of the first attempt: if it moved since, the next poll
    // edits every room's message to the new one.
    db.mark_dwd_alert_posted(
        id,
        headline,
        region,
        event,
        &ids_json,
        first.start_ms,
        first.end_ms,
        plain,
        html,
    )
    .await?;
    Ok(true)
}

/// Append a level-change notice (escalation or de-escalation) to the original warning.
pub(super) fn append_level_change_to_warning(
    original_plain: &str,
    original_html: &str,
    old_level: i64,
    new_level: i64,
    event: &str,
) -> (String, String) {
    let time = chrono::Local::now().format("%H:%M").to_string();
    let emoji = dwd_level_emoji(new_level);
    let label = dwd_level_label_for_event(event, new_level);
    let event = event.trim();
    let event_note = if event.is_empty() {
        String::new()
    } else {
        format!("; jetzt: {event}")
    };
    let (arrow, verb) = if new_level > old_level {
        ("⬆️", "erhöht")
    } else {
        ("⬇️", "reduziert")
    };
    let new_plain = format!(
        "{original_plain}\n\n---\n{arrow} Warnung aktualisiert: Warnstufe {verb} auf {emoji} {label}{event_note} ({time})."
    );
    let event_note_html = alert_html_escape(&event_note);
    let new_html = format!(
        "{original_html}<br><br><hr><em>{arrow} Warnung aktualisiert: Warnstufe {verb} auf {emoji} {label}{event_note_html} ({time}).</em>"
    );
    (new_plain, new_html)
}

/// Standalone all-clear fallback for when event IDs were not stored.
pub(super) fn format_dwd_allclear(headline: &str, region: &str) -> (String, String) {
    let body = format!("📍 {region}\nDie Warnung ist nicht mehr aktiv.");
    let headline_html = alert_html_escape(headline);
    let body_html = alert_html_escape(&body).replace('\n', "<br>");
    let plain = format!("✅ Entwarnung: {headline}\n{body}\n📰 Source: DWD Weather Warnings");
    let html = format!(
        "<b>✅ Entwarnung: {headline_html}</b><br>{body_html}<br><em>📰 Source: DWD Weather Warnings</em>"
    );
    (plain, html)
}
