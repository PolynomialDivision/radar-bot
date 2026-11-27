//! NINA (warnung.bund.de): the region key, the dashboard poll, posting and updating warnings.

use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result};
use tracing::{info, warn};

use super::fetch_api;
use super::post::{
    append_allclear_to_warning, concise_sentences, edit_alert_message_in_rooms, format_alert,
    format_rfc3339_validity, post_alert_to_rooms, AlertRooms, DwdEditResult,
};
use crate::fetch::Http;
use crate::store::Db;

/// The NINA dashboard of the region `ags`: the warnings now in force, as a JSON array.
pub(super) async fn fetch_nina_dashboard(http: &Http, ags: &str) -> Result<serde_json::Value> {
    let url = format!("https://warnung.bund.de/api31/dashboard/{ags}.json");
    let body = fetch_api(http, &url, "NINA").await?.body;
    let json: serde_json::Value = serde_json::from_str(&body).context("NINA JSON parse failed")?;
    anyhow::ensure!(json.is_array(), "NINA response is not an array");
    Ok(json)
}

/// The NINA region key (AGS, Amtlicher Gemeindeschlüssel) of a German `city`: built in for a
/// city-state, else NINA's completion search. `Ok(None)` when NINA does not know the city;
/// `Err` when the search failed or its answer is not a list of results (it is tried again).
pub(crate) async fn nina_ags_for_city(
    http: &crate::fetch::Http,
    city: &str,
) -> std::result::Result<Option<String>, String> {
    if let Some(ags) = known_nina_ags(city) {
        info!("NINA AGS resolved from built-in mapping: {ags} ({city})");
        return Ok(Some(ags.to_owned()));
    }
    info!("Looking up NINA AGS for '{city}'");
    let url = format!(
        "https://warnung.bund.de/api31/completion/search?q={}",
        crate::url_encode(city)
    );
    let response = http
        .get_api(&url, &[], Duration::from_secs(15))
        .await
        .map_err(|e| format!("NINA region search: {e}"))?;
    if !response.is_success() {
        return Err(format!(
            "NINA region search answered HTTP {}",
            response.status
        ));
    }
    let results: serde_json::Value = serde_json::from_str(&response.body)
        .map_err(|e| format!("NINA region search answer: {e}"))?;
    if !results.is_array() {
        return Err("NINA region search answer is not a list".to_owned());
    }
    match results[0]["id"].as_str() {
        Some(ags) => {
            info!("NINA AGS resolved: {ags} ({city})");
            Ok(Some(ags.to_owned()))
        }
        None => {
            warn!("NINA does not know '{city}', NINA alerts disabled");
            Ok(None)
        }
    }
}

pub(super) fn known_nina_ags(city: &str) -> Option<&'static str> {
    match city.trim().to_lowercase().as_str() {
        // Berlin is a city-state; warnung.bund.de dashboard expects the 12-digit ARS.
        "berlin" => Some("110000000000"),
        _ => None,
    }
}

pub(super) async fn check_nina(
    client: &impl AlertRooms,
    http: &Http,
    db: &Db,
    ags: &str,
) -> Result<()> {
    let json = fetch_nina_dashboard(http, ags).await?;
    let warnings = json.as_array().context("NINA response is not an array")?;

    // The warnings in the feed (without their version), for the post keys.
    let mut current: HashSet<String> = HashSet::new();
    for w in warnings {
        let id = match w["id"].as_str() {
            Some(id) => id,
            None => continue,
        };
        if is_nina_dwd_weather_warning(id) {
            tracing::debug!("Skipping NINA DWD weather warning already handled via DWD: {id}");
            continue;
        }

        let data = &w["payload"]["data"];
        let headline = data["headline"].as_str().unwrap_or("Warnung");
        let active_key = nina_active_key(id);
        if data["msgType"].as_str() != Some("Cancel") {
            current.insert(active_key.clone());
        }
        let alert_key = nina_alert_key(id, data);
        if db.is_alert_seen(&alert_key).await? {
            continue;
        }

        if data["msgType"].as_str() == Some("Cancel") {
            match db.get_active_nina_warning(&active_key).await? {
                Some(active) => {
                    let (plain, html) =
                        append_allclear_to_warning(&active.original_plain, &active.original_html);
                    match edit_alert_message_in_rooms(client, &active.event_ids_json, &plain, &html)
                        .await
                    {
                        DwdEditResult::Ok => {
                            db.mark_nina_allclear_sent(&active_key).await?;
                            db.mark_alert_seen(&alert_key, "nina").await?;
                            info!("ALL-CLEAR sent [nina]: {}", active.headline);
                        }
                        DwdEditResult::MissingOriginal => {
                            db.mark_nina_allclear_sent(&active_key).await?;
                            db.mark_alert_seen(&alert_key, "nina").await?;
                            info!(
                                "ALL-CLEAR skipped [nina], original message is gone: {}",
                                active.headline
                            );
                        }
                        DwdEditResult::Failed => {
                            warn!(
                                "ALL-CLEAR send failed [nina], will retry next poll: {}",
                                active.headline
                            );
                        }
                    }
                }
                None => {
                    db.mark_alert_seen(&alert_key, "nina").await?;
                    info!("ALL-CLEAR skipped [nina], no active message found: {headline}");
                }
            }
            continue;
        }

        let desc = format_nina_description(data);
        let link = format!("https://warnung.bund.de/meldung/{id}");
        let (plain, html) = format_alert("NINA/warnung.bund.de", headline, &desc, Some(&link));

        let severity = data["severity"].as_str();
        if let Some(active) = db.get_active_nina_warning(&active_key).await? {
            // A rise in severity is a new message (an edit does not notify), posted before
            // the edit; when it does not reach every room, both wait for the next poll.
            if nina_severity_rose(active.severity.as_deref(), severity) {
                let (up_plain, up_html) = format_nina_escalation(data, headline, active.seen_at);
                let reply_to: std::collections::HashMap<String, String> =
                    serde_json::from_str(&active.event_ids_json).unwrap_or_default();
                let key = nina_escalation_key(&alert_key, severity.unwrap_or(""));
                if !client.post(&key, &up_plain, &up_html, &reply_to).await.0 {
                    warn!("ESCALATION post failed [nina], will retry next poll: {headline}");
                    continue;
                }
                info!("ESCALATION posted [nina]: {headline}");
            }
            match edit_alert_message_in_rooms(client, &active.event_ids_json, &plain, &html).await {
                DwdEditResult::Ok => {
                    db.mark_nina_alert_posted(
                        &active_key,
                        headline,
                        &nina_region(data),
                        &active.event_ids_json,
                        &plain,
                        &html,
                    )
                    .await?;
                    db.set_nina_severity(&active_key, severity).await?;
                    db.mark_alert_seen(&alert_key, "nina").await?;
                    info!("ALERT updated [nina]: {headline}");
                }
                DwdEditResult::MissingOriginal => {
                    match post_nina_warning_to_rooms(
                        client,
                        db,
                        &active_key,
                        &alert_key,
                        headline,
                        &nina_region(data),
                        &plain,
                        &html,
                    )
                    .await?
                    {
                        NinaPosted::ThisVersion => {
                            db.mark_alert_seen(&alert_key, "nina").await?;
                            info!("ALERT reposted [nina] after missing original: {headline}");
                        }
                        NinaPosted::EarlierVersion => {
                            info!("ALERT reposted [nina], updated next poll: {headline}");
                        }
                        NinaPosted::Failed => {
                            warn!("ALERT repost failed [nina], will retry next poll: {headline}");
                        }
                    }
                }
                DwdEditResult::Failed => {
                    warn!("ALERT update failed [nina], will retry next poll: {headline}");
                }
            }
        } else {
            match post_nina_warning_to_rooms(
                client,
                db,
                &active_key,
                &alert_key,
                headline,
                &nina_region(data),
                &plain,
                &html,
            )
            .await?
            {
                NinaPosted::ThisVersion => {
                    db.mark_alert_seen(&alert_key, "nina").await?;
                    info!("ALERT sent [nina]: {headline}");
                }
                NinaPosted::EarlierVersion => {
                    info!(
                        "ALERT sent [nina], earlier version's rooms updated next poll: {headline}"
                    );
                }
                NinaPosted::Failed => {
                    warn!("ALERT post failed [nina], will retry next poll: {headline}");
                }
            }
        }
    }

    // Warnings that left the feed before every room had them.
    db.forget_post_keys_except(NINA_POST_KEYS, &current).await?;
    Ok(())
}

pub(super) fn is_nina_dwd_weather_warning(id: &str) -> bool {
    id.starts_with("dwd.") || id.contains(".DWD.")
}

pub(super) fn nina_active_key(id: &str) -> String {
    let Some((base, suffix)) = id.rsplit_once('-') else {
        return id.to_owned();
    };
    if suffix.len() == 3 && suffix.chars().all(|c| c.is_ascii_digit()) {
        base.to_owned()
    } else {
        id.to_owned()
    }
}

pub(super) fn nina_region(data: &serde_json::Value) -> String {
    data["area"]
        .as_array()
        .and_then(|areas| areas.first())
        .and_then(|area| area["areaDesc"].as_str())
        .unwrap_or("Region unbekannt")
        .to_owned()
}

pub(super) fn nina_alert_key(id: &str, data: &serde_json::Value) -> String {
    let sent = data["sent"].as_str().unwrap_or("");
    let effective = data["effective"].as_str().unwrap_or("");
    let expires = data["expires"].as_str().unwrap_or("");
    let severity = data["severity"].as_str().unwrap_or("");
    format!("nina:{id}:{sent}:{effective}:{expires}:{severity}")
}

pub(super) fn format_nina_description(data: &serde_json::Value) -> String {
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
        lines.push(format!("💡 {advice}"));
    }

    lines.join("\n")
}

/// The `source` of NINA rows in `alert_post_keys`.
pub(super) const NINA_POST_KEYS: &str = "nina";

/// What [`post_nina_warning_to_rooms`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NinaPosted {
    /// Every room has this version.
    ThisVersion,
    /// Every room has the warning, but those that got an earlier version's
    /// first attempt still show that one: the version is not marked seen,
    /// so the next poll edits the messages to it.
    EarlierVersion,
    /// Some room failed: tried again next poll.
    Failed,
}

/// `alert_key` names this version of the warning (its ID with the version
/// number, its send time): see [`dwd_post_key`] for why `id` alone is not
/// enough. While an earlier version's post has not reached every room, its
/// key is used ([`Db::post_key`]), so rooms that have it get no second copy.
#[allow(clippy::too_many_arguments)]
pub(super) async fn post_nina_warning_to_rooms(
    client: &impl AlertRooms,
    db: &Db,
    id: &str,
    alert_key: &str,
    headline: &str,
    region: &str,
    plain: &str,
    html: &str,
) -> Result<NinaPosted> {
    let first = db.post_key(id, NINA_POST_KEYS, alert_key, 0, 0).await?;
    let (all_ok, room_event_ids) = post_alert_to_rooms(client, &first.key, plain, html).await;
    if !all_ok {
        return Ok(NinaPosted::Failed);
    }
    let ids_json = serde_json::to_string(&room_event_ids).unwrap_or_else(|_| String::from("{}"));
    db.mark_nina_alert_posted(id, headline, region, &ids_json, plain, html)
        .await?;
    // `alert_key` ends in the severity of this version.
    db.set_nina_severity(id, alert_key.rsplit(':').next().filter(|s| !s.is_empty()))
        .await?;
    Ok(if first.key == alert_key {
        NinaPosted::ThisVersion
    } else {
        NinaPosted::EarlierVersion
    })
}

/// The rank of a CAP severity; unknown values rank lowest.
fn nina_severity_rank(severity: &str) -> u8 {
    match severity.trim().to_ascii_lowercase().as_str() {
        "minor" => 1,
        "moderate" => 2,
        "severe" => 3,
        "extreme" => 4,
        _ => 0,
    }
}

/// Whether an update raises the severity of the posted version. A posted version of unknown
/// severity (a row from before it was stored) has not "risen".
pub(super) fn nina_severity_rose(posted: Option<&str>, update: Option<&str>) -> bool {
    match (posted, update) {
        (Some(posted), Some(update)) => nina_severity_rank(update) > nina_severity_rank(posted),
        _ => false,
    }
}

/// The transaction key of the escalation post of the version `alert_key` names.
pub(super) fn nina_escalation_key(alert_key: &str, severity: &str) -> String {
    format!("{alert_key}:up:{severity}")
}

fn nina_severity_label(severity: &str) -> &'static str {
    match nina_severity_rank(severity) {
        4 => "Extreme Gefahr",
        3 => "Schwere Gefahr",
        2 => "Mittlere Gefahr",
        _ => "Warnung",
    }
}

/// The short message for a NINA warning whose severity rose; `original_at` is when the
/// original was posted (seconds).
pub(super) fn format_nina_escalation(
    data: &serde_json::Value,
    headline: &str,
    original_at: i64,
) -> (String, String) {
    let severity = data["severity"].as_str().unwrap_or("");
    let emoji = if nina_severity_rank(severity) >= 4 {
        "🔴"
    } else {
        "🟠"
    };
    let validity = format_rfc3339_validity(data["effective"].as_str(), data["expires"].as_str())
        .unwrap_or_default();
    let original = chrono::DateTime::from_timestamp(original_at, 0)
        .map(|t| t.with_timezone(&chrono::Local).format("%H:%M").to_string())
        .unwrap_or_default();
    let lines = [
        format!(
            "⬆️ {emoji} {}: {}",
            nina_severity_label(severity),
            headline.trim()
        ),
        format!("🕒 {validity}"),
        format!("Die Warnung von {original} wurde verschärft."),
    ];
    let plain = lines.join("\n");
    let html = lines
        .iter()
        .map(|line| super::post::alert_html_escape(line))
        .collect::<Vec<_>>()
        .join("<br>");
    (plain, html)
}
