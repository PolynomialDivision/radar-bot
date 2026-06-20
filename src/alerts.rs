use std::{
    collections::{HashMap, HashSet},
    time::Duration,
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

    loop {
        if let Some(ref ags) = config.nina_ags {
            if let Err(e) = check_nina(&client, &http, &db, ags).await {
                warn!("NINA check failed: {e:#}");
            }
        }
        if !config.dwd_region_keywords.is_empty() {
            if let Err(e) = check_dwd_weather_warnings(
                &client,
                &http,
                &db,
                &config.dwd_region_keywords,
                config.ref_point,
                &mut dwd_area_cache,
            )
            .await
            {
                warn!("DWD weather-warning check failed: {e:#}");
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

    // Collect all warning IDs that are currently valid (pass region, time, geometry filters).
    // We upsert every passing warning into dwd_active_warnings so that on the next poll
    // we can detect which ones have disappeared.
    let mut current_active: HashSet<String> = HashSet::new();

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

                let start = w["start"].as_i64().unwrap_or(0);
                let end = w["end"].as_i64().unwrap_or(i64::MAX);
                if start > now_ms || end < now_ms {
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
                    .unwrap_or("Wetterwarnung");
                let headline = w["headline"].as_str().unwrap_or(event);
                let id = dwd_warning_id(bucket, region, w);

                // Heartbeat upsert: mark as currently active on every poll.
                // Event IDs / original content are left intact via COALESCE.
                current_active.insert(id.clone());
                db.upsert_dwd_active_warning(&id, headline, region, event, None, None, None).await?;

                if db.is_alert_seen(&id).await? {
                    continue;
                }

                let desc = format_dwd_description(region, w);
                let (plain, html) = format_alert(
                    "DWD Weather Warnings",
                    headline,
                    &desc,
                    Some("https://www.dwd.de/DE/wetter/warnungen_gemeinden/warnWetter_node.html"),
                );
                let (all_ok, room_event_ids) =
                    post_dwd_warning_to_rooms(client, &plain, &html).await;
                if all_ok {
                    db.mark_alert_seen(&id, "dwd").await?;
                    // Store event IDs and original content so the all-clear can edit
                    // the original messages rather than posting a new one.
                    let ids_json = serde_json::to_string(&room_event_ids)
                        .unwrap_or_else(|_| String::from("{}"));
                    db.upsert_dwd_active_warning(
                        &id,
                        headline,
                        region,
                        event,
                        Some(&ids_json),
                        Some(&plain),
                        Some(&html),
                    )
                    .await?;
                    info!("ALERT sent [dwd]: {headline} ({region})");
                } else {
                    warn!("ALERT post failed [dwd], will retry next poll: {headline} ({region})");
                }
            }
        }
    }

    // Resolve warnings no longer in the DWD feed or no longer valid.
    // Prefer editing the original message so no extra timeline entry appears.
    // Falls back to a standalone message when event IDs were not stored.
    let previously_active = db.list_active_dwd_warnings().await?;
    for prev in previously_active {
        if current_active.contains(&prev.id) {
            continue;
        }
        let clear_key = format!("dwd_clear:{}", prev.id);
        if !db.is_alert_seen(&clear_key).await? {
            let sent = match (
                prev.event_ids_json.as_deref(),
                prev.original_plain.as_deref(),
                prev.original_html.as_deref(),
            ) {
                (Some(ids_json), Some(orig_plain), Some(orig_html)) => {
                    let (new_plain, new_html) =
                        append_allclear_to_warning(orig_plain, orig_html);
                    edit_dwd_warning_in_rooms(client, ids_json, &new_plain, &new_html).await
                }
                _ => {
                    // No event IDs stored — fall back to a standalone all-clear message.
                    let (plain, html) = format_dwd_allclear(&prev.headline, &prev.region);
                    crate::post_to_rooms(client, &plain, &html).await
                }
            };
            if sent {
                db.mark_alert_seen(&clear_key, "dwd_clear").await?;
                db.remove_dwd_active_warning(&prev.id).await?;
                info!("ALL-CLEAR sent [dwd]: {} ({})", prev.headline, prev.region);
            } else {
                warn!(
                    "ALL-CLEAR send failed [dwd], will retry next poll: {} ({})",
                    prev.headline, prev.region
                );
            }
        } else {
            // All-clear already sent in a previous run; just clean up the active row.
            db.remove_dwd_active_warning(&prev.id).await?;
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

fn dwd_warning_id(bucket: &str, region: &str, w: &serde_json::Value) -> String {
    let event = w["event"].as_str().unwrap_or("warning");
    let start = w["start"].as_i64().unwrap_or(0);
    let end = w["end"].as_i64().unwrap_or(0);
    let level = w["level"].as_i64().unwrap_or(0);
    format!("dwd:{bucket}:{region}:{event}:{start}:{end}:{level}")
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

fn format_dwd_description(region: &str, w: &serde_json::Value) -> String {
    let description = w["description"].as_str().unwrap_or("").trim();
    let instruction = w["instruction"].as_str().unwrap_or("").trim();

    let mut parts = vec![format!("📍 {region}")];
    if let Some(valid) = format_dwd_validity(w) {
        parts.push(format!("🕒 {valid}"));
    }
    if let Some(details) = concise_sentences(description, 2) {
        parts.push(format!("ℹ️ {details}"));
    }
    if let Some(advice) = concise_sentences(instruction, 1) {
        parts.push(format!("✅ {advice}"));
    }
    parts.join("\n")
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
        match room.send(RoomMessageEventContent::text_html(plain, html)).await {
            Ok(resp) => {
                event_ids.insert(room.room_id().to_string(), resp.response.event_id.to_string());
            }
            Err(e) => {
                error!("Failed to post DWD warning to {}: {e}", room.room_id());
                all_ok = false;
            }
        }
    }
    (all_ok, event_ids)
}

/// Edit the original DWD warning message in each room to append the all-clear notice.
/// `event_ids_json` is a JSON object mapping room_id strings to event_id strings.
/// Returns true if all edits succeeded.
async fn edit_dwd_warning_in_rooms(
    client: &Client,
    event_ids_json: &str,
    new_plain: &str,
    new_html: &str,
) -> bool {
    let map: HashMap<String, String> = match serde_json::from_str(event_ids_json) {
        Ok(m) => m,
        Err(e) => {
            warn!("Could not parse DWD event_ids_json for edit: {e}");
            return false;
        }
    };

    let mut all_ok = true;
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
        }
    }
    all_ok
}

/// Append an all-clear notice to the original warning content.
/// The `<hr>` separator is well-supported in Matrix HTML clients.
fn append_allclear_to_warning(original_plain: &str, original_html: &str) -> (String, String) {
    let time = chrono::Local::now().format("%H:%M").to_string();
    let new_plain = format!(
        "{original_plain}\n\n---\n✅ Entwarnung ({time}): Diese Warnung gilt nicht mehr."
    );
    let new_html = format!(
        "{original_html}<hr><em>✅ Entwarnung ({time}): Diese Warnung gilt nicht mehr.</em>"
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
            "description": "\nAm Samstag wird eine extreme Wärmebelastung erwartet.\nMit einer zusätzlichen Belastung aufgrund verringerter nächtlicher Abkühlung ist insbesondere im dicht bebauten Stadtgebiet von Berlin zu rechnen.\nNoch ein Satz, der nicht in die kurze Nachricht soll.\n",
            "instruction": "Hitzebelastung kann für den menschlichen Körper gefährlich werden. Vermeiden Sie nach Möglichkeit die Hitze, trinken Sie ausreichend Wasser und halten Sie die Innenräume kühl."
        });

        let desc = format_dwd_description("Berlin", &warning);

        assert!(desc.contains("📍 Berlin"));
        assert!(desc.contains("🕒"));
        assert!(desc.contains("ℹ️ Am Samstag wird eine extreme Wärmebelastung erwartet."));
        assert!(
            desc.contains("✅ Hitzebelastung kann für den menschlichen Körper gefährlich werden.")
        );
        assert!(!desc.contains("Noch ein Satz"));
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
        let (plain, html) =
            format_dwd_allclear("Amtliche WARNUNG vor HITZE", "Berlin, Stadt");

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
