use std::time::Duration;

use anyhow::{Context, Result};
use matrix_sdk::Client;
use tokio::time::sleep;
use tracing::{info, warn};

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
    loop {
        if let Some(ref ags) = config.nina_ags {
            if let Err(e) = check_nina(&client, &http, &db, ags).await {
                warn!("NINA check failed: {e:#}");
            }
        }
        if !config.dwd_region_keywords.is_empty() {
            if let Err(e) =
                check_dwd_weather_warnings(&client, &http, &db, &config.dwd_region_keywords).await
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

        if db.is_alert_seen(id).await? {
            continue;
        }

        let data = &w["payload"]["data"];
        if data["msgType"].as_str() == Some("Cancel") {
            continue;
        }

        let headline = data["headline"].as_str().unwrap_or("Warnung");
        let severity = data["severity"].as_str();
        let desc: String = data["description"]
            .as_str()
            .unwrap_or("")
            .trim()
            .chars()
            .take(400)
            .collect();

        let (plain, html) = format_alert("NINA/warnung.bund.de", headline, severity, &desc, None);
        if crate::post_to_rooms(client, &plain, &html).await {
            db.mark_alert_seen(id, "nina").await?;
            info!("ALERT sent [nina]: {headline}");
        } else {
            warn!("ALERT post failed [nina], will retry next poll: {headline}");
        }
    }

    Ok(())
}

// ── DWD weather warnings ─────────────────────────────────────────────────────

async fn check_dwd_weather_warnings(
    client: &Client,
    http: &reqwest::Client,
    db: &Db,
    region_keywords: &[String],
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

    for bucket in ["warnings", "vorabInformation"] {
        let Some(regions) = json[bucket].as_object() else {
            continue;
        };
        for warnings in regions.values() {
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

                let event = w["event"]
                    .as_str()
                    .or_else(|| w["headline"].as_str())
                    .unwrap_or("Wetterwarnung");
                let id = dwd_warning_id(bucket, region, w);
                if db.is_alert_seen(&id).await? {
                    continue;
                }

                let headline = w["headline"].as_str().unwrap_or(event);
                let severity = w["level"]
                    .as_i64()
                    .map(|level| format!("DWD level {level}"));
                let desc = format_dwd_description(region, w);

                let (plain, html) = format_alert(
                    "DWD Weather Warnings",
                    headline,
                    severity.as_deref(),
                    &desc,
                    None,
                );
                if crate::post_to_rooms(client, &plain, &html).await {
                    db.mark_alert_seen(&id, "dwd").await?;
                    info!("ALERT sent [dwd]: {headline} ({region})");
                } else {
                    warn!("ALERT post failed [dwd], will retry next poll: {headline} ({region})");
                }
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

fn dwd_warning_id(bucket: &str, region: &str, w: &serde_json::Value) -> String {
    let event = w["event"].as_str().unwrap_or("warning");
    let start = w["start"].as_i64().unwrap_or(0);
    let end = w["end"].as_i64().unwrap_or(0);
    let level = w["level"].as_i64().unwrap_or(0);
    format!("dwd:{bucket}:{region}:{event}:{start}:{end}:{level}")
}

fn format_dwd_description(region: &str, w: &serde_json::Value) -> String {
    let description = w["description"].as_str().unwrap_or("").trim();
    let instruction = w["instruction"].as_str().unwrap_or("").trim();
    match (description.is_empty(), instruction.is_empty()) {
        (false, false) => format!("{region}\n{description}\n{instruction}"),
        (false, true) => format!("{region}\n{description}"),
        (true, false) => format!("{region}\n{instruction}"),
        (true, true) => region.to_owned(),
    }
    .chars()
    .take(700)
    .collect()
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
        let place = props["place"].as_str().unwrap_or("");
        let mag = props["mag"]
            .as_f64()
            .map(|m| format!("M{m:.1}"))
            .unwrap_or_default();
        let alert = props["alert"].as_str(); // "green","yellow","orange","red"
        let url = props["url"].as_str();

        let desc = if place.is_empty() {
            mag.clone()
        } else {
            format!("{mag} — {place}")
        };

        let (plain, html) = format_alert("USGS Earthquakes", title, alert, &desc, url);
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

        let desc: String = item
            .description
            .as_deref()
            .unwrap_or("")
            .trim()
            .chars()
            .take(300)
            .collect();
        let (plain, html) = format_alert("GDACS", &item.title, None, &desc, item.link.as_deref());
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
    severity: Option<&str>,
    description: &str,
    link: Option<&str>,
) -> (String, String) {
    let sev = severity.map(|s| format!(" [{s}]")).unwrap_or_default();
    let link_plain = link.map(|l| format!("\n{l}")).unwrap_or_default();
    let link_html = link
        .map(|l| format!("<br><a href=\"{l}\">{l}</a>"))
        .unwrap_or_default();

    let plain = format!("EMERGENCY{sev}: {event}\n{description}{link_plain}\nSource: {source}");
    let html = format!(
        "<b>🚨 EMERGENCY{sev}: {event}</b><br>{description}{link_html}<br><em>Source: {source}</em>"
    );
    (plain, html)
}
