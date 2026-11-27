//! USGS significant earthquakes and GDACS disasters near the reference point.

use anyhow::{Context, Result};
use tracing::{info, warn};

use super::fetch_api;
use super::post::{concise_sentences, format_alert, post_alert_to_rooms, AlertRooms};
use crate::fetch::Http;
use crate::store::Db;

pub(super) fn format_usgs_description(f: &serde_json::Value) -> String {
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

pub(super) fn format_gdacs_description(
    item: &crate::FeedItem,
    coords: Option<(f64, f64)>,
) -> String {
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

/// The significant earthquakes of the last day (USGS GeoJSON).
pub(super) async fn fetch_usgs(http: &Http) -> Result<serde_json::Value> {
    let url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson";
    let body = fetch_api(http, url, "USGS").await?.body;
    serde_json::from_str(&body).context("USGS JSON parse failed")
}

/// The items of the GDACS feed (a feed the parser cannot read yields none, as before).
pub(super) async fn fetch_gdacs(http: &Http) -> Result<Vec<crate::FeedItem>> {
    let xml = fetch_api(http, "https://www.gdacs.org/xml/rss.xml", "GDACS")
        .await?
        .body;
    Ok(crate::parse_feed(&xml, "GDACS"))
}

pub(super) async fn check_usgs(
    client: &impl AlertRooms,
    http: &Http,
    db: &Db,
    ref_point: Option<(f64, f64)>,
    max_distance_km: Option<f64>,
) -> Result<()> {
    let json = fetch_usgs(http).await?;
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
            .is_some_and(|t| t < cutoff_ms)
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
                _ => continue,                  // no coordinates: can't verify proximity, skip
            }
        }

        let props = &f["properties"];
        let title = props["title"].as_str().unwrap_or("Earthquake");
        let url = props["url"].as_str();

        let desc = format_usgs_description(f);

        let (plain, html) = format_alert("USGS Earthquakes", title, &desc, url);
        // Retried with the same transaction IDs: rooms that got it get no second copy.
        let key = format!("usgs:{id}");
        if post_alert_to_rooms(client, &key, &plain, &html).await.0 {
            db.mark_alert_seen(id, "usgs").await?;
            info!("ALERT sent [usgs]: {title}");
        } else {
            warn!("ALERT post failed [usgs], will retry next poll: {title}");
        }
    }

    Ok(())
}

pub(super) async fn check_gdacs(
    client: &impl AlertRooms,
    http: &Http,
    db: &Db,
    ref_point: Option<(f64, f64)>,
    max_distance_km: Option<f64>,
) -> Result<()> {
    let items = fetch_gdacs(http).await?;
    let cutoff = chrono::Utc::now().timestamp() - 48 * 3600;

    for item in items {
        // Green = routine monitoring, not actionable. Only send Orange and Red.
        if item.title.to_lowercase().starts_with("green") {
            continue;
        }

        // Skip events older than 48 hours to avoid backlog spam on first run.
        if item.published_at.is_some_and(|t| t < cutoff) {
            continue;
        }

        // Distance filter using <geo:lat>/<geo:long> from the RSS item.
        if let (Some(max_km), Some((ref_lat, ref_lon))) = (max_distance_km, ref_point) {
            match item.coords {
                Some((lat, lon)) if haversine_km(ref_lat, ref_lon, lat, lon) <= max_km => {}
                Some(_) => continue, // too far
                None => continue,    // no coordinates: can't verify proximity, skip
            }
        }

        if db.is_alert_seen(&item.guid).await? {
            continue;
        }

        let desc = format_gdacs_description(&item, item.coords);
        let (plain, html) = format_alert("GDACS", &item.title, &desc, item.link.as_deref());
        // Retried with the same transaction IDs: rooms that got it get no second copy.
        let key = format!("gdacs:{}", item.guid);
        if post_alert_to_rooms(client, &key, &plain, &html).await.0 {
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

pub(super) fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}
