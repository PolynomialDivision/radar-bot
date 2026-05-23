use std::time::Duration;

use chrono::{Local, NaiveTime};
use matrix_sdk::Client;
use tokio::time::sleep;
use tracing::{info, warn};

// ── Loop ──────────────────────────────────────────────────────────────────────

pub async fn weather_loop(
    client: Client,
    http: reqwest::Client,
    ref_point: (f64, f64),
    location_name: String,
    post_time: NaiveTime,
) {
    loop {
        let now = Local::now();
        let today = now.date_naive();
        let target = if now.time() < post_time {
            today.and_time(post_time)
        } else {
            (today + chrono::Duration::days(1)).and_time(post_time)
        };
        let secs = (target - now.naive_local()).num_seconds().max(0) as u64;
        info!("Next weather forecast in {secs}s (at {})", target.format("%H:%M"));
        sleep(Duration::from_secs(secs)).await;

        match fetch_forecast(&http, ref_point, &location_name).await {
            Some((plain, html)) => {
                crate::post_to_rooms(&client, &plain, &html).await;
            }
            None => warn!("Weather forecast fetch failed — skipping today"),
        }
    }
}

// ── Fetch ─────────────────────────────────────────────────────────────────────

async fn fetch_forecast(
    http: &reqwest::Client,
    (lat, lon): (f64, f64),
    location: &str,
) -> Option<(String, String)> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={lat}&longitude={lon}\
         &daily=weathercode,temperature_2m_max,temperature_2m_min,\
         precipitation_sum,windspeed_10m_max\
         &timezone=auto&forecast_days=1"
    );

    let body = tokio::time::timeout(
        Duration::from_secs(15),
        http.get(&url).send(),
    )
    .await.ok()?.ok()?
    .text().await.ok()?;

    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let d = &json["daily"];

    let code    = d["weathercode"][0].as_i64().unwrap_or(0);
    let t_max   = d["temperature_2m_max"][0].as_f64()?;
    let t_min   = d["temperature_2m_min"][0].as_f64()?;
    let precip  = d["precipitation_sum"][0].as_f64().unwrap_or(0.0);
    let wind    = d["windspeed_10m_max"][0].as_f64().unwrap_or(0.0);

    let (icon, desc) = wmo_description(code);
    let date = Local::now().format("%A, %d %b").to_string();

    let plain = format!(
        "{icon} Weather for {location} — {date}\n{desc}\n🌡 {t_min:.0}°C – {t_max:.0}°C  💧 {precip:.1}mm  💨 {wind:.0} km/h"
    );
    let html = format!(
        "<b>{icon} Weather for {location} — {date}</b><br>\
         {desc}<br>\
         🌡 {t_min:.0}°C – {t_max:.0}°C &nbsp; 💧 {precip:.1}mm &nbsp; 💨 {wind:.0} km/h"
    );

    Some((plain, html))
}

// ── WMO weather code → (icon, description) ───────────────────────────────────

fn wmo_description(code: i64) -> (&'static str, &'static str) {
    match code {
        0            => ("☀️",  "Clear sky"),
        1            => ("🌤️", "Mainly clear"),
        2            => ("⛅",  "Partly cloudy"),
        3            => ("☁️",  "Overcast"),
        45 | 48      => ("🌫️", "Fog"),
        51 | 53 | 55 => ("🌦️", "Drizzle"),
        56 | 57      => ("🌦️", "Freezing drizzle"),
        61 | 63 | 65 => ("🌧️", "Rain"),
        66 | 67      => ("🌧️", "Freezing rain"),
        71 | 73 | 75 => ("🌨️", "Snowfall"),
        77           => ("🌨️", "Snow grains"),
        80 | 81 | 82 => ("🌦️", "Rain showers"),
        85 | 86      => ("🌨️", "Snow showers"),
        95           => ("⛈️",  "Thunderstorm"),
        96 | 99      => ("⛈️",  "Thunderstorm with hail"),
        _            => ("🌡️", "Unknown conditions"),
    }
}
