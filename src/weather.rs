use std::time::Duration;

use chrono::{Local, NaiveTime, Timelike};
use matrix_sdk::Client;
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(serde::Deserialize, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub enum WeatherProvider {
    #[default]
    Brightsky,
    Openmeteo,
}

// ── Loop ──────────────────────────────────────────────────────────────────────

pub async fn weather_loop(
    client: Client,
    http: reqwest::Client,
    ref_point: (f64, f64),
    post_time: NaiveTime,
    provider: WeatherProvider,
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

        let result = match provider {
            WeatherProvider::Brightsky  => fetch_brightsky(&http, ref_point).await,
            WeatherProvider::Openmeteo  => fetch_openmeteo(&http, ref_point).await,
        };

        match result {
            Some((plain, html)) => { crate::post_to_rooms(&client, &plain, &html).await; }
            None => warn!("Weather forecast fetch failed — skipping today"),
        }
    }
}

// ── BrightSky (DWD) ───────────────────────────────────────────────────────────

async fn fetch_brightsky(http: &reqwest::Client, (lat, lon): (f64, f64)) -> Option<(String, String)> {
    let date = Local::now().format("%Y-%m-%d").to_string();
    let url  = format!("https://api.brightsky.dev/weather?lat={lat}&lon={lon}&date={date}");

    let body = tokio::time::timeout(Duration::from_secs(15), http.get(&url).send())
        .await.ok()?.ok()?.text().await.ok()?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let entries = json["weather"].as_array()?;

    // BrightSky returns UTC timestamps; convert to local hour for matching.
    let utc_offset_h = Local::now().offset().local_minus_utc() / 3600;
    let target_local_hours: &[i64] = &[7, 13, 18, 22];

    let mut rows_plain = Vec::new();
    let mut rows_html  = Vec::new();

    for &local_h in target_local_hours {
        let utc_h = (local_h - utc_offset_h as i64).rem_euclid(24) as u32;

        let entry = entries.iter().find(|e| {
            e["timestamp"].as_str()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|dt| dt.hour() == utc_h)
                .unwrap_or(false)
        })?;

        let temp   = entry["temperature"].as_f64()?;
        let precip = entry["precipitation_probability"].as_i64().unwrap_or(0);
        let wind   = entry["wind_speed"].as_f64().unwrap_or(0.0);
        let icon   = entry["icon"].as_str().unwrap_or("");
        let (emoji, _) = brightsky_icon(icon);
        let label  = period_label(local_h as u32);

        rows_plain.push(format!("{label}: {emoji} {temp:.0}°C  💧 {precip}%  💨 {wind:.0} km/h"));
        rows_html.push(format!("{label}: {emoji} {temp:.0}°C &nbsp; 💧 {precip}% &nbsp; 💨 {wind:.0} km/h"));
    }

    format_message(rows_plain, rows_html)
}

fn brightsky_icon(icon: &str) -> (&'static str, &'static str) {
    match icon {
        "clear-day"           => ("☀️",  "Clear"),
        "clear-night"         => ("🌙",  "Clear"),
        "partly-cloudy-day"   => ("⛅",  "Partly cloudy"),
        "partly-cloudy-night" => ("🌤️", "Partly cloudy"),
        "cloudy"              => ("☁️",  "Cloudy"),
        "fog"                 => ("🌫️", "Fog"),
        "wind"                => ("🌬️", "Windy"),
        "rain"                => ("🌧️", "Rain"),
        "sleet"               => ("🌨️", "Sleet"),
        "snow"                => ("❄️",  "Snow"),
        "hail"                => ("🌨️", "Hail"),
        "thunderstorm"        => ("⛈️",  "Thunderstorm"),
        _                     => ("🌡️", "Unknown"),
    }
}

// ── Open-Meteo (fallback) ─────────────────────────────────────────────────────

async fn fetch_openmeteo(http: &reqwest::Client, (lat, lon): (f64, f64)) -> Option<(String, String)> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={lat}&longitude={lon}\
         &hourly=weathercode,temperature_2m,precipitation_probability,windspeed_10m\
         &timezone=auto&forecast_days=1"
    );

    let body = tokio::time::timeout(Duration::from_secs(15), http.get(&url).send())
        .await.ok()?.ok()?.text().await.ok()?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let h = &json["hourly"];

    let mut rows_plain = Vec::new();
    let mut rows_html  = Vec::new();

    for &hour in &[7usize, 13, 18, 22] {
        let code   = h["weathercode"][hour].as_i64().unwrap_or(0);
        let temp   = h["temperature_2m"][hour].as_f64()?;
        let precip = h["precipitation_probability"][hour].as_i64().unwrap_or(0);
        let wind   = h["windspeed_10m"][hour].as_f64().unwrap_or(0.0);
        let (emoji, _) = wmo_icon(code);
        let label  = period_label(hour as u32);

        rows_plain.push(format!("{label}: {emoji} {temp:.0}°C  💧 {precip}%  💨 {wind:.0} km/h"));
        rows_html.push(format!("{label}: {emoji} {temp:.0}°C &nbsp; 💧 {precip}% &nbsp; 💨 {wind:.0} km/h"));
    }

    format_message(rows_plain, rows_html)
}

fn wmo_icon(code: i64) -> (&'static str, &'static str) {
    match code {
        0            => ("☀️",  "Clear"),
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
        _            => ("🌡️", "Unknown"),
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn period_label(local_hour: u32) -> &'static str {
    match local_hour {
        7  => "🌅 Morning",
        13 => "🌞 Midday",
        18 => "🌆 Evening",
        22 => "🌙 Night",
        _  => "⏰",
    }
}

fn format_message(rows_plain: Vec<String>, rows_html: Vec<String>) -> Option<(String, String)> {
    if rows_plain.is_empty() {
        return None;
    }
    let date  = Local::now().format("%A, %d %b").to_string();
    let plain = format!("Weather — {date}\n{}", rows_plain.join("\n"));
    let html  = format!("<strong>Weather — {date}</strong><br>{}", rows_html.join("<br>"));
    Some((plain, html))
}
