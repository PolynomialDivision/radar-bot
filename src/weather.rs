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

        match fetch_forecast(&http, ref_point).await {
            Some((plain, html)) => {
                crate::post_to_rooms(&client, &plain, &html).await;
            }
            None => warn!("Weather forecast fetch failed — skipping today"),
        }
    }
}

// ── Fetch ─────────────────────────────────────────────────────────────────────

async fn fetch_forecast(http: &reqwest::Client, (lat, lon): (f64, f64)) -> Option<(String, String)> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={lat}&longitude={lon}\
         &hourly=weathercode,temperature_2m,precipitation_probability,windspeed_10m\
         &timezone=auto&forecast_days=1"
    );

    let body = tokio::time::timeout(
        Duration::from_secs(15),
        http.get(&url).send(),
    )
    .await.ok()?.ok()?
    .text().await.ok()?;

    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let h = &json["hourly"];

    // Representative hours for each period of the day.
    let periods: &[(&str, usize)] = &[
        ("🌅 Morning",  7),
        ("🌞 Midday",  13),
        ("🌆 Evening", 18),
        ("🌙 Night",   22),
    ];

    let date = Local::now().format("%A, %d %b").to_string();
    let mut plain_rows = Vec::new();
    let mut html_rows  = Vec::new();

    for &(label, hour) in periods {
        let code   = h["weathercode"][hour].as_i64().unwrap_or(0);
        let temp   = h["temperature_2m"][hour].as_f64()?;
        let precip = h["precipitation_probability"][hour].as_i64().unwrap_or(0);
        let wind   = h["windspeed_10m"][hour].as_f64().unwrap_or(0.0);
        let (icon, _) = wmo_description(code);

        plain_rows.push(format!("{label}: {icon} {temp:.0}°C  💧 {precip}%  💨 {wind:.0} km/h"));
        html_rows.push(format!(
            "{label}: {icon} {temp:.0}°C &nbsp; 💧 {precip}% &nbsp; 💨 {wind:.0} km/h"
        ));
    }

    let plain = format!("Weather — {date}\n{}", plain_rows.join("\n"));
    let html  = format!("<b>Weather — {date}</b><br>{}", html_rows.join("<br>"));

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
        _            => ("🌡️", "Unknown"),
    }
}
