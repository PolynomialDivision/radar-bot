use std::time::Duration;

use chrono::{Local, Timelike};
use mxbot_common::{
    clock::Zone,
    config::TimeOfDay,
    schedule::{self, Delivery, Job, Outcome, Rule, Slot},
    supervise::Alive,
    Bot,
};
use tracing::warn;

use crate::db::Db;

#[derive(serde::Deserialize, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub enum WeatherProvider {
    #[default]
    Brightsky,
    Openmeteo,
}

// ── Loop ──────────────────────────────────────────────────────────────────────

/// The forecast as a scheduled job: daily at `post_time` (local time), one
/// missed by a restart caught up within [`crate::CATCH_UP`], recorded in the
/// `alerts` table once posted (`weather:2026-10-30`, as before).
pub fn weather_job(zone: Zone, post_time: TimeOfDay) -> Job {
    let rule = Rule::daily(post_time);
    Job::new(
        "weather",
        zone,
        Delivery::AtLeastOnce,
        crate::CATCH_UP,
        move || Some(rule.clone()),
    )
}

pub async fn weather_loop(
    client: Bot,
    http: reqwest::Client,
    db: Db,
    ref_point: (f64, f64),
    job: Job,
    provider: WeatherProvider,
    alive: Alive,
) {
    schedule::run(job, db, alive, move |slot: Slot| {
        let client = client.clone();
        let http = http.clone();
        let provider = provider.clone();
        async move { post_forecast(&client, &http, ref_point, &provider, &slot).await }
    })
    .await
}

/// Fetch and post the forecast of `slot`. A failed fetch or a post that did
/// not get through is tried again after [`crate::RETRY_AFTER`] instead of
/// losing the day's forecast.
async fn post_forecast(
    client: &Bot,
    http: &reqwest::Client,
    ref_point: (f64, f64),
    provider: &WeatherProvider,
    slot: &Slot,
) -> Outcome {
    let result = match provider {
        WeatherProvider::Brightsky => fetch_brightsky(http, ref_point).await,
        WeatherProvider::Openmeteo => fetch_openmeteo(http, ref_point).await,
    };
    let Some((plain, html)) = result else {
        warn!("Weather forecast fetch failed, trying again later");
        return Outcome::RetryIn(crate::RETRY_AFTER);
    };
    let key = format!("radar-bot:weather:{}", slot.key());
    let deadline = std::time::Instant::now() + crate::SEND_FOR;
    crate::post_slot(client, &plain, &html, &key, deadline)
        .await
        .outcome(crate::RETRY_AFTER)
}

// ── BrightSky (DWD) ───────────────────────────────────────────────────────────

async fn fetch_brightsky(
    http: &reqwest::Client,
    (lat, lon): (f64, f64),
) -> Option<(String, String)> {
    let date = Local::now().format("%Y-%m-%d").to_string();
    let url = format!("https://api.brightsky.dev/weather?lat={lat}&lon={lon}&date={date}");

    let body = tokio::time::timeout(Duration::from_secs(15), http.get(&url).send())
        .await
        .ok()?
        .ok()?
        .text()
        .await
        .ok()?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let entries = json["weather"].as_array()?;

    // BrightSky returns UTC timestamps; convert to local hour for matching.
    let utc_offset_h = Local::now().offset().local_minus_utc() / 3600;
    let target_local_hours: &[i64] = &[7, 13, 18, 22];

    let mut rows_plain = Vec::new();
    let mut rows_html = Vec::new();

    for &local_h in target_local_hours {
        let utc_h = (local_h - utc_offset_h as i64).rem_euclid(24) as u32;

        let entry = entries.iter().find(|e| {
            e["timestamp"]
                .as_str()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|dt| dt.hour() == utc_h)
                .unwrap_or(false)
        })?;

        let temp = entry["temperature"].as_f64()?;
        let precip = entry["precipitation_probability"].as_i64().unwrap_or(0);
        let wind = entry["wind_speed"].as_f64().unwrap_or(0.0);
        let icon = entry["icon"].as_str().unwrap_or("");
        let (emoji, _) = brightsky_icon(icon);
        let label = period_label(local_h as u32);

        rows_plain.push(format!(
            "{label}: {emoji} {temp:.0}°C  💧 {precip}%  💨 {wind:.0} km/h"
        ));
        rows_html.push(format!(
            "{label}: {emoji} {temp:.0}°C &nbsp; 💧 {precip}% &nbsp; 💨 {wind:.0} km/h"
        ));
    }

    format_message(rows_plain, rows_html)
}

fn brightsky_icon(icon: &str) -> (&'static str, &'static str) {
    match icon {
        "clear-day" => ("☀️", "Clear"),
        "clear-night" => ("🌙", "Clear"),
        "partly-cloudy-day" => ("⛅", "Partly cloudy"),
        "partly-cloudy-night" => ("🌤️", "Partly cloudy"),
        "cloudy" => ("☁️", "Cloudy"),
        "fog" => ("🌫️", "Fog"),
        "wind" => ("🌬️", "Windy"),
        "rain" => ("🌧️", "Rain"),
        "sleet" => ("🌨️", "Sleet"),
        "snow" => ("❄️", "Snow"),
        "hail" => ("🌨️", "Hail"),
        "thunderstorm" => ("⛈️", "Thunderstorm"),
        _ => ("🌡️", "Unknown"),
    }
}

// ── Open-Meteo (fallback) ─────────────────────────────────────────────────────

async fn fetch_openmeteo(
    http: &reqwest::Client,
    (lat, lon): (f64, f64),
) -> Option<(String, String)> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={lat}&longitude={lon}\
         &hourly=weathercode,temperature_2m,precipitation_probability,windspeed_10m\
         &timezone=auto&forecast_days=1"
    );

    let body = tokio::time::timeout(Duration::from_secs(15), http.get(&url).send())
        .await
        .ok()?
        .ok()?
        .text()
        .await
        .ok()?;
    let json: serde_json::Value = serde_json::from_str(&body).ok()?;
    let h = &json["hourly"];

    let mut rows_plain = Vec::new();
    let mut rows_html = Vec::new();

    for &hour in &[7usize, 13, 18, 22] {
        let code = h["weathercode"][hour].as_i64().unwrap_or(0);
        let temp = h["temperature_2m"][hour].as_f64()?;
        let precip = h["precipitation_probability"][hour].as_i64().unwrap_or(0);
        let wind = h["windspeed_10m"][hour].as_f64().unwrap_or(0.0);
        let (emoji, _) = wmo_icon(code);
        let label = period_label(hour as u32);

        rows_plain.push(format!(
            "{label}: {emoji} {temp:.0}°C  💧 {precip}%  💨 {wind:.0} km/h"
        ));
        rows_html.push(format!(
            "{label}: {emoji} {temp:.0}°C &nbsp; 💧 {precip}% &nbsp; 💨 {wind:.0} km/h"
        ));
    }

    format_message(rows_plain, rows_html)
}

fn wmo_icon(code: i64) -> (&'static str, &'static str) {
    match code {
        0 => ("☀️", "Clear"),
        1 => ("🌤️", "Mainly clear"),
        2 => ("⛅", "Partly cloudy"),
        3 => ("☁️", "Overcast"),
        45 | 48 => ("🌫️", "Fog"),
        51 | 53 | 55 => ("🌦️", "Drizzle"),
        56 | 57 => ("🌦️", "Freezing drizzle"),
        61 | 63 | 65 => ("🌧️", "Rain"),
        66 | 67 => ("🌧️", "Freezing rain"),
        71 | 73 | 75 => ("🌨️", "Snowfall"),
        77 => ("🌨️", "Snow grains"),
        80..=82 => ("🌦️", "Rain showers"),
        85 | 86 => ("🌨️", "Snow showers"),
        95 => ("⛈️", "Thunderstorm"),
        96 | 99 => ("⛈️", "Thunderstorm with hail"),
        _ => ("🌡️", "Unknown"),
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn period_label(local_hour: u32) -> &'static str {
    match local_hour {
        7 => "🌅 Morning",
        13 => "🌞 Midday",
        18 => "🌆 Evening",
        22 => "🌙 Night",
        _ => "⏰",
    }
}

fn format_message(rows_plain: Vec<String>, rows_html: Vec<String>) -> Option<(String, String)> {
    if rows_plain.is_empty() {
        return None;
    }
    let date = Local::now().format("%A, %d %b").to_string();
    let plain = format!("🌦️ Weather · {date}\n{}", rows_plain.join("\n"));
    let html = format!(
        "🌦️ <strong>Weather · {date}</strong><br>{}",
        rows_html.join("<br>")
    );
    Some((plain, html))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use mxbot_common::schedule::Runner;

    fn utc(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn temp_db(what: &str) -> (Db, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "radar-bot-{what}-{}-{}.db",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        (Db::open(&path).expect("open temp db"), path)
    }

    /// One round of the forecast job at `now`, its work answering `outcome`;
    /// when it wants to look again, and whether the work ran.
    async fn round(
        runner: &mut Runner,
        db: &Db,
        now: &str,
        outcome: Outcome,
    ) -> (Option<DateTime<Utc>>, bool) {
        let mut ran = false;
        let wake = runner
            .round(db, utc(now), &mut |_slot: Slot| {
                ran = true;
                async move { outcome }
            })
            .await;
        (wake, ran)
    }

    fn eight() -> Runner {
        Runner::new(weather_job(Zone::DEFAULT, TimeOfDay::new(8, 0)))
    }

    #[tokio::test]
    async fn the_forecast_is_at_local_time_across_both_dst_changes() {
        let (db, path) = temp_db("weather-dst");
        db.mark_alert_seen("weather:2026-03-28", "weather")
            .await
            .unwrap();
        db.mark_alert_seen("weather:2026-10-24", "weather")
            .await
            .unwrap();
        let mut weather = eight();
        // 08:00 CEST is 06:00 UTC: not an hour late.
        let (wake, ran) = round(&mut weather, &db, "2026-03-28T22:00:00Z", Outcome::Done).await;
        assert_eq!(wake, Some(utc("2026-03-29T06:00:00Z")));
        assert!(!ran);
        // 08:00 CET is 07:00 UTC: an hour early (where a naive sleep woke) nothing is due.
        let mut weather = eight();
        let (wake, _) = round(&mut weather, &db, "2026-10-24T22:00:00Z", Outcome::Done).await;
        assert_eq!(wake, Some(utc("2026-10-25T07:00:00Z")));
        let (_, ran) = round(&mut weather, &db, "2026-10-25T06:00:00Z", Outcome::Done).await;
        assert!(!ran);
        let (_, ran) = round(&mut weather, &db, "2026-10-25T07:00:00Z", Outcome::Done).await;
        assert!(ran);
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn a_forecast_that_failed_is_tried_again_and_recorded_only_once_posted() {
        let (db, path) = temp_db("weather-retry");
        let mut weather = eight();
        let retry = Outcome::RetryIn(crate::RETRY_AFTER);
        // The fetch (or the post) failed at 08:00: not recorded, tried at 08:15.
        let (wake, ran) = round(&mut weather, &db, "2026-10-30T07:00:00Z", retry).await;
        assert!(ran);
        assert!(!db.is_alert_seen("weather:2026-10-30").await.unwrap());
        assert_eq!(wake, Some(utc("2026-10-30T07:00:00Z")));
        let (wake, ran) = round(&mut weather, &db, "2026-10-30T07:01:00Z", Outcome::Done).await;
        assert!(!ran);
        assert_eq!(wake, Some(utc("2026-10-30T07:15:00Z")));
        let (_, ran) = round(&mut weather, &db, "2026-10-30T07:15:00Z", Outcome::Done).await;
        assert!(ran);
        assert!(db.is_alert_seen("weather:2026-10-30").await.unwrap());
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn a_restart_after_the_post_time_catches_the_forecast_up() {
        let (db, path) = temp_db("weather-restart");
        // Posted by the version before (same row): not again.
        db.mark_alert_seen("weather:2026-10-29", "weather")
            .await
            .unwrap();
        let (_, ran) = round(&mut eight(), &db, "2026-10-29T09:00:00Z", Outcome::Done).await;
        assert!(!ran);
        // Down over 08:00: posted at the restart at 10:00, not after 11:00.
        let (_, ran) = round(&mut eight(), &db, "2026-10-30T09:00:00Z", Outcome::Done).await;
        assert!(ran);
        let (_, ran) = round(&mut eight(), &db, "2026-10-31T10:30:00Z", Outcome::Done).await;
        assert!(!ran);
        std::fs::remove_file(path).ok();
    }
}
