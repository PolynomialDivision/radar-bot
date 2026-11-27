use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Local, Timelike};
use mxbot_common::{
    clock::Zone,
    config::TimeOfDay,
    schedule::{self, Delivery, Job, Outcome, Rule, Slot},
    supervise::Alive,
    Bot,
};
use tracing::warn;

use crate::fetch::{FetchError, Http};
use crate::store::Db;

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
    http: Http,
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
    http: &Http,
    ref_point: (f64, f64),
    provider: &WeatherProvider,
    slot: &Slot,
) -> Outcome {
    let now = Local::now();
    if periods_ahead(now).next().is_none() {
        // Late in the evening: nothing of the day is left to forecast.
        return Outcome::Skipped;
    }
    let rows = match provider {
        WeatherProvider::Brightsky => fetch_brightsky(http, ref_point)
            .await
            .map(|json| brightsky_rows(&json, now)),
        WeatherProvider::Openmeteo => fetch_openmeteo(http, ref_point)
            .await
            .map(|json| openmeteo_rows(&json, now)),
    };
    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => {
            warn!("Weather forecast fetch failed, trying again later: {e:#}");
            return Outcome::RetryIn(crate::RETRY_AFTER);
        }
    };
    let Some((plain, html)) = format_message(&rows, now) else {
        warn!("Weather forecast has no period ahead, trying again later");
        return Outcome::RetryIn(crate::RETRY_AFTER);
    };
    let key = format!("radar-bot:weather:{}", slot.key());
    let deadline = std::time::Instant::now() + crate::SEND_FOR;
    crate::post_slot(client, &plain, &html, &key, deadline)
        .await
        .outcome(crate::RETRY_AFTER)
}

// ── Forecast rows ─────────────────────────────────────────────────────────────

/// The hours (local time) the forecast shows, one row each.
const PERIOD_HOURS: [u32; 4] = [7, 13, 18, 22];

/// One period of the day. A value the provider does not give stays `None` (shown as "–").
#[derive(Debug, Clone, PartialEq)]
struct Row {
    hour: u32,
    emoji: &'static str,
    temp: f64,
    precip_percent: Option<i64>,
    wind_kmh: Option<f64>,
}

impl Row {
    fn render(&self, separator: &str) -> String {
        let precip = self
            .precip_percent
            .map_or("–".to_owned(), |p| format!("{p}%"));
        let wind = self
            .wind_kmh
            .map_or("–".to_owned(), |w| format!("{w:.0} km/h"));
        format!(
            "{}: {} {:.0}°C{separator}💧 {precip}{separator}💨 {wind}",
            period_label(self.hour),
            self.emoji,
            self.temp
        )
    }
}

/// The periods still ahead at `now` (the current hour counts), in order.
fn periods_ahead(now: DateTime<Local>) -> impl Iterator<Item = u32> {
    PERIOD_HOURS.into_iter().filter(move |h| *h >= now.hour())
}

// ── BrightSky (DWD) ───────────────────────────────────────────────────────────

async fn fetch_brightsky(http: &Http, (lat, lon): (f64, f64)) -> Result<serde_json::Value> {
    let date = Local::now().format("%Y-%m-%d").to_string();
    let url = format!("https://api.brightsky.dev/weather?lat={lat}&lon={lon}&date={date}");
    fetch_json(http, &url).await
}

/// Rows from a BrightSky answer (hourly entries with UTC timestamps).
fn brightsky_rows(json: &serde_json::Value, now: DateTime<Local>) -> Vec<Row> {
    let Some(entries) = json["weather"].as_array() else {
        return Vec::new();
    };
    periods_ahead(now)
        .filter_map(|local_h| {
            let entry = entries.iter().find(|e| {
                e["timestamp"]
                    .as_str()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                    .is_some_and(|dt| {
                        let local = dt.with_timezone(&Local);
                        local.date_naive() == now.date_naive() && local.hour() == local_h
                    })
            })?;
            Some(Row {
                hour: local_h,
                emoji: brightsky_icon(entry["icon"].as_str().unwrap_or("")).0,
                temp: entry["temperature"].as_f64()?,
                precip_percent: entry["precipitation_probability"].as_i64(),
                wind_kmh: entry["wind_speed"].as_f64(),
            })
        })
        .collect()
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

async fn fetch_openmeteo(http: &Http, (lat, lon): (f64, f64)) -> Result<serde_json::Value> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={lat}&longitude={lon}\
         &hourly=weathercode,temperature_2m,precipitation_probability,windspeed_10m\
         &timezone=auto&forecast_days=1"
    );
    fetch_json(http, &url).await
}

/// Rows from an Open-Meteo answer (hourly arrays from local midnight).
fn openmeteo_rows(json: &serde_json::Value, now: DateTime<Local>) -> Vec<Row> {
    let h = &json["hourly"];
    periods_ahead(now)
        .filter_map(|hour| {
            let i = hour as usize;
            Some(Row {
                hour,
                emoji: h["weathercode"][i]
                    .as_i64()
                    .map_or("🌡️", |code| wmo_icon(code).0),
                temp: h["temperature_2m"][i].as_f64()?,
                precip_percent: h["precipitation_probability"][i].as_i64(),
                wind_kmh: h["windspeed_10m"][i].as_f64(),
            })
        })
        .collect()
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

/// `GET url` of a forecast provider as JSON; a failed request or a non-2xx answer is an error.
async fn fetch_json(http: &Http, url: &str) -> Result<serde_json::Value> {
    let response = match http.get_api(url, &[], Duration::from_secs(15)).await {
        Ok(response) => response,
        Err(FetchError::Timeout) => bail!("forecast fetch timed out"),
        Err(e) => bail!("forecast request failed: {e}"),
    };
    if !response.is_success() {
        bail!(
            "forecast answered HTTP {} ({})",
            response.status,
            response.content_type()
        );
    }
    serde_json::from_str(&response.body).context("forecast JSON parse failed")
}

fn period_label(local_hour: u32) -> &'static str {
    match local_hour {
        7 => "🌅 Morning",
        13 => "🌞 Midday",
        18 => "🌆 Evening",
        22 => "🌙 Night",
        _ => "⏰",
    }
}

/// The forecast message; `None` when no period is left to show.
fn format_message(rows: &[Row], now: DateTime<Local>) -> Option<(String, String)> {
    if rows.is_empty() {
        return None;
    }
    let date = now.format("%A, %d %b").to_string();
    let plain_rows: Vec<String> = rows.iter().map(|r| r.render("  ")).collect();
    let html_rows: Vec<String> = rows.iter().map(|r| r.render(" &nbsp; ")).collect();
    let plain = format!("🌦️ Weather · {date}\n{}", plain_rows.join("\n"));
    let html = format!(
        "🌦️ <strong>Weather · {date}</strong><br>{}",
        html_rows.join("<br>")
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

    fn temp_db(_what: &str) -> (Db, tempfile::TempDir) {
        crate::testing::test_db()
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
        let (db, _dir) = temp_db("weather-dst");
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
    }

    #[tokio::test]
    async fn a_forecast_that_failed_is_tried_again_and_recorded_only_once_posted() {
        let (db, _dir) = temp_db("weather-retry");
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
    }

    #[tokio::test]
    async fn a_restart_after_the_post_time_catches_the_forecast_up() {
        let (db, _dir) = temp_db("weather-restart");
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
    }

    fn at(hour: u32) -> DateTime<Local> {
        use chrono::TimeZone;
        Local.with_ymd_and_hms(2026, 11, 20, hour, 0, 0).unwrap()
    }

    fn brightsky_entry(hour: u32, precip: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "timestamp": at(hour).with_timezone(&Utc).to_rfc3339(),
            "icon": "cloudy",
            "temperature": 4.4,
            "precipitation_probability": precip,
            "wind_speed": 12.0,
        })
    }

    #[test]
    fn the_forecast_shows_what_is_known_and_still_ahead() {
        let json = serde_json::json!({"weather": [
            brightsky_entry(7, serde_json::Value::Null),
            brightsky_entry(13, serde_json::Value::Null),
            // 18:00 missing from the answer
            brightsky_entry(22, serde_json::json!(40)),
        ]});

        let rows = brightsky_rows(&json, at(9));
        let (plain, _) = format_message(&rows, at(9)).unwrap();

        assert_eq!(
            plain.lines().skip(1).collect::<Vec<_>>(),
            vec![
                "🌞 Midday: ☁️ 4°C  💧 –  💨 12 km/h",
                "🌙 Night: ☁️ 4°C  💧 40%  💨 12 km/h",
            ]
        );
        assert!(format_message(&brightsky_rows(&json, at(23)), at(23)).is_none());
    }

    #[test]
    fn open_meteo_rows_skip_past_and_missing_hours() {
        let mut temps = vec![serde_json::json!(1.0); 24];
        temps[18] = serde_json::Value::Null;
        let json = serde_json::json!({"hourly": {
            "weathercode": vec![3; 24],
            "temperature_2m": temps,
            "precipitation_probability": vec![serde_json::Value::Null; 24],
            "windspeed_10m": vec![10.0; 24],
        }});

        let rows = openmeteo_rows(&json, at(12));

        assert_eq!(
            rows.iter().map(|r| r.hour).collect::<Vec<_>>(),
            vec![13, 22]
        );
        assert!(rows.iter().all(|r| r.precip_percent.is_none()));
    }

    #[tokio::test]
    async fn a_forecast_answer_that_is_no_success_is_an_error() {
        let point = (52.47, 13.42);
        let http = |status, body: &str| {
            Http::recorded(std::sync::Arc::new(crate::fetch::Recorded::new().route(
                "https://api.brightsky.dev/",
                status,
                body,
            )))
        };

        assert!(fetch_brightsky(&http(200, "{\"weather\": []}"), point)
            .await
            .is_ok());
        assert!(fetch_brightsky(&http(503, "{\"weather\": []}"), point)
            .await
            .is_err());
        assert!(fetch_brightsky(&http(200, "<html>"), point).await.is_err());
        assert!(fetch_openmeteo(&Http::offline(), point).await.is_err());
    }
}
