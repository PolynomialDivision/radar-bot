mod alerts;
mod article;
mod config;
mod db;
mod digest;
mod feed;
mod filter;
mod geo;
mod polling;
mod sources;
mod weather;

use article::*;
use config::*;
use digest::*;
use feed::*;
use filter::*;
use geo::*;
use polling::*;

use dashmap::DashMap;
use db::Db;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;

use anyhow::Result;
use chrono::{Local, NaiveTime, Timelike};
use mxbot_common::{
    admin::Dispatch,
    clock::Zone,
    config::{MatrixConfig, SecurityConfig, TimeOfDay},
    matrix_sdk::{
        deserialized_responses::EncryptionInfo,
        ruma::events::room::message::{
            MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
        },
        Room, RoomState,
    },
    schedule::{self, Delivered},
    supervise::{supervise_ticking, Alive},
    Bot,
};

use serde::{Deserialize, Serialize};
use tokio::{task::JoinSet, time::sleep, time::Duration};
use tracing::{error, info, warn};

// ── Posting ───────────────────────────────────────────────────────────────────

/// Returns true if all rooms received the message. On false the caller should
/// requeue the items so they appear in the next digest.
pub(crate) async fn post_to_rooms(bot: &Bot, plain: &str, html: &str) -> bool {
    let failed = bot
        .broadcast(RoomMessageEventContent::text_html(plain, html))
        .await;
    for (room, e) in &failed {
        error!("Failed to post to {room}: {e}");
    }
    failed.is_empty()
}

/// How long a scheduled post (digest, forecast) keeps retrying a homeserver
/// that does not answer (a restart, an outage of minutes).
pub(crate) const SEND_FOR: Duration = Duration::from_secs(10 * 60);
/// When a scheduled post that did not get through (or whose forecast could
/// not be fetched) is tried again, within its catch-up window.
pub(crate) const RETRY_AFTER: Duration = Duration::from_secs(15 * 60);
/// How late a digest or forecast missed by a restart still goes out.
pub(crate) const CATCH_UP: Duration = Duration::from_secs(3 * 60 * 60);

/// A scheduled post into the broadcast rooms: transaction IDs from `key`
/// (it names the slot, so a post tried again gets no second copy where it
/// arrived), transient failures retried until `deadline`. Failures are
/// logged as [`post_to_rooms`] logs them.
pub(crate) async fn post_slot(
    bot: &Bot,
    plain: &str,
    html: &str,
    key: &str,
    deadline: std::time::Instant,
) -> Delivered {
    let delivered = schedule::post(
        bot.broadcast_rooms(),
        RoomMessageEventContent::text_html(plain, html),
        key,
        deadline,
    )
    .await;
    for (room, e) in &delivered.failed {
        error!("Failed to post to {room}: {e}");
    }
    delivered
}

// ── main ──────────────────────────────────────────────────────────────────────

/// Admin-only `!source-stats [hours]`.
async fn source_stats_reply(db: &Db, body: &str) -> String {
    let hours = body
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(24)
        .clamp(1, 24 * 30);
    match db.source_stats_summary(hours).await {
        Ok(stats) if stats.is_empty() => {
            format!("🤷 No source stats in the last {hours}h.")
        }
        Ok(stats) => {
            let mut lines = vec![format!("📊 Source stats, last {hours}h")];
            for stat in stats {
                lines.push(String::new());
                lines.push(format!(
                    "📰 {}\n📥 {} total · 📬 {} queued\n🔎 {} candidate · 🗑️ {} dropped · 🔁 {} retry",
                    stat.source_name,
                    stat.total,
                    stat.queued,
                    stat.candidate,
                    stat.dropped,
                    stat.retry_later
                ));
            }
            lines.join("\n")
        }
        Err(e) => {
            warn!("!source-stats failed: {e}");
            "❌ Source stats query failed.".to_owned()
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    mxbot_common::logging::init("radar_bot");

    let test_mode = std::env::args().any(|a| a == "--test");
    let mut config: Config =
        mxbot_common::config::load_toml(&mxbot_common::config::config_path_from_args())?;
    // `TZ` (as compose sets it), else Europe/Berlin: the digest and forecast times.
    let zone = Zone::resolve(None)?;

    // radar-bot keeps its Matrix sqlite store in store/matrix_store/, next to
    // store/items.db.
    let store_dir = mxbot_common::config::store_path_from_env();
    let bot = Bot::builder("radar-bot", env!("CARGO_PKG_VERSION"))
        .store_path(&store_dir)
        .matrix_store_path(store_dir.join("matrix_store"))
        .admin_help("📊 !source-stats [hours]: items per source")
        .start(&config.matrix, &config.security)
        .await?;

    let db = Db::open(&store_dir.join("items.db"))?;

    bot.client.add_event_handler({
        let bot = bot.clone();
        let db = db.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room, encryption: Option<EncryptionInfo>| {
            let bot = bot.clone();
            let db = db.clone();
            async move {
                if ev.sender == bot.user_id || room.state() != RoomState::Joined {
                    return;
                }
                if bot.admin.handle(&room, &ev, encryption.as_ref()).await == Dispatch::Handled {
                    return;
                }
                let MessageType::Text(text) = &ev.content.msgtype else {
                    return;
                };
                let body = text.body.trim();
                if body.starts_with("!source-stats") {
                    if !bot.is_admin(&ev.sender) {
                        warn!("!source-stats from non-admin {}, ignored", ev.sender);
                        return;
                    }
                    let reply = source_stats_reply(&db, body).await;
                    room.send(RoomMessageEventContent::text_plain(reply))
                        .await
                        .ok();
                }
            }
        }
    });

    info!("Performing initial sync...");
    bot.initial_sync().await;
    info!(
        "Initial sync complete. {} source(s) configured.",
        config.sources.len()
    );

    // Crash recovery: anything left in 'processing' from last run goes back to 'queued'.
    let recovered = db.recover_processing().await?;
    if recovered > 0 {
        warn!(
            "{recovered} item(s) recovered from interrupted digest, will retry at next digest time"
        );
    }

    let geocode_cache: GeocodeCache = Arc::new(DashMap::new());
    let http = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (compatible; radar-bot/0.1)")
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(60))
        .build()?;

    // Resolve reference address to coordinates once at startup.
    let ref_point: Option<(f64, f64)> = if let Some(ref addr) = config.filter.reference_address {
        info!("Geocoding reference address: {addr}");
        match geocode_location(&http, addr, "").await.as_deref() {
            Ok([hit, ..]) => {
                info!("Reference point: {:.5}, {:.5}", hit.lat, hit.lon);
                Some((hit.lat, hit.lon))
            }
            Ok([]) | Err(()) => {
                warn!("Could not geocode reference address '{addr}', distance scoring disabled");
                None
            }
        }
    } else {
        warn!("No reference_address configured, distance scoring disabled");
        None
    };

    if let Some(point) = ref_point {
        // Nominatim's usage policy: at most one request per second.
        sleep(NOMINATIM_INTERVAL).await;
        config.filter.home_borough = reverse_borough(&http, point).await;
        match &config.filter.home_borough {
            Some(borough) => info!("Reference district: {borough}"),
            None => warn!("Could not resolve the reference district, district hints ignored"),
        }
    }

    let bluesky_ctx: Option<sources::BlueskyContext> = match (
        config.bluesky.identifier,
        config.bluesky.password,
    ) {
        (Some(identifier), Some(password)) => {
            info!("Bluesky credentials configured for {identifier}");
            Some(sources::BlueskyContext {
                identifier,
                password,
                session: sources::bluesky::new_shared_session(),
            })
        }
        _ => {
            let has_bluesky = config
                .sources
                .iter()
                .any(|s| matches!(s.source_type, sources::SourceType::Bluesky));
            if has_bluesky {
                warn!("Bluesky source(s) configured but [bluesky] identifier/password missing, searches will fail");
            }
            None
        }
    };
    let dwd_region_keywords = if config.weather.alerts_enabled {
        derive_warning_region_keywords(&config.filter, &config.weather)
    } else {
        Vec::new()
    };
    if config.weather.alerts_enabled && dwd_region_keywords.is_empty() {
        warn!("DWD weather warnings disabled, no weather.warning_region_keywords, geocode_city, or reference_address city found");
    } else if !dwd_region_keywords.is_empty() {
        info!(
            "DWD weather warning region keyword(s): {:?}",
            dwd_region_keywords
        );
    }

    if test_mode {
        // Run twice: run 1 builds the geocode cache (cold), run 2 should hit it (warm).
        // test_mode bypasses the DB seen-check (is_new = true always) and does not write
        // to the DB, so both runs process the same items without affecting state.
        for run in 1..=2u32 {
            let cache_before = geocode_cache.len();
            info!("Test mode: run {run}/2 (geocode cache: {cache_before} entries before)");
            poll_once(
                &bot,
                &http,
                &config.sources,
                &config.filter,
                ref_point,
                &db,
                &geocode_cache,
                bluesky_ctx.as_ref(),
                true,
            )
            .await;
            let cache_after = geocode_cache.len();
            info!("Run {run}/2 done, geocode cache: {cache_before} → {cache_after} entries");

            // In test mode poll_once doesn't write to DB; query what would be queued by
            // re-running take_for_digest on an empty DB (no-op): instead just note it.
            let items = db
                .take_for_digest(config.filter.digest_threshold)
                .await
                .unwrap_or_default();
            if !items.is_empty() {
                let clustered_items = cluster_digest_items(items);
                let chunks = chunk_digest(&clustered_items);
                let total = chunks.len();
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let header = if total > 1 {
                        format!("Test digest run {run} ({}/{})", i + 1, total)
                    } else {
                        format!("Test digest (run {run}/2)")
                    };
                    let (plain, html) = format_digest(&chunk, &header);
                    post_to_rooms(&bot, &plain, &html).await;
                    if total > 1 {
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            } else {
                info!("Run {run}/2: no items passed the filter");
            }
        }
        return Ok(());
    }

    if let Some(job) = digest_job(zone, &config.schedule.digest_times) {
        // A digest's run: the DB, then its chunks retried for SEND_FOR at most.
        let within = job.stall_after(SEND_FOR + Duration::from_secs(10 * 60));
        supervise_ticking("radar digest", within, |alive| {
            digest_loop(
                bot.clone(),
                job,
                db.clone(),
                config.filter.digest_threshold,
                config.filter.candidate_min_sources,
                config.filter.candidate_retention_hours,
                alive,
            )
        });
    }

    if config.emergency.enabled {
        let nina_ags = match ref_point {
            Some(pt) => alerts::lookup_nina_ags(&http, pt).await,
            None => {
                warn!("No reference_address, NINA alerts disabled");
                None
            }
        };
        // A round takes well under two minutes (every request has a timeout).
        let within = Duration::from_secs(3 * (config.emergency.poll_interval_secs + 120));
        supervise_ticking("radar alerts", within, |alive| {
            alerts::alert_loop(
                bot.clone(),
                http.clone(),
                db.clone(),
                alerts::AlertConfig {
                    nina_ags,
                    ref_point,
                    dwd_region_keywords,
                    dwd_lead: Duration::from_secs(config.weather.warning_lead_hours * 3600),
                    max_distance_km: config.emergency.max_distance_km,
                    poll_interval_secs: config.emergency.poll_interval_secs,
                },
                alive,
            )
        });
    }

    if config.weather.enabled {
        if let Some(pt) = ref_point {
            let post_time_str = config
                .weather
                .post_time
                .or_else(|| config.schedule.digest_times.first().cloned())
                .unwrap_or_else(|| "08:00".to_owned());
            let post_time = {
                let mut p = post_time_str.splitn(2, ':');
                let h: u32 = p.next().and_then(|s| s.parse().ok()).unwrap_or(8);
                let m: u32 = p.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                NaiveTime::from_hms_opt(h, m, 0)
                    .unwrap_or_else(|| NaiveTime::from_hms_opt(8, 0, 0).unwrap())
            };
            let post_time = TimeOfDay::new(post_time.hour(), post_time.minute());
            let job = weather::weather_job(zone, post_time);
            // A forecast's run: its fetch (15 s), then the post retried for SEND_FOR.
            let within = job.stall_after(SEND_FOR + Duration::from_secs(5 * 60));
            supervise_ticking("radar weather", within, |alive| {
                weather::weather_loop(
                    bot.clone(),
                    http.clone(),
                    db.clone(),
                    pt,
                    job,
                    config.weather.provider,
                    alive,
                )
            });
        } else {
            warn!("Weather forecast disabled, no reference_address configured");
        }
    }

    // Spawn poll loop. A round with a cold geocode cache can take several minutes
    // (Nominatim allows one request per second), so allow ten per round.
    let within = Duration::from_secs(3 * (config.schedule.poll_interval_minutes + 10) * 60);
    supervise_ticking("radar poll", within, |alive| {
        poll_loop(
            bot.clone(),
            http,
            config.sources,
            config.filter,
            ref_point,
            config.schedule.poll_interval_minutes,
            db,
            geocode_cache,
            bluesky_ctx,
            alive,
        )
    });

    // Continuous Matrix sync
    bot.run().await
}
