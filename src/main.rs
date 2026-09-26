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
use chrono::{Local, NaiveTime};
use mxbot_common::{
    admin::Dispatch,
    config::{MatrixConfig, SecurityConfig},
    matrix_sdk::{
        deserialized_responses::EncryptionInfo,
        ruma::events::room::message::{
            MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
        },
        Room, RoomState,
    },
    Bot,
};

use serde::{Deserialize, Serialize};
use tokio::{task::JoinSet, time::sleep, time::Duration};
use tracing::{error, info, warn};

// ── Posting ───────────────────────────────────────────────────────────────────

/// Returns true if all rooms received the message. On false the caller should
/// requeue the items so they appear in the next digest.
pub(crate) async fn post_to_rooms(bot: &Bot, plain: &str, html: &str) -> bool {
    let mut all_ok = true;
    for room in bot.broadcast_rooms() {
        if let Err(e) = room
            .send(RoomMessageEventContent::text_html(plain, html))
            .await
        {
            error!("Failed to post to {}: {e}", room.room_id());
            all_ok = false;
        }
    }
    all_ok
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
            format!("No source stats recorded in the last {hours}h.")
        }
        Ok(stats) => {
            let mut lines = vec![format!("Source stats, last {hours}h:")];
            for stat in stats {
                lines.push(format!(
                    "{}: {} total, {} queued, {} candidate, {} dropped, {} retry",
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
            "Source stats query failed.".to_owned()
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    mxbot_common::logging::init("radar_bot");

    let test_mode = std::env::args().any(|a| a == "--test");
    let config: Config =
        mxbot_common::config::load_toml(&mxbot_common::config::config_path_from_args())?;

    // radar-bot keeps its Matrix sqlite store in store/matrix_store/, next to
    // store/items.db.
    let store_dir = mxbot_common::config::store_path_from_env();
    let bot = Bot::builder("radar-bot", env!("CARGO_PKG_VERSION"))
        .store_path(&store_dir)
        .matrix_store_path(store_dir.join("matrix_store"))
        .admin_help("!source-stats [hours] — per-source item counts")
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
                        warn!("!source-stats from non-admin {} — ignored", ev.sender);
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
        warn!("{recovered} item(s) recovered from interrupted digest — will retry at next digest time");
    }

    let geocode_cache: GeocodeCache = Arc::new(DashMap::new());
    let http = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (compatible; radar-bot/0.1)")
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
                warn!("Could not geocode reference address '{addr}' — distance scoring disabled");
                None
            }
        }
    } else {
        warn!("No reference_address configured — distance scoring disabled");
        None
    };

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
                warn!("Bluesky source(s) configured but [bluesky] identifier/password missing — searches will fail");
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
        warn!("DWD weather warnings disabled — no weather.warning_region_keywords, geocode_city, or reference_address city found");
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
            info!("Run {run}/2 done — geocode cache: {cache_before} → {cache_after} entries");

            // In test mode poll_once doesn't write to DB; query what would be queued by
            // re-running take_for_digest on an empty DB (no-op) — instead just note it.
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

    tokio::spawn(digest_loop(
        bot.clone(),
        config.schedule.digest_times.clone(),
        db.clone(),
        config.filter.digest_threshold,
        config.filter.candidate_min_sources,
        config.filter.candidate_retention_hours,
    ));

    if config.emergency.enabled {
        let nina_ags = match ref_point {
            Some(pt) => alerts::lookup_nina_ags(&http, pt).await,
            None => {
                warn!("No reference_address — NINA alerts disabled");
                None
            }
        };
        tokio::spawn(alerts::alert_loop(
            bot.clone(),
            http.clone(),
            db.clone(),
            alerts::AlertConfig {
                nina_ags,
                ref_point,
                dwd_region_keywords,
                max_distance_km: config.emergency.max_distance_km,
                poll_interval_secs: config.emergency.poll_interval_secs,
            },
        ));
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
            tokio::spawn(weather::weather_loop(
                bot.clone(),
                http.clone(),
                db.clone(),
                pt,
                post_time,
                config.weather.provider,
            ));
        } else {
            warn!("Weather forecast disabled — no reference_address configured");
        }
    }

    // Spawn poll loop
    tokio::spawn(poll_loop(
        bot.clone(),
        http,
        config.sources,
        config.filter,
        ref_point,
        config.schedule.poll_interval_minutes,
        db,
        geocode_cache,
        bluesky_ctx,
    ));

    // Continuous Matrix sync
    bot.run().await
}
