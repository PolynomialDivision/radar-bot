mod alerts;
mod config;
mod digest;
mod dry_run;
#[cfg(test)]
mod eval;
mod extract;
mod feed;
mod fetch;
mod filter;
mod geo;
#[cfg(test)]
mod hostile_input;
mod polling;
mod profile;
mod sources;
mod store;
#[cfg(test)]
mod testing;
mod weather;

use crate::config::derive_warning_region_keywords;
use crate::config::Config;
use crate::config::EmergencyConfig;
use crate::config::FilterConfig;
use crate::config::ScheduleConfig;
use crate::config::SourceConfig;
use crate::config::WeatherConfig;
use crate::digest::digest_job;
use crate::digest::digest_loop;
use crate::polling::poll_loop;
use crate::polling::source_verdict;
use crate::polling::PollCtx;
use fetch::Http;
// Reached as `crate::…` by the warning and source modules.
use feed::{parse_feed, parse_feed_date, parse_feed_entries, FeedItem};
use geo::url_encode;
use std::path::PathBuf;
use std::sync::Arc;
use store::Db;

use anyhow::Result;
use mxbot_common::command::{Commands, Parsed, Spec};
use mxbot_common::{
    admin::Dispatch,
    clock::Zone,
    matrix_sdk::{
        deserialized_responses::EncryptionInfo,
        ruma::events::room::message::{
            MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
        },
        Room, RoomState,
    },
    schedule::{self, Delivered},
    supervise::{self, supervise, supervise_ticking, Alive},
    Bot,
};

use tokio::{time::sleep, time::Duration};
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

/// The bot's chat commands (`!admin …` is mxbot-common's).
const COMMANDS: Commands = Commands::flat(&[Spec::new("source-stats", "📊", "items per source")
    .usage("[hours]")
    .admin()]);

/// Admin-only `!source-stats [hours]`: items per source, then the failing and silent ones.
async fn source_stats_reply(db: &Db, args: &str, source_names: &[String]) -> String {
    let mut reply = source_counts_reply(db, args).await;
    match db.source_health().await {
        Ok(health) => {
            let (failing, silent) =
                source_verdict(&health, source_names, chrono::Utc::now().timestamp());
            if !failing.is_empty() {
                reply.push_str(&format!(
                    "\n\n⚠️ Failing {} polls in a row: {}",
                    store::FAILING_AFTER,
                    failing.join(", ")
                ));
            }
            if !silent.is_empty() {
                reply.push_str(&format!(
                    "\n\n🔇 No new item for 30 days: {}",
                    silent.join(", ")
                ));
            }
        }
        Err(e) => warn!("!source-stats: source states failed: {e:#}"),
    }
    reply
}

/// Items per source in the last `args` hours (default 24).
async fn source_counts_reply(db: &Db, args: &str) -> String {
    let hours = args
        .split_whitespace()
        .next()
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

/// The command line: `radar-bot [--dry-run [--fixtures DIR] [--db FILE]] [CONFIG.toml]`, or
/// `radar-bot --check-config CONFIG.toml`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Args {
    pub(crate) config: PathBuf,
    pub(crate) check_config: bool,
    pub(crate) dry_run: bool,
    pub(crate) fixtures: Option<PathBuf>,
    pub(crate) db: Option<PathBuf>,
}

impl Args {
    pub(crate) fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut parsed = Args {
            config: PathBuf::from("config.toml"),
            check_config: false,
            dry_run: false,
            fixtures: None,
            db: None,
        };
        let mut config = None;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--dry-run" => parsed.dry_run = true,
                "--check-config" => parsed.check_config = true,
                "--fixtures" | "--db" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("{arg} needs a path"))?;
                    if arg == "--fixtures" {
                        parsed.fixtures = Some(value.into());
                    } else {
                        parsed.db = Some(value.into());
                    }
                }
                _ if !arg.starts_with('-') && config.is_none() => {
                    config = Some(PathBuf::from(arg));
                }
                _ => anyhow::bail!(
                    "unknown argument {arg:?}; usage: radar-bot [--dry-run [--fixtures DIR] [--db FILE]] [CONFIG.toml] | --check-config CONFIG.toml"
                ),
            }
        }
        if !parsed.dry_run && (parsed.fixtures.is_some() || parsed.db.is_some()) {
            anyhow::bail!("--fixtures and --db only go with --dry-run");
        }
        if parsed.check_config && (parsed.dry_run || config.is_none()) {
            anyhow::bail!("--check-config takes a configuration file and nothing else");
        }
        if let Some(config) = config {
            parsed.config = config;
        }
        Ok(parsed)
    }
}

/// The HTTP client under [`Http`]: no redirects of its own ([`Http`] follows them, one hop at
/// a time), the User-Agent set per request.
pub(crate) fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(60))
        .build()?)
}

/// `--check-config`: the configuration loads and validates; one line per source. No network,
/// no store.
fn check_config(path: &std::path::Path) -> Result<()> {
    let config: Config = mxbot_common::config::load_toml(path)?;
    config.validate()?;
    for source in &config.sources {
        println!("{}", source_line(source));
    }
    println!(
        "{}: {} source(s), valid",
        path.display(),
        config.sources.len()
    );
    Ok(())
}

/// A source for `--check-config`: its type, id and name.
fn source_line(source: &SourceConfig) -> String {
    let kind = match source.source_type {
        sources::SourceType::Rss => "rss",
        sources::SourceType::GoogleNews => "google_news",
        sources::SourceType::Bluesky => "bluesky",
    };
    format!("{kind:<12} {:<32} {}", source.id(), source.name)
}

/// The reference point of `profile` (none: distance scoring off), with its district (Bezirk)
/// in `filter.home_borough`.
pub(crate) fn apply_profile(
    profile: Option<&profile::HomeProfile>,
    filter: &mut FilterConfig,
) -> Option<(f64, f64)> {
    filter.home_borough = profile.and_then(|p| p.borough.clone());
    if profile.is_some() && filter.home_borough.is_none() {
        warn!("The reference district is unknown, district hints ignored");
    }
    profile.map(profile::HomeProfile::point)
}

/// The Bluesky credentials and shared session, when configured.
pub(crate) fn bluesky_context(config: &Config) -> Option<sources::BlueskyContext> {
    match (&config.bluesky.identifier, &config.bluesky.password) {
        (Some(identifier), Some(password)) => {
            info!("Bluesky credentials configured for {identifier}");
            Some(sources::BlueskyContext {
                identifier: identifier.clone(),
                password: password.clone(),
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
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1))?;
    if args.check_config {
        if let Err(e) = check_config(&args.config) {
            eprintln!("{}: {e:#}", args.config.display());
            std::process::exit(1);
        }
        return Ok(());
    }
    if args.dry_run {
        dry_run::init_logging();
        let config: Config = mxbot_common::config::load_toml(&args.config)?;
        config.validate()?;
        return dry_run::run(config, args.fixtures.as_deref(), args.db.as_deref()).await;
    }

    mxbot_common::logging::init("radar_bot");
    let config: Config = mxbot_common::config::load_toml(&args.config)?;
    config.validate()?;
    let source_names: Arc<Vec<String>> =
        Arc::new(config.sources.iter().map(|s| s.name.clone()).collect());
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
    supervise_ticking(
        "radar retention",
        Duration::from_secs(2 * 86_400),
        |alive| retention_loop(db.clone(), alive),
    );

    bot.client.add_event_handler({
        let bot = bot.clone();
        let db = db.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room, encryption: Option<EncryptionInfo>| {
            let bot = bot.clone();
            let db = db.clone();
            let source_names = source_names.clone();
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
                match COMMANDS.parse_as(&text.body, || bot.is_admin(&ev.sender)) {
                    Parsed::Run(command) if command.is("source-stats") => {
                        let reply = source_stats_reply(&db, command.rest, &source_names).await;
                        room.send(RoomMessageEventContent::text_plain(reply))
                            .await
                            .ok();
                    }
                    Parsed::Refused(_) => {
                        warn!("!source-stats from non-admin {}, ignored", ev.sender);
                    }
                    _ => {}
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

    let http = Http::live(http_client()?, config.http.agents());
    let bluesky = bluesky_context(&config);
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

    let address = config.filter.reference_address.clone();
    let loops = Loops {
        bot: bot.clone(),
        http: http.clone(),
        db: db.clone(),
        zone,
        sources: config.sources,
        filter: config.filter,
        schedule: config.schedule,
        weather: config.weather,
        emergency: config.emergency,
        dwd_region_keywords,
        bluesky,
    };
    match address {
        // The loops that measure from the reference point start once it is known.
        Some(address) => {
            supervise("radar profile", async move {
                let alive = supervise::report("radar profile");
                profile::keep_profile(db, http, address, alive, profile::RETRY, move |profile| {
                    loops.start(Some(profile))
                })
                .await
            });
        }
        None => {
            warn!("No reference_address configured: distance scoring, weather and NINA disabled");
            loops.start(None);
        }
    }

    // Continuous Matrix sync
    bot.run().await
}

/// Once a day: delete what has outlived its use ([`Db::apply_retention`]).
async fn retention_loop(db: Db, alive: Alive) {
    const DAY: Duration = Duration::from_secs(86_400);
    alive.expect_every(DAY);
    loop {
        alive.tick();
        match db.apply_retention(chrono::Utc::now().timestamp()).await {
            Ok(n) => {
                if n > 0 {
                    info!("Retention: deleted {n} old row(s)");
                }
                alive.ok();
            }
            Err(e) => {
                warn!("Retention failed: {e:#}");
                alive.failed(format!("{e:#}"));
            }
        }
        sleep(DAY).await;
    }
}

/// The loops that need the reference point (poll, weather, warnings) and what they run with.
struct Loops {
    bot: Bot,
    http: Http,
    db: Db,
    zone: Zone,
    sources: Vec<SourceConfig>,
    filter: FilterConfig,
    schedule: ScheduleConfig,
    weather: WeatherConfig,
    emergency: EmergencyConfig,
    dwd_region_keywords: Vec<String>,
    bluesky: Option<sources::BlueskyContext>,
}

impl Loops {
    fn start(self, profile: Option<profile::HomeProfile>) {
        let Loops {
            bot,
            http,
            db,
            zone,
            sources,
            mut filter,
            schedule,
            weather,
            emergency,
            dwd_region_keywords,
            bluesky,
        } = self;
        let ref_point = apply_profile(profile.as_ref(), &mut filter);

        if emergency.enabled {
            let nina_ags = profile.as_ref().and_then(|p| p.nina_ags.clone());
            if nina_ags.is_none() {
                warn!("No NINA region for the reference address, NINA alerts disabled");
            }
            // A round takes well under two minutes (every request has a timeout).
            let within = Duration::from_secs(3 * (emergency.poll_interval_secs + 120));
            let config = alerts::AlertConfig {
                nina_ags,
                ref_point,
                dwd_region_keywords,
                dwd_lead: Duration::from_secs(weather.warning_lead_hours * 3600),
                max_distance_km: emergency.max_distance_km,
                poll_interval_secs: emergency.poll_interval_secs,
            };
            supervise_ticking("radar alerts", within, |alive| {
                alerts::alert_loop(bot.clone(), http.clone(), db.clone(), config, alive)
            });
        }

        if weather.enabled {
            if let Some(pt) = ref_point {
                // Validated at start.
                let post_time = weather
                    .forecast_time(&schedule)
                    .expect("a valid forecast time");
                let job = weather::weather_job(zone, post_time);
                // A forecast's run: its fetch (15 s), then the post retried for SEND_FOR.
                let within = job.stall_after(SEND_FOR + Duration::from_secs(5 * 60));
                let provider = weather.provider.clone();
                supervise_ticking("radar weather", within, |alive| {
                    weather::weather_loop(
                        bot.clone(),
                        http.clone(),
                        db.clone(),
                        pt,
                        job,
                        provider,
                        alive,
                    )
                });
            } else {
                warn!("Weather forecast disabled, no reference point");
            }
        }

        // A round with a cold geocode cache can take several minutes (Nominatim allows one
        // request per second), so allow ten per round.
        let minutes = schedule.poll_interval_minutes;
        let within = Duration::from_secs(3 * (minutes + 10) * 60);
        let ctx = PollCtx::new(http.clone(), filter, ref_point);
        supervise_ticking("radar poll", within, |alive| {
            poll_loop(ctx, db.clone(), sources, minutes, bluesky, alive)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Result<Args> {
        Args::parse(list.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn the_command_line_takes_a_config_and_the_dry_run_flags_only() {
        assert_eq!(
            args(&["/app/config/config.toml"]).unwrap().config,
            PathBuf::from("/app/config/config.toml")
        );
        assert_eq!(args(&[]).unwrap().config, PathBuf::from("config.toml"));
        let dry = args(&[
            "--dry-run",
            "--fixtures",
            "tests/fixtures",
            "config.toml.example",
        ]);
        assert_eq!(dry.unwrap().config, PathBuf::from("config.toml.example"));
        let dry = args(&["--dry-run", "--fixtures", "fx", "--db", "copy.db", "c.toml"]).unwrap();
        assert!(dry.dry_run);
        assert_eq!(dry.fixtures, Some(PathBuf::from("fx")));
        assert_eq!(dry.db, Some(PathBuf::from("copy.db")));

        assert!(args(&["--test"]).is_err());
        assert!(args(&["a.toml", "b.toml"]).is_err());
        assert!(args(&["--fixtures", "fx"]).is_err());
        assert!(args(&["--dry-run", "--db"]).is_err());
    }

    #[test]
    fn source_stats_is_an_admin_command_with_hours() {
        let run = COMMANDS.parse_as("!source-stats 48", || true);
        let Parsed::Run(command) = run else {
            panic!("{run:?}");
        };
        assert!(command.is("source-stats"));
        assert_eq!(command.rest, "48");
        assert!(matches!(
            COMMANDS.parse_as("!source-stats", || false),
            Parsed::Refused(_)
        ));
        assert_eq!(COMMANDS.parse("!weather"), Parsed::NotOurs);
    }

    #[test]
    fn check_config_validates_without_network_or_store() {
        let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.toml.example");
        assert!(args(&["--check-config", "c.toml"]).unwrap().check_config);
        assert!(args(&["--check-config"]).is_err());
        assert!(args(&["--check-config", "--dry-run", "c.toml"]).is_err());

        check_config(&example).unwrap();

        let typo = std::fs::read_to_string(&example)
            .unwrap()
            .replace("digest_threshold = 2", "digest_treshold = 2");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, typo).unwrap();
        let error = format!("{:#}", check_config(&path).unwrap_err());
        assert!(error.contains("digest_treshold"), "{error}");
    }

    #[test]
    fn check_config_lists_type_id_and_name() {
        let source: SourceConfig = toml::from_str(
            "name = \"Local Police\"\ntype = \"rss\"\nurl = \"https://example.com/rss\"",
        )
        .unwrap();
        let line = source_line(&source);
        assert!(line.starts_with("rss"), "{line}");
        assert!(line.contains("local-police"), "{line}");
        assert!(line.ends_with("Local Police"), "{line}");
    }
}
