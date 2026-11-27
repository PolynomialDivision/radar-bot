//! Official warnings, posted at once: NINA, DWD, and optionally USGS and GDACS.

mod dwd;
mod nina;
mod post;
mod quakes;
#[cfg(test)]
mod tests;

pub(crate) use nina::nina_ags_for_city;

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use mxbot_common::supervise::Alive;
use tokio::time::sleep;
use tracing::warn;

use crate::fetch::{FetchError, Http, Response};
use crate::store::Db;
use dwd::check_dwd_weather_warnings;
use nina::check_nina;
pub(crate) use post::AlertRooms;
use quakes::{check_gdacs, check_usgs};

const DWD_ACTIVE_RECONCILE_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub struct AlertConfig {
    /// The home profile, once known: its point for the DWD cell test, its region for NINA.
    /// Until then DWD runs without the cell test (it fails open) and NINA waits.
    pub home: tokio::sync::watch::Receiver<Option<crate::profile::HomeProfile>>,
    /// DWD warning region keywords, e.g. ["Berlin"]. Empty = skip direct DWD warnings.
    pub dwd_region_keywords: Vec<String>,
    /// How long before its start a DWD warning is posted.
    pub dwd_lead: Duration,
    /// Drop USGS events farther than this from ref_point. None = send all.
    pub max_distance_km: Option<f64>,
    pub poll_interval_secs: u64,
    /// USGS and GDACS, every [`QUAKE_EVERY`] rounds.
    pub quakes: bool,
}

/// USGS and GDACS are asked every this many rounds (30 minutes at the default 120 s).
pub(crate) const QUAKE_EVERY: u64 = 15;

/// Whether round `n` (from 0) asks USGS and GDACS.
pub(crate) fn quake_round(quakes: bool, n: u64) -> bool {
    quakes && n.is_multiple_of(QUAKE_EVERY)
}

pub async fn alert_loop(
    client: impl AlertRooms + Send + 'static,
    http: Http,
    db: Db,
    config: AlertConfig,
    alive: Alive,
) {
    let mut dwd_area_cache: HashMap<String, bool> = HashMap::new();
    let mut last_dwd_reconcile: Option<Instant> = None;

    alive.expect_every(Duration::from_secs(config.poll_interval_secs));
    let mut n: u64 = 0;
    loop {
        alive.tick();
        // The checks that failed this round, for the status file.
        let mut failed: Vec<(&str, String)> = Vec::new();
        let (ref_point, nina_ags) = match config.home.borrow().as_ref() {
            Some(home) => (Some(home.point()), home.nina_ags.clone()),
            None => (None, None),
        };
        if let Some(ref ags) = nina_ags {
            if let Err(e) = check_nina(&client, &http, &db, ags).await {
                warn!("NINA check failed: {e:#}");
                failed.push(("NINA", format!("{e:#}")));
            }
        }
        if !config.dwd_region_keywords.is_empty() {
            let reconcile_active_messages = last_dwd_reconcile
                .is_none_or(|last| last.elapsed() >= DWD_ACTIVE_RECONCILE_INTERVAL);
            if let Err(e) = check_dwd_weather_warnings(
                &client,
                &http,
                &db,
                &config.dwd_region_keywords,
                config.dwd_lead,
                ref_point,
                &mut dwd_area_cache,
                reconcile_active_messages,
            )
            .await
            {
                warn!("DWD weather-warning check failed: {e:#}");
                failed.push(("DWD", format!("{e:#}")));
            } else if reconcile_active_messages {
                last_dwd_reconcile = Some(Instant::now());
            }
        }
        if quake_round(config.quakes, n) {
            if let Err(e) = check_usgs(&client, &http, &db, ref_point, config.max_distance_km).await
            {
                warn!("USGS check failed: {e:#}");
                failed.push(("USGS", format!("{e:#}")));
            }
            if let Err(e) =
                check_gdacs(&client, &http, &db, ref_point, config.max_distance_km).await
            {
                warn!("GDACS check failed: {e:#}");
                failed.push(("GDACS", format!("{e:#}")));
            }
        }
        report_round(&alive, &failed);
        sleep(Duration::from_secs(config.poll_interval_secs)).await;
        n = n.wrapping_add(1);
    }
}

/// How long one request of a warning service may take.
const FETCH_TIMEOUT: Duration = Duration::from_secs(15);

/// `GET url` of a warning service (`what` names it in errors), with the bot's own User-Agent:
/// the answer when it is a success or `304 Not Modified`, else an error.
pub(super) async fn fetch_api(http: &Http, url: &str, what: &str) -> Result<Response> {
    let response = match http.get_api(url, &[], FETCH_TIMEOUT).await {
        Ok(response) => response,
        Err(FetchError::Timeout) => bail!("{what} fetch timed out"),
        Err(e) => bail!("{what} request failed: {e}"),
    };
    ensure_success(response, what)
}

/// `response` when it is a success, else an error naming `what` and the status.
pub(super) fn ensure_success(response: Response, what: &str) -> Result<Response> {
    if !response.is_success() {
        bail!(
            "{what} answered HTTP {} ({})",
            response.status,
            response.content_type()
        );
    }
    Ok(response)
}

/// A round of checks for the status file: ok when every check worked, failed (naming the
/// checks and the first error) otherwise.
fn report_round(alive: &Alive, failed: &[(&str, String)]) {
    let Some((_, first)) = failed.first() else {
        alive.ok();
        return;
    };
    let names: Vec<&str> = failed.iter().map(|(name, _)| *name).collect();
    alive.failed(format!("{} check failed: {first}", names.join(", ")));
}
