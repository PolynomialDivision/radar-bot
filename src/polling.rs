//! Polling sources and scoring/queueing new items.

// ── Polling loop ──────────────────────────────────────────────────────────────

use crate::geo::Geocoder;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use mxbot_common::supervise::Alive;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::{FilterConfig, SourceConfig};
use crate::extract::article::{fetch_article, Page};
use crate::feed::FeedItem;
use crate::fetch::Http;
use crate::filter::{keyword_check, normalize, ItemText, TermMatch};
use crate::geo::{
    best_evidence, find_configured_places, find_nearest_distance, format_distance,
    haversine_meters, DistanceLookup, EvidenceKind, LocationEvidence, HEARTBEAT_INTERVAL,
};
use crate::sources::SourceError;
use crate::store::Db;
use crate::{sources, store};

/// After this many polls ending in "retry later" (article page or geocoder
/// unreachable), an item is decided with the information at hand, with the
/// default 30-minute interval that is three hours.
pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 6;

pub(crate) enum ProcessOutcome {
    Queued(FeedItem),
    Candidate { item: FeedItem, reason: String },
    Dropped { item: FeedItem, reason: String },
    Retry { item: FeedItem, reason: String },
}

/// What a poll round and its item tasks work with. Cheap to clone.
#[derive(Clone)]
pub(crate) struct PollCtx {
    pub(crate) http: Http,
    pub(crate) filter: Arc<FilterConfig>,
    pub(crate) ref_point: Option<(f64, f64)>,
    pub(crate) geocoder: Geocoder,
}

impl PollCtx {
    /// Street lookups are anchored to `filter.geocode_city`; their cache lives as long as the
    /// context.
    pub(crate) fn new(http: Http, filter: FilterConfig, ref_point: Option<(f64, f64)>) -> Self {
        let geocoder = Geocoder::new(http.clone(), filter.geocode_city.as_deref().unwrap_or(""));
        PollCtx {
            http,
            filter: Arc::new(filter),
            ref_point,
            geocoder,
        }
    }
}

/// Process a single feed item: fetch article, keyword-check, geocode, score.
pub(crate) async fn process_item(
    ctx: PollCtx,
    source: SourceConfig,
    mut item: FeedItem,
    final_attempt: bool,
) -> ProcessOutcome {
    let PollCtx {
        http,
        filter,
        ref_point,
        geocoder,
    } = ctx;
    let source_name = source.name.clone();
    item.deliver_always = !source.filter;

    // Fetch article body (used for filtering + geocoding street extraction).
    // extract_article_body() isolates <article>/<main> to avoid navigation false-positives.
    let needs_article = source.filter || source.base_implied_meters.is_some();
    let mut article_fetch_failed = false;
    if needs_article && item.article_text.is_none() && !item.paywalled {
        if let Some(ref url) = item.link {
            tracing::debug!("fetching article [{}] {:?}", source_name, item.title);
            match fetch_article(&http, url, &item.title).await {
                Page::Article(article) => {
                    item.article_text = Some(article.text);
                    // An old article resurfacing in a feed (or a Google News
                    // search) often carries a fresh feed date; trust the older.
                    item.coords = item.coords.or(article.event.coords);
                    item.venue = article.event.address;
                    item.district = item.district.take().or(article.district);
                    if let Some(page_date) = article.published_at {
                        item.published_at = Some(
                            item.published_at
                                .map_or(page_date, |feed| feed.min(page_date)),
                        );
                    }
                    if let Some(link) = article.resolved_link {
                        item.link = Some(link);
                    }
                    item.link_note = article.link_note;
                }
                Page::NotAnArticle(content_type) => {
                    tracing::debug!(
                        "link is no article page [{}] {:?}: {content_type}",
                        source_name,
                        item.title
                    );
                }
                Page::Failed => {
                    article_fetch_failed = true;
                }
            }
            if article_fetch_failed {
                tracing::debug!(
                    "article fetch failed/empty [{}] {:?}",
                    source_name,
                    item.title
                );
            }
            if article_fetch_failed && final_attempt {
                // Retried often enough: decide with feed title/description.
                info!(
                    "  giving up on article fetch [{}] {:?}",
                    source_name, item.title
                );
                article_fetch_failed = false;
            }
        }
    }

    // ── 0. Age, now that the page may have told us the real publish date ────
    let max_age_hours = source.max_age_hours.or(filter.max_age_hours);
    if let (Some(max_age), Some(published)) = (max_age_hours, item.published_at) {
        let age_hours = (chrono::Utc::now().timestamp() - published) / 3600;
        if age_hours > max_age as i64 {
            let reason = format!("published {age_hours}h ago, older than {max_age}h");
            tracing::debug!("DROP [{}] {:?}: {reason}", source_name, item.title);
            return ProcessOutcome::Dropped { item, reason };
        }
    }

    let text = ItemText::of(&item);
    let mut evidence: Vec<LocationEvidence> = Vec::new();

    // ── 1. Keyword check → implied distance ───────────────────────────────────
    let effective_required: &[String] = source.required.as_deref().unwrap_or(&filter.required);
    if source.filter {
        match keyword_check(&item, &filter, effective_required) {
            None => {
                let reason = "required/blocklist keyword filter did not pass";
                if article_fetch_failed {
                    tracing::debug!(
                        "RETRY [{}] {:?}: article fetch failed before keyword decision",
                        source_name,
                        item.title
                    );
                    return ProcessOutcome::Retry {
                        item,
                        reason: "article fetch failed before keyword decision".to_owned(),
                    };
                }
                tracing::debug!("DROP [{}] {:?}: {reason}", source_name, item.title);
                return ProcessOutcome::Dropped {
                    item,
                    reason: reason.to_owned(),
                };
            }
            Some(kw) => {
                if !kw.matched.is_empty() {
                    info!(
                        "PASS [{}] {:?} terms=[{}]",
                        source_name,
                        item.title,
                        kw.matched.join(", ")
                    );
                }
                evidence.extend(kw.evidence);
            }
        }
    }
    if let Some(meters) = source.base_implied_meters {
        evidence.push(LocationEvidence {
            meters,
            label: String::new(),
            kind: EvidenceKind::SourceDefault,
            prominent: true,
            penalty: 0,
        });
    }

    // ── 2. Geocode → actual distance ──────────────────────────────────────────
    if let Some(point) = ref_point {
        match find_nearest_distance(&geocoder, &item, &text, point).await {
            DistanceLookup::Found(found) => {
                info!(
                    "  📍 [{}] {:?} → {} at {}{}",
                    source_name,
                    found.label,
                    item.title,
                    format_distance(found.meters),
                    if found.prominent { "" } else { " (body only)" }
                );
                evidence.push(found);
            }
            DistanceLookup::NoMatch => {
                tracing::debug!("  no street geocoded [{}] {:?}", source_name, item.title);
            }
            DistanceLookup::TransientFailure if final_attempt => {
                info!(
                    "  giving up on geocoding [{}] {:?}",
                    source_name, item.title
                );
            }
            DistanceLookup::TransientFailure => {
                return ProcessOutcome::Retry {
                    item,
                    reason: "geocode transient failure before distance decision".to_owned(),
                };
            }
        }
    }

    if let (Some((lat, lon)), Some((ref_lat, ref_lon))) = (item.coords, ref_point) {
        evidence.push(LocationEvidence {
            meters: haversine_meters(ref_lat, ref_lon, lat, lon),
            label: item
                .venue
                .clone()
                .unwrap_or_else(|| "source coordinates".to_owned()),
            kind: EvidenceKind::Coordinates,
            prominent: true,
            penalty: 0,
        });
    }

    for place in find_configured_places(&text, &filter, ref_point) {
        info!(
            "  📍 [{}] configured place {:?} → {} at {}{}",
            source_name,
            place.label,
            item.title,
            format_distance(place.meters),
            if place.prominent { "" } else { " (body only)" }
        );
        evidence.push(place);
    }

    if let Some(why) = located_elsewhere(&item, &filter) {
        info!("  ↘ [{}] {:?}: {why}", source_name, item.title);
        for e in evidence.iter_mut().filter(|e| e.is_textual()) {
            e.penalty += 1;
        }
    }

    // ── 3. Final score ────────────────────────────────────────────────────────
    // The strongest single piece of evidence decides. A source default only
    // sets a floor: it neither shows as a location nor makes two city-wide
    // items look like they happened at the same place.
    item.evidence = evidence.clone();
    let best = best_evidence(evidence);
    let score = best.as_ref().map_or(0, LocationEvidence::score);
    item.score = score;
    item.max_score = 5;
    item.distance_meters = best.as_ref().filter(|e| e.is_measured()).map(|e| e.meters);
    item.location_label = best
        .filter(|e| e.kind != EvidenceKind::SourceDefault)
        .map(|e| e.label);

    if source.filter && score < filter.digest_threshold {
        let reason = format!(
            "score {score} below digest threshold {}",
            filter.digest_threshold
        );
        if article_fetch_failed {
            tracing::debug!(
                "RETRY [{}] {:?}: article fetch failed and {reason}",
                source_name,
                item.title
            );
            return ProcessOutcome::Retry {
                item,
                reason: format!("article fetch failed and {reason}"),
            };
        }
        if score >= filter.candidate_min_score && score > 0 {
            tracing::debug!("CANDIDATE [{}] {:?}: {reason}", source_name, item.title);
            return ProcessOutcome::Candidate { item, reason };
        }
        tracing::debug!("DROP [{}] {:?}: {reason}", source_name, item.title);
        return ProcessOutcome::Dropped { item, reason };
    }

    info!("QUEUE [{}] {:?} score={}", source_name, item.title, score);
    ProcessOutcome::Queued(item)
}
/// District labels that do not point anywhere in particular.
const NON_SPECIFIC_DISTRICTS: &[&str] = &["bezirksübergreifend", "berlinweit", "landesweit"];

/// Why the item is probably not about the neighbourhood, if it is: the
/// source files it under another Bezirk, or its title names an `elsewhere`
/// place and none of the configured own areas or places.
pub(crate) fn located_elsewhere(item: &FeedItem, filter: &FilterConfig) -> Option<String> {
    if let (Some(district), Some(home)) = (item.district.as_deref(), filter.home_borough.as_deref())
    {
        let district_n = normalize(district);
        let specific = !NON_SPECIFIC_DISTRICTS
            .iter()
            .any(|d| district_n.contains(&normalize(d)));
        if specific && district_n.trim() != normalize(home).trim() {
            return Some(format!("filed under {district}, not {home}"));
        }
    }

    let title = ItemText {
        prominent: normalize(&item.title),
        rest: String::new(),
    };
    let other = filter
        .elsewhere
        .iter()
        .find(|term| title.mention(term, TermMatch::Place).is_some())?;
    let own_terms = filter.area.iter().flat_map(|group| &group.terms).chain(
        filter
            .places
            .iter()
            .flat_map(|p| std::iter::once(&p.name).chain(&p.aliases)),
    );
    let names_own = own_terms
        .into_iter()
        .any(|term| title.mention(term, TermMatch::Place).is_some());
    (!names_own).then(|| format!("title names {other}"))
}

/// One poll round. `decide_now` decides every item in this round, as on its last retry
/// (`--dry-run`, which has no next round).
pub(crate) async fn poll_once(
    ctx: &PollCtx,
    db: &Db,
    sources: &[SourceConfig],
    bluesky: Option<&sources::BlueskyContext>,
    decide_now: bool,
) -> PollSummary {
    let http = &ctx.http;
    let mut summary = PollSummary {
        sources: sources.len(),
        ..PollSummary::default()
    };

    // Fetch all feeds concurrently and collect new items, then process them in parallel.
    // Seen-check stays sequential to avoid races; article fetch + geocoding run concurrently.
    let mut pending: Vec<(SourceConfig, FeedItem)> = Vec::new();
    let mut total_by_source: HashMap<String, usize> = HashMap::new();
    let mut source_tasks: JoinSet<(SourceConfig, Result<sources::Fetched, SourceError>)> =
        JoinSet::new();
    let bluesky_owned = bluesky.cloned();

    for source in sources {
        let source = source.clone();
        let http = http.clone();
        let bluesky = bluesky_owned.clone();
        source_tasks.spawn(async move {
            let items = sources::build_adapter(&source, bluesky.as_ref())
                .fetch_items(&http)
                .await;
            (source, items)
        });
    }

    let now = chrono::Utc::now().timestamp();
    let mut results = Vec::with_capacity(sources.len());
    while let Some(res) = source_tasks.join_next().await {
        match res {
            Ok(v) => results.push(v),
            Err(e) => {
                warn!("Source fetch task panicked: {e}");
                summary.panicked += 1;
            }
        }
    }
    // Every source unreachable: the network is down, not the sources. The round does not
    // count against them.
    let unreachable = !results.is_empty()
        && summary.panicked == 0
        && results
            .iter()
            .all(|(_, result)| matches!(result, Err(SourceError::Unavailable(_))));
    if unreachable {
        warn!("No source reachable this round");
        mxbot_common::supervise::report("radar sources").failed("no source reachable");
        return summary;
    }
    for (source, result) in results {
        let fetched = match result {
            Ok(items) => items,
            Err(e) => {
                warn!("Source '{}' failed: {e}", source.name);
                if let Err(e) = db
                    .record_source_round(&source.name, Err(e.to_string()), now)
                    .await
                {
                    warn!("Failed to record the state of '{}': {e:#}", source.name);
                }
                continue;
            }
        };
        let newest_entry_at = fetched.newest_entry_at;
        let items = fetched.items;
        *total_by_source.entry(source.name.clone()).or_default() += items.len();
        summary.fetched += items.len();

        let guids = items.iter().map(|item| item.guid.clone()).collect();
        let seen = match db.seen_by_source(&source.id(), guids).await {
            Ok(seen) => seen,
            Err(e) => {
                // Without the seen-set every item would look new: skip the source this round.
                warn!(
                    "Skipping '{}' this round, the seen-check failed: {e:#}",
                    source.name
                );
                continue;
            }
        };
        let new: Vec<FeedItem> = items
            .into_iter()
            .filter(|item| !seen.contains(&item.guid))
            .collect();
        if let Err(e) = db
            .record_source_round(&source.name, Ok(new.len()), now)
            .await
        {
            warn!("Failed to record the state of '{}': {e:#}", source.name);
        }
        if let Some(newest) = newest_entry_at {
            if let Err(e) = db.record_newest_entry(&source.name, newest).await {
                warn!(
                    "Failed to record the newest entry of '{}': {e:#}",
                    source.name
                );
            }
        }
        pending.extend(new.into_iter().map(|item| (source.clone(), item)));
    }
    report_source_health(db, sources, now).await;

    // Spawn one task per item: article fetches and geocoding run in parallel.
    // Geocoding tasks share the NominatimLimiter so total Nominatim traffic stays ≤ 1 req/s.
    let mut tasks: JoinSet<ProcessOutcome> = JoinSet::new();
    // What a task is about, for when it panics and returns nothing.
    let mut in_task: HashMap<tokio::task::Id, (FeedItem, u32)> = HashMap::new();
    let n_pending = pending.len();
    summary.new_items = n_pending;
    // For the observations: each item's source, and what the gate would do with its page
    // (judged on the feed item, before any page is read).
    let by_name: HashMap<String, SourceConfig> = sources
        .iter()
        .map(|s| (s.name.clone(), s.clone()))
        .collect();
    let mut gates: HashMap<(String, String), &'static str> = HashMap::new();
    for (source, item) in pending {
        gates.insert(
            (source.name.clone(), item.guid.clone()),
            crate::pipeline::gate(&source, &item, &ctx.filter),
        );
        let attempts = db.retry_attempts(&item.guid).await.unwrap_or(0);
        let about = item.clone();
        let handle = tasks.spawn(process_item(
            ctx.clone(),
            source,
            item,
            decide_now || attempts >= MAX_RETRY_ATTEMPTS,
        ));
        in_task.insert(handle.id(), (about, attempts));
    }
    if n_pending > 0 {
        info!("Processing {} new item(s) in parallel...", n_pending);
    }

    // Heartbeat: log progress every 5s so the bot doesn't appear frozen during geocoding.
    let n_done_shared = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ticker = {
        let cache = ctx.geocoder.cache.clone();
        let n_done_shared = n_done_shared.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                let done = n_done_shared.load(std::sync::atomic::Ordering::Relaxed);
                let cache_n = cache.len();
                info!(
                    "  ... {}/{} items done, {} geocode cache entries",
                    done, n_pending, cache_n
                );
            }
        })
    };

    let mut passed_by_source: HashMap<String, usize> = HashMap::new();
    let mut candidate_by_source: HashMap<String, usize> = HashMap::new();
    let mut dropped_by_source: HashMap<String, usize> = HashMap::new();
    let mut retried_by_source: HashMap<String, usize> = HashMap::new();
    let mut candidate_reasons: HashMap<String, usize> = HashMap::new();
    let mut drop_reasons: HashMap<String, usize> = HashMap::new();
    let mut retry_reasons: HashMap<String, usize> = HashMap::new();

    while let Some(res) = tasks.join_next_with_id().await {
        n_done_shared.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match res.map(|(_, outcome)| outcome) {
            Ok(ProcessOutcome::Queued(item)) => {
                *passed_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                db.insert_queued(&item).await.ok();
                observe(db, &by_name, &gates, &item).await;
            }
            Ok(ProcessOutcome::Candidate { item, reason }) => {
                *candidate_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *candidate_reasons.entry(reason.clone()).or_default() += 1;
                db.insert_candidate(&item, &reason).await.ok();
                observe(db, &by_name, &gates, &item).await;
            }
            Ok(ProcessOutcome::Dropped { item, reason }) => {
                *dropped_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *drop_reasons.entry(reason.clone()).or_default() += 1;
                db.insert_dropped(
                    &item.guid,
                    &item.source_name,
                    &item.title,
                    item.link.as_deref(),
                    &reason,
                )
                .await
                .ok();
                observe(db, &by_name, &gates, &item).await;
            }
            Ok(ProcessOutcome::Retry { item, reason }) => {
                *retried_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *retry_reasons.entry(reason).or_default() += 1;
                db.record_retry(&item.guid).await.ok();
            }
            Err(e) => {
                warn!("Item task panicked: {e}");
                if let Some((item, attempts)) = in_task.remove(&e.id()) {
                    item_panicked(db, &item, attempts, decide_now).await;
                }
            }
        }
    }
    ticker.abort();

    let mut source_stats = Vec::new();
    for source in sources {
        let total = total_by_source.get(&source.name).copied().unwrap_or(0);
        let passed = passed_by_source.get(&source.name).copied().unwrap_or(0);
        let candidates = candidate_by_source.get(&source.name).copied().unwrap_or(0);
        let dropped = dropped_by_source.get(&source.name).copied().unwrap_or(0);
        let retried = retried_by_source.get(&source.name).copied().unwrap_or(0);
        if source.filter {
            info!("Source '{}': {total} in feed, {passed} passed, {candidates} candidate, {dropped} dropped, {retried} retry-later", source.name);
        } else {
            info!("Source '{}': {total} in feed, {passed} new", source.name);
        }
        source_stats.push(store::SourcePollStat {
            source_name: source.name.clone(),
            total,
            queued: passed,
            candidate: candidates,
            dropped,
            retry_later: retried,
        });
    }
    if let Err(e) = db.record_source_stats(source_stats).await {
        warn!("Failed to record source stats: {e}");
    }
    for (reason, count) in candidate_reasons {
        info!("Candidate reason: {count} item(s): {reason}");
    }
    for (reason, count) in drop_reasons {
        info!("Drop reason: {count} item(s): {reason}");
    }
    for (reason, count) in retry_reasons {
        info!("Retry reason: {count} item(s): {reason}");
    }
    summary
}

/// Writes the observation of a decided item (S2: the shadow decision next to the digest).
async fn observe(
    db: &Db,
    by_name: &HashMap<String, SourceConfig>,
    gates: &HashMap<(String, String), &'static str>,
    item: &FeedItem,
) {
    let Some(source) = by_name.get(&item.source_name) else {
        return;
    };
    let gate = gates
        .get(&(item.source_name.clone(), item.guid.clone()))
        .copied()
        .unwrap_or("fetch");
    let observation =
        crate::pipeline::observation(source, item, gate, chrono::Utc::now().timestamp());
    if let Err(e) = db.insert_observation(observation).await {
        warn!("Failed to store the observation of {:?}: {e:#}", item.title);
    }
}

/// An item whose task panicked: counted like a retry, and after [`MAX_RETRY_ATTEMPTS`] (or in
/// a round that decides everything) recorded as dropped, so that it is not processed again.
async fn item_panicked(db: &Db, item: &FeedItem, attempts: u32, decide_now: bool) {
    if let Err(e) = db.record_retry(&item.guid).await {
        warn!("Failed to count the panic of {:?}: {e:#}", item.title);
    }
    if decide_now || attempts + 1 >= MAX_RETRY_ATTEMPTS {
        db.insert_dropped(
            &item.guid,
            &item.source_name,
            &item.title,
            item.link.as_deref(),
            "panicked while processing",
        )
        .await
        .ok();
    }
}

/// The configured sources that are failing (with their last error), and those that are silent.
pub(crate) fn source_verdict(
    health: &[store::SourceHealth],
    names: &[String],
    now: i64,
) -> (Vec<String>, Vec<String>) {
    let configured: HashSet<&str> = names.iter().map(String::as_str).collect();
    let mine = || {
        health
            .iter()
            .filter(|h| configured.contains(h.source_name.as_str()))
    };
    let failing = mine()
        .filter(|h| h.is_failing())
        .map(|h| {
            format!(
                "{} ({})",
                h.source_name,
                h.last_error.as_deref().unwrap_or("no error recorded")
            )
        })
        .collect();
    let silent = mine()
        .filter(|h| h.is_silent(now))
        .map(|h| h.source_name.clone())
        .collect();
    (failing, silent)
}

/// A feed whose newest entry is older than this many days is stale, unless the source says.
pub(crate) const STALE_AFTER_DAYS: u64 = 45;

/// How a source is doing, as the admins are told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceState {
    Ok,
    /// Answers, but nothing new to the bot for a month.
    Silent,
    /// The newest entry it lists is older than its `stale_after_days`.
    Stale,
    /// Failed [`store::FAILING_AFTER`] polls in a row.
    Failing,
}

impl SourceState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SourceState::Ok => "ok",
            SourceState::Silent => "silent",
            SourceState::Stale => "stale",
            SourceState::Failing => "failing",
        }
    }

    fn parse(stored: Option<&str>) -> Self {
        match stored {
            Some("silent") => SourceState::Silent,
            Some("stale") => SourceState::Stale,
            Some("failing") => SourceState::Failing,
            // Never reported: as if it was fine.
            _ => SourceState::Ok,
        }
    }
}

/// Whether a source is a search, whose answers are old or empty by nature.
fn is_search(source: &SourceConfig) -> bool {
    !matches!(source.source_type, sources::SourceType::Rss)
}

/// The state of `source` from its health and the newest entry it lists.
pub(crate) fn source_state(
    source: &SourceConfig,
    health: &store::SourceHealth,
    newest_entry_at: Option<i64>,
    now: i64,
) -> SourceState {
    let stale_after = source.stale_after_days.unwrap_or(STALE_AFTER_DAYS) as i64 * 86_400;
    if health.is_failing() {
        SourceState::Failing
    } else if !is_search(source) && newest_entry_at.is_some_and(|t| now - t > stale_after) {
        SourceState::Stale
    } else if health.is_silent(now) {
        SourceState::Silent
    } else {
        SourceState::Ok
    }
}

/// The admin notice for `source` going from `before` to `now`; `None` when there is nothing
/// to say (no change, or a search going quiet, which searches do).
pub(crate) fn source_notice(
    source: &SourceConfig,
    before: SourceState,
    now: SourceState,
    health: &store::SourceHealth,
    newest_entry_at: Option<i64>,
) -> Option<String> {
    let name = &source.name;
    match now {
        _ if now == before => None,
        SourceState::Failing => Some(format!(
            "⚠️ Quelle „{name}“ fällt seit {} Abrufen aus: {}",
            health.consecutive_failures,
            health.last_error.as_deref().unwrap_or("unbekannter Fehler")
        )),
        SourceState::Stale => {
            let since = newest_entry_at
                .and_then(|t| chrono::DateTime::from_timestamp(t, 0))
                .map(|t| {
                    t.with_timezone(&chrono::Local)
                        .format("%d.%m.%Y")
                        .to_string()
                })
                .unwrap_or_default();
            Some(format!(
                "🕸️ Quelle „{name}“ ist veraltet: der neueste Eintrag ist vom {since}"
            ))
        }
        SourceState::Silent if is_search(source) => None,
        SourceState::Silent => Some(format!(
            "🔇 Quelle „{name}“ hat seit 30 Tagen nichts Neues geliefert"
        )),
        SourceState::Ok => Some(format!("✅ Quelle „{name}“ läuft wieder")),
    }
}

/// A configured source with what is known about it.
pub(crate) struct SourceReport {
    pub(crate) source: SourceConfig,
    pub(crate) state: SourceState,
    pub(crate) health: store::SourceHealth,
    pub(crate) newest_entry_at: Option<i64>,
    /// The state last reported to the admins.
    pub(crate) reported: SourceState,
}

/// The current state of every configured source that was polled, in configuration order.
pub(crate) async fn source_reports(
    db: &Db,
    sources: &[SourceConfig],
    now: i64,
) -> anyhow::Result<Vec<SourceReport>> {
    let health = db.source_health().await?;
    let reports = db.source_reports().await?;
    Ok(sources
        .iter()
        .filter_map(|source| {
            let health = health
                .iter()
                .find(|h| h.source_name == source.name)?
                .clone();
            let (newest_entry_at, reported) =
                reports.get(&source.name).cloned().unwrap_or_default();
            Some(SourceReport {
                state: source_state(source, &health, newest_entry_at, now),
                source: source.clone(),
                health,
                newest_entry_at,
                reported: SourceState::parse(reported.as_deref()),
            })
        })
        .collect())
}

/// After a round: the notices for the admins about sources whose state changed since it was
/// last reported, and the new state stored as reported (also when it is not worth a notice).
pub(crate) async fn source_notices(db: &Db, sources: &[SourceConfig], now: i64) -> Vec<String> {
    let reports = match source_reports(db, sources, now).await {
        Ok(reports) => reports,
        Err(e) => {
            warn!("Failed to read the source states for the admins: {e:#}");
            return Vec::new();
        }
    };
    let mut notices = Vec::new();
    for report in reports.iter().filter(|r| r.state != r.reported) {
        notices.extend(source_notice(
            &report.source,
            report.reported,
            report.state,
            &report.health,
            report.newest_entry_at,
        ));
        if let Err(e) = db
            .set_reported(&report.source.name, report.state.as_str())
            .await
        {
            warn!(
                "Failed to store the reported state of '{}': {e:#}",
                report.source.name
            );
        }
    }
    notices
}

/// The names of the sources of `reports` in `state`.
pub(crate) fn names_in(reports: &[SourceReport], state: SourceState) -> Vec<String> {
    reports
        .iter()
        .filter(|r| r.state == state)
        .map(|r| r.source.name.clone())
        .collect()
}

/// The weekly line for the admins: the sources that are not ok, or `None` when all are.
pub(crate) async fn weekly_source_summary(
    db: &Db,
    sources: &[SourceConfig],
    now: i64,
) -> anyhow::Result<Option<String>> {
    let reports = source_reports(db, sources, now).await?;
    let parts: Vec<String> = [
        (SourceState::Failing, "fallen aus"),
        (SourceState::Stale, "veraltet"),
        (SourceState::Silent, "still"),
    ]
    .into_iter()
    .filter_map(|(state, label)| {
        let names = names_in(&reports, state);
        (!names.is_empty()).then(|| format!("{label}: {}", names.join(", ")))
    })
    .collect();
    Ok((!parts.is_empty()).then(|| format!("📡 Quellen diese Woche · {}", parts.join(" · "))))
}

/// After a round: `radar sources` fails while a source has failed
/// [`store::FAILING_AFTER`] polls in a row, naming it; the gauge `radar_sources_silent`
/// counts the sources that answer without new items for a month.
async fn report_source_health(db: &Db, sources: &[SourceConfig], now: i64) {
    let health = match db.source_health().await {
        Ok(health) => health,
        Err(e) => {
            warn!("Failed to read the source states: {e:#}");
            return;
        }
    };
    let names: Vec<String> = sources.iter().map(|s| s.name.clone()).collect();
    let (failing, silent) = source_verdict(&health, &names, now);
    mxbot_common::status::gauge("radar_sources_silent").set(silent.len() as f64);
    let alive = mxbot_common::supervise::report("radar sources");
    if failing.is_empty() {
        alive.ok();
    } else {
        alive.failed(format!(
            "{} source(s) failing {} polls in a row: {}",
            failing.len(),
            store::FAILING_AFTER,
            failing.join(", ")
        ));
    }
}

/// What a poll round saw, for the status file (the round does not depend on it).
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PollSummary {
    /// Sources asked.
    pub sources: usize,
    /// Items in all feeds together.
    pub fetched: usize,
    /// Items not seen before.
    pub new_items: usize,
    /// Source fetches that panicked.
    pub panicked: usize,
}

impl PollSummary {
    /// Failed when a fetch panicked or no source returned anything (feeds are never all
    /// empty: the network or the sources are down; a source's own errors are logged and
    /// return no items), ok with new items, idle without.
    pub(crate) fn report(&self, alive: &Alive) {
        if self.panicked > 0 {
            alive.failed(format!("{} source fetch(es) panicked", self.panicked));
        } else if self.sources > 0 && self.fetched == 0 {
            alive.failed("no source returned any item");
        } else if self.new_items > 0 {
            alive.ok();
        } else {
            alive.idle();
        }
    }
}

/// What the poll loop is connected to besides the store.
#[derive(Default)]
pub(crate) struct PollLinks {
    /// The home profile as the profile task publishes it: a later one (its district, a
    /// refresh) is taken over before the next round.
    pub(crate) home: Option<tokio::sync::watch::Receiver<Option<crate::profile::HomeProfile>>>,
    /// Notices for the admins (a source that fails, goes stale or recovers).
    pub(crate) admins: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

/// Poll every `interval_mins`.
pub(crate) async fn poll_loop(
    mut ctx: PollCtx,
    db: Db,
    sources: Vec<SourceConfig>,
    interval_mins: u64,
    bluesky: Option<sources::BlueskyContext>,
    mut links: PollLinks,
    alive: Alive,
) {
    let interval = Duration::from_secs(interval_mins * 60);
    alive.expect_every(interval);
    loop {
        alive.tick();
        let changed = links
            .home
            .as_mut()
            .filter(|u| u.has_changed().unwrap_or(false));
        if let Some(updates) = changed {
            let profile = updates.borrow_and_update().clone();
            let mut filter = (*ctx.filter).clone();
            let ref_point = crate::apply_profile(profile.as_ref(), &mut filter);
            info!("Home profile updated, taken over for this round");
            ctx = PollCtx::new(ctx.http.clone(), filter, ref_point);
        }
        poll_once(&ctx, &db, &sources, bluesky.as_ref(), false)
            .await
            .report(&alive);
        let notices = source_notices(&db, &sources, chrono::Utc::now().timestamp()).await;
        if let Some(admins) = &links.admins {
            for notice in notices {
                admins.send(notice).ok();
            }
        }
        info!("Next poll in {interval_mins}m");
        sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AreaGroup;
    use crate::config::FilterConfig;
    use crate::config::SourceConfig;
    use crate::feed::FeedItem;
    use crate::fetch::Http;

    use crate::store;
    use crate::store::Db;

    use std::sync::Arc;

    #[test]
    fn poll_rounds_are_reported_for_the_status_file() {
        use mxbot_common::supervise::{report, snapshot};
        let alive = report("radar poll under test");
        let summary = |fetched, new_items, panicked| PollSummary {
            sources: 2,
            fetched,
            new_items,
            panicked,
        };
        summary(10, 2, 0).report(&alive);
        summary(10, 0, 0).report(&alive);
        summary(0, 0, 0).report(&alive);
        summary(5, 0, 1).report(&alive);
        let task = snapshot()
            .into_iter()
            .find(|t| t.name == "radar poll under test")
            .unwrap();
        assert_eq!(
            (task.ok_total, task.idle_total, task.failed_total),
            (1, 1, 2)
        );
        assert_eq!(
            task.last_err_reason.as_deref(),
            Some("1 source fetch(es) panicked")
        );
        // No sources configured: nothing to fetch is not a failure.
        PollSummary::default().report(&alive);
        let task = snapshot()
            .into_iter()
            .find(|t| t.name == "radar poll under test")
            .unwrap();
        assert_eq!(task.idle_total, 2);
    }

    fn source(base: Option<f64>, max_age_hours: Option<u64>) -> SourceConfig {
        SourceConfig {
            name: "Polizei Berlin".to_owned(),
            id: None,
            scope: None,
            official: false,
            category: None,
            source_type: sources::SourceType::Rss,
            url: None,
            query: None,
            limit: None,
            max_age_hours,
            filter: true,
            base_implied_meters: base,
            required: Some(vec![]),
            stale_after_days: None,
        }
    }

    fn filter() -> FilterConfig {
        FilterConfig {
            digest_threshold: 3,
            candidate_min_score: 1,
            area: vec![AreaGroup {
                implied_meters: 1500.0,
                terms: vec!["Charlottenburg".to_owned()],
            }],
            ..Default::default()
        }
    }

    fn report(title: &str, article: &str) -> FeedItem {
        FeedItem {
            guid: title.to_owned(),
            title: title.to_owned(),
            link: None,
            link_note: None,
            description: None,
            article_text: Some(article.to_owned()),
            source_name: "Polizei Berlin".to_owned(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            location_label: None,
            published_at: Some(chrono::Utc::now().timestamp()),
            ..Default::default()
        }
    }

    async fn run(source: SourceConfig, item: FeedItem) -> ProcessOutcome {
        process_item(
            PollCtx::new(Http::offline(), filter(), None),
            source,
            item,
            false,
        )
        .await
    }

    #[tokio::test]
    async fn city_wide_report_is_a_candidate_without_fake_location() {
        let item = report(
            "Mann bei Raub mit Messer verletzt",
            "Charlottenburg-Wilmersdorf Nr. 1262 Ein Mann wurde in Kreuzberg verletzt.",
        );

        let ProcessOutcome::Candidate { item, .. } = run(source(Some(9000.0), None), item).await
        else {
            panic!("expected a candidate");
        };

        assert_eq!(item.score, 1);
        assert_eq!(item.distance_meters, None);
        assert_eq!(item.location_label, None);
    }

    #[tokio::test]
    async fn local_report_keeps_its_district_label() {
        let item = report(
            "Polizeieinsatz",
            "Nr. 1253 Bei einem Polizeieinsatz in Charlottenburg wurden zwei Bauarbeiter verletzt.",
        );

        let ProcessOutcome::Candidate { item, .. } = run(source(Some(9000.0), None), item).await
        else {
            panic!("expected a candidate below threshold 3");
        };

        assert_eq!(item.score, 2);
        assert_eq!(item.location_label.as_deref(), Some("Charlottenburg"));
    }

    #[tokio::test]
    async fn items_older_than_max_age_are_dropped() {
        let mut item = report("Einsatz in Charlottenburg", "Einsatz in Charlottenburg.");
        item.published_at = Some(chrono::Utc::now().timestamp() - 5 * 86_400);

        let outcome = run(source(None, Some(72)), item).await;

        assert!(
            matches!(outcome, ProcessOutcome::Dropped { reason, .. } if reason.contains("older than 72h"))
        );
    }

    #[test]
    fn items_filed_under_another_district_or_titled_elsewhere_are_flagged() {
        let mut filter = filter();
        filter.home_borough = Some("Charlottenburg-Wilmersdorf".to_owned());
        filter.elsewhere = vec!["Neukölln".to_owned()];

        let mut filed = report("Raub", "");
        filed.district = Some("Spandau".to_owned());
        let mut home = report("Raub", "");
        home.district = Some("Charlottenburg-Wilmersdorf".to_owned());
        let mut citywide = report("Raub", "");
        citywide.district = Some("bezirksübergreifend".to_owned());

        assert!(located_elsewhere(&filed, &filter).is_some());
        assert!(located_elsewhere(&home, &filter).is_none());
        assert!(located_elsewhere(&citywide, &filter).is_none());
        assert!(located_elsewhere(&report("Messerangriff in Neukölln", ""), &filter).is_some());
        assert!(located_elsewhere(
            &report("Demo von Neukölln nach Charlottenburg", ""),
            &filter
        )
        .is_none());
    }

    #[tokio::test]
    async fn source_coordinates_count_as_measured_evidence() {
        let mut item = report("Konzert", "Ein Konzert.");
        item.coords = Some((52.4745, 13.4205));
        item.venue = Some("Musterstraße 1".to_owned());

        // Offline: the venue address is a known geocoder miss.
        let ctx = PollCtx::new(Http::offline(), filter(), Some((52.4736, 13.4205)));
        ctx.geocoder
            .cache
            .insert("Musterstraße 1".to_owned(), Vec::new());

        let outcome = process_item(ctx, source(Some(9000.0), None), item, false).await;

        let ProcessOutcome::Queued(item) = outcome else {
            panic!("expected queued");
        };
        assert_eq!(item.score, 5);
        assert_eq!(item.location_label.as_deref(), Some("Musterstraße 1"));
        assert!(item.distance_meters.is_some_and(|m| m < 200.0));
    }

    #[tokio::test]
    async fn unreachable_article_is_decided_after_the_last_retry() {
        let mut item = report("Einsatz", "");
        item.article_text = None;
        item.link = Some("https://unreachable.test/report".to_owned());
        let run = |final_attempt| {
            process_item(
                PollCtx::new(Http::offline(), filter(), None),
                source(Some(9000.0), None),
                item.clone(),
                final_attempt,
            )
        };

        assert!(matches!(run(false).await, ProcessOutcome::Retry { .. }));
        assert!(matches!(run(true).await, ProcessOutcome::Candidate { .. }));
    }

    #[tokio::test]
    async fn retry_attempts_are_counted_and_cleared_on_decision() {
        let (db, _dir) = crate::testing::test_db();
        let item = report("Einsatz", "");

        db.record_retry(&item.guid).await.unwrap();
        db.record_retry(&item.guid).await.unwrap();
        assert_eq!(db.retry_attempts(&item.guid).await.unwrap(), 2);

        db.insert_candidate(&item, "test").await.unwrap();
        assert_eq!(db.retry_attempts(&item.guid).await.unwrap(), 0);
    }

    /// F-01: an unfiltered source is meant to be delivered whatever its distance, but the
    /// digest drops its queued items below the threshold.
    #[tokio::test]
    async fn an_unfiltered_source_is_delivered_whatever_its_score() {
        let mut local = source(Some(1_500.0), None);
        local.filter = false;
        let ProcessOutcome::Queued(item) =
            run(local, report("Hoffest im Hinterhof", "Mit Kuchen.")).await
        else {
            panic!("an unfiltered source's item is queued");
        };
        assert_eq!(item.score, 2);
        let (db, _dir) = crate::testing::test_db();
        db.insert_queued(&item).await.unwrap();

        let mut filtered = report("Einsatz", "Ein Einsatz.");
        filtered.guid = "filtered".to_owned();
        filtered.score = 2;
        db.insert_queued(&filtered).await.unwrap();

        let taken = db.take_for_digest(3).await.unwrap();

        assert_eq!(taken.len(), 1, "threshold 3 dropped the unfiltered item");
        assert_eq!(taken[0].guid, item.guid);
    }

    fn feed(name: &str, url: &str) -> SourceConfig {
        SourceConfig {
            name: name.to_owned(),
            url: Some(url.to_owned()),
            ..source(Some(9_000.0), None)
        }
    }

    fn names(sources: &[SourceConfig]) -> Vec<String> {
        sources.iter().map(|s| s.name.clone()).collect()
    }

    async fn rounds(http: &Http, sources: &[SourceConfig], db: &Db, n: usize) {
        let ctx = PollCtx::new(http.clone(), filter(), None);
        for _ in 0..n {
            poll_once(&ctx, db, sources, None, true).await;
        }
    }

    #[tokio::test]
    async fn a_feed_answering_not_modified_is_healthy_and_gives_no_items() {
        let (db, _dir) = crate::testing::test_db();
        let http = Http::recorded(Arc::new(crate::fetch::Recorded::new().route(
            "https://feed.test/",
            304,
            "",
        )));
        let sources = [feed("Quiet Feed", "https://feed.test/rss")];

        rounds(&http, &sources, &db, 1).await;

        let health = db.source_health().await.unwrap();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].consecutive_failures, 0);
        assert!(health[0].last_success_at.is_some());
        assert_eq!(health[0].last_item_at, None);
    }

    #[tokio::test]
    async fn six_failed_polls_in_a_row_name_the_source() {
        let (db, _dir) = crate::testing::test_db();
        let http = Http::recorded(Arc::new(
            crate::fetch::Recorded::new()
                .route("https://gone.test/", 404, "<html>Not Found</html>")
                .route(
                    "https://ok.test/",
                    200,
                    &crate::testing::fixture("feeds/police_title_only.xml"),
                ),
        ));
        let sources = [
            feed("Gone Feed", "https://gone.test/rss"),
            feed("Fine Feed", "https://ok.test/rss"),
        ];
        let now = chrono::Utc::now().timestamp();

        rounds(&http, &sources, &db, 5).await;
        let (failing, _) =
            source_verdict(&db.source_health().await.unwrap(), &names(&sources), now);
        assert!(failing.is_empty(), "five are not yet failing: {failing:?}");

        rounds(&http, &sources, &db, 1).await;
        let health = db.source_health().await.unwrap();
        let (failing, silent) = source_verdict(&health, &names(&sources), now);

        assert_eq!(failing, vec!["Gone Feed (refused: HTTP 404)"]);
        assert!(silent.is_empty());
        let fine = health
            .iter()
            .find(|h| h.source_name == "Fine Feed")
            .unwrap();
        assert!(fine.last_item_at.is_some());
    }

    #[test]
    fn a_source_without_new_items_for_a_month_is_silent() {
        let now = 100 * 86_400;
        let health = |last_item_at, failures| store::SourceHealth {
            source_name: "S".to_owned(),
            first_polled_at: 0,
            last_success_at: Some(now),
            last_item_at,
            last_error: None,
            consecutive_failures: failures,
        };
        let sources = vec!["S".to_owned()];

        assert_eq!(
            source_verdict(&[health(Some(now - 31 * 86_400), 0)], &sources, now)
                .1
                .len(),
            1
        );
        assert_eq!(
            source_verdict(&[health(Some(now - 29 * 86_400), 0)], &sources, now)
                .1
                .len(),
            0
        );
        assert_eq!(source_verdict(&[health(None, 0)], &sources, now).1.len(), 1);
        // Failing is reported as failing, not as silent; unknown names are ignored.
        assert_eq!(source_verdict(&[health(None, 6)], &sources, now).1.len(), 0);
        assert_eq!(
            source_verdict(&[health(None, 0)], &[], now),
            (vec![], vec![])
        );
    }

    fn linked(title: &str, url: &str) -> FeedItem {
        let mut item = report(title, "");
        item.article_text = None;
        item.link = Some(url.to_owned());
        item
    }

    async fn run_on(http: Http, item: FeedItem) -> ProcessOutcome {
        process_item(
            PollCtx::new(http, filter(), None),
            source(Some(9000.0), None),
            item,
            false,
        )
        .await
    }

    fn page_http(status: u16, body: &str) -> Http {
        Http::recorded(Arc::new(crate::fetch::Recorded::new().route(
            "https://page.test/",
            status,
            body,
        )))
    }

    #[tokio::test]
    async fn a_soft_hyphen_does_not_hide_an_area_term() {
        let in_title = report("Polizeieinsatz in Charlotten\u{AD}burg", "Ein Einsatz.");
        let ProcessOutcome::Candidate { item, .. } =
            run(source(Some(9000.0), None), in_title).await
        else {
            panic!("expected a candidate");
        };
        assert_eq!(item.location_label.as_deref(), Some("Charlottenburg"));

        let page = "<html><body><article><h1>Polizeieinsatz</h1><p>Bei einem Einsatz in \
                    Charlotten&shy;burg wurden zwei Bauarbeiter verletzt.</p></article></body></html>";
        let in_page = linked("Polizeieinsatz", "https://page.test/1");
        let ProcessOutcome::Candidate { item, .. } = run_on(page_http(200, page), in_page).await
        else {
            panic!("expected a candidate");
        };
        assert_eq!(item.location_label.as_deref(), Some("Charlottenburg"));
    }

    #[tokio::test]
    async fn a_server_error_page_is_retried() {
        let item = linked("Einsatz", "https://page.test/1");

        let outcome = run_on(
            page_http(503, "<html><body>Service Unavailable</body></html>"),
            item,
        )
        .await;

        assert!(matches!(outcome, ProcessOutcome::Retry { .. }));
    }

    #[tokio::test]
    async fn a_refused_page_is_decided_on_the_feed_text() {
        let refused = "<html><body><article><p>Zugriff verweigert. Polizeieinsatz in \
                       Charlottenburg und weitere Meldungen aus der Perleberger Straße.</p>\
                       </article></body></html>";
        let item = linked("Einsatz", "https://page.test/1");

        let ProcessOutcome::Candidate { item, .. } = run_on(page_http(403, refused), item).await
        else {
            panic!("decided as a candidate on its source default");
        };

        assert_eq!(item.article_text, None);
        assert_eq!(item.location_label, None);
    }

    #[tokio::test]
    async fn a_page_over_the_cap_is_decided_and_never_fetched_again() {
        let (db, _dir) = crate::testing::test_db();
        let rss = r#"<?xml version="1.0"?><rss version="2.0"><channel><title>Feed</title>
            <item><title>Einsatz am Abend</title><link>https://big.test/page</link>
            <guid>big-1</guid></item></channel></rss>"#;
        let recorded = Arc::new(
            crate::fetch::Recorded::new()
                .route("https://feed.test/", 200, rss)
                .route("https://big.test/", 200, &"x".repeat(3 * 1024 * 1024)),
        );
        let http = Http::recorded(recorded.clone());
        let sources = [feed("Big Pages", "https://feed.test/rss")];
        let ctx = PollCtx::new(http, filter(), None);

        poll_once(&ctx, &db, &sources, None, false).await;
        poll_once(&ctx, &db, &sources, None, false).await;

        let page_requests = recorded
            .calls()
            .iter()
            .filter(|call| call.contains("big.test"))
            .count();
        assert_eq!(page_requests, 1);
        assert_eq!(db.retry_attempts("big-1").await.unwrap(), 0);
        assert!(db
            .seen_guids(vec!["big-1".to_owned()])
            .await
            .unwrap()
            .contains("big-1"));
    }

    fn police_feed() -> String {
        crate::testing::fixture("feeds/police_title_only.xml")
    }

    #[tokio::test]
    async fn a_failing_seen_check_skips_the_source_for_the_round() {
        let (db, dir) = crate::testing::test_db();
        mxbot_common::sqlite::rusqlite::Connection::open(dir.path().join("items.db"))
            .unwrap()
            .execute_batch("DROP TABLE items")
            .unwrap();
        let recorded =
            Arc::new(crate::fetch::Recorded::new().route("https://ok.test/", 200, &police_feed()));
        let sources = [feed("Fine Feed", "https://ok.test/rss")];
        let ctx = PollCtx::new(Http::recorded(recorded.clone()), filter(), None);

        let summary = poll_once(&ctx, &db, &sources, None, true).await;

        assert_eq!(summary.new_items, 0);
        assert_eq!(recorded.calls(), vec!["GET https://ok.test/rss"]);
        assert!(db.source_health().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_round_without_any_reachable_source_does_not_count_against_them() {
        let (db, _dir) = crate::testing::test_db();
        let sources = [
            feed("Feed A", "https://a.test/rss"),
            feed("Feed B", "https://b.test/rss"),
        ];

        rounds(&Http::offline(), &sources, &db, 6).await;

        // (`radar sources` then says "no source reachable"; other tests report the same task
        // concurrently, so it is not asserted here.)
        assert!(db.source_health().await.unwrap().is_empty());

        // One reachable source: the other one's failure counts.
        let http = Http::recorded(Arc::new(crate::fetch::Recorded::new().route(
            "https://a.test/",
            200,
            &police_feed(),
        )));
        rounds(&http, &sources, &db, 1).await;
        let health = db.source_health().await.unwrap();
        let b = health.iter().find(|h| h.source_name == "Feed B").unwrap();
        assert_eq!(b.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn an_item_that_keeps_panicking_is_dropped_after_the_last_attempt() {
        let (db, _dir) = crate::testing::test_db();
        let item = report("Kaputt", "");

        for attempts in 0..MAX_RETRY_ATTEMPTS - 1 {
            item_panicked(&db, &item, attempts, false).await;
            assert!(db
                .seen_guids(vec![item.guid.clone()])
                .await
                .unwrap()
                .is_empty());
        }
        item_panicked(&db, &item, MAX_RETRY_ATTEMPTS - 1, false).await;

        assert!(db
            .seen_guids(vec![item.guid.clone()])
            .await
            .unwrap()
            .contains(&item.guid));
    }

    fn rss_dated(guid: &str, days_ago: i64) -> String {
        let date = (chrono::Utc::now() - chrono::Duration::days(days_ago)).to_rfc2822();
        format!(
            r#"<?xml version="1.0"?><rss version="2.0"><channel><title>Blog</title>
            <item><title>Eintrag {guid}</title><guid>{guid}</guid><pubDate>{date}</pubDate></item>
            </channel></rss>"#
        )
    }

    #[tokio::test]
    async fn a_stale_feed_is_reported_once_and_its_recovery_once() {
        let (db, _dir) = crate::testing::test_db();
        let sources = [feed("Old Blog", "https://blog.test/rss")];
        let answer = |body: String| {
            Http::recorded(Arc::new(crate::fetch::Recorded::new().route(
                "https://blog.test/",
                200,
                &body,
            )))
        };
        let reported =
            |db: Db| async move { db.source_reports().await.unwrap()["Old Blog"].1.clone() };
        let now = || chrono::Utc::now().timestamp();

        rounds(&answer(rss_dated("old", 60)), &sources, &db, 1).await;
        let first = source_notices(&db, &sources, now()).await;
        rounds(&answer(rss_dated("old", 60)), &sources, &db, 1).await;
        let second = source_notices(&db, &sources, now()).await;

        assert_eq!(first.len(), 1, "{first:?}");
        assert!(first[0].contains("„Old Blog“ ist veraltet"), "{first:?}");
        assert!(second.is_empty(), "{second:?}");
        assert_eq!(reported(db.clone()).await.as_deref(), Some("stale"));

        rounds(&answer(rss_dated("fresh", 1)), &sources, &db, 1).await;
        let recovered = source_notices(&db, &sources, now()).await;
        let after = source_notices(&db, &sources, now()).await;

        assert_eq!(
            recovered,
            vec!["✅ Quelle „Old Blog“ läuft wieder".to_owned()]
        );
        assert!(after.is_empty());
        assert_eq!(reported(db).await.as_deref(), Some("ok"));
    }

    #[test]
    fn searches_are_never_stale_and_go_quiet_without_a_notice() {
        let now = 100 * 86_400;
        let health = store::SourceHealth {
            source_name: "S".to_owned(),
            first_polled_at: 0,
            last_success_at: Some(now),
            last_item_at: Some(now - 40 * 86_400),
            last_error: None,
            consecutive_failures: 0,
        };
        let mut search = feed("S", "https://s.test/");
        search.source_type = sources::SourceType::GoogleNews;
        let old = Some(now - 90 * 86_400);

        assert_eq!(
            source_state(&search, &health, old, now),
            SourceState::Silent
        );
        assert_eq!(
            source_notice(&search, SourceState::Ok, SourceState::Silent, &health, old),
            None
        );
        let blog = feed("S", "https://s.test/");
        assert_eq!(source_state(&blog, &health, old, now), SourceState::Stale);
        assert!(source_notice(&blog, SourceState::Ok, SourceState::Silent, &health, old).is_some());
    }

    #[tokio::test]
    async fn a_feed_without_entries_is_malformed_but_an_empty_search_is_not() {
        let empty =
            r#"<?xml version="1.0"?><rss version="2.0"><channel><title>T</title></channel></rss>"#;
        let http = Http::recorded(Arc::new(
            crate::fetch::Recorded::new().route("https://", 200, empty),
        ));
        let rss = sources::build_adapter(&feed("Feed", "https://feed.test/rss"), None);
        let mut search = feed("Search", "https://unused.test/");
        search.source_type = sources::SourceType::GoogleNews;
        search.query = Some("Musterstraße".to_owned());

        assert!(matches!(
            rss.fetch_items(&http).await,
            Err(SourceError::Malformed(why)) if why == "no entries"
        ));
        let found = sources::build_adapter(&search, None)
            .fetch_items(&http)
            .await;
        assert!(found.unwrap().items.is_empty());
    }

    #[tokio::test]
    async fn a_paywalled_item_is_decided_on_its_feed_text_without_a_page() {
        let recorded = Arc::new(crate::fetch::Recorded::new().route(
            "https://page.test/",
            200,
            "<html><body><article>Text</article></body></html>",
        ));
        let mut item = linked("Polizeieinsatz in Charlottenburg", "https://page.test/1");
        item.paywalled = true;

        let outcome = run_on(Http::recorded(recorded.clone()), item).await;

        assert!(matches!(outcome, ProcessOutcome::Candidate { .. }));
        assert!(recorded.calls().is_empty(), "{:?}", recorded.calls());
    }

    #[tokio::test]
    async fn every_decided_item_is_observed_once_with_its_shadow_decision() {
        let (db, _dir) = crate::testing::test_db();
        let http = Http::recorded(Arc::new(crate::fetch::Recorded::new().route(
            "https://ok.test/",
            200,
            &police_feed(),
        )));
        let sources = [feed("Fine Feed", "https://ok.test/rss")];

        rounds(&http, &sources, &db, 1).await;
        let first = db.observation_rows().await.unwrap();
        rounds(&http, &sources, &db, 1).await;
        let second = db.observation_rows().await.unwrap();

        let items = crate::feed::parse_feed(&police_feed(), "x").len();
        assert_eq!(first.len(), items);
        assert_eq!(second, first, "a second round observes nothing new");
        for (source_id, _, category, class, decision) in &first {
            assert_eq!(source_id, "fine-feed", "no id: the slug of the name");
            assert!(
                crate::model::Category::parse(category).is_some(),
                "{category}"
            );
            assert!(crate::model::Class::parse(class).is_some(), "{class}");
            let record: serde_json::Value = serde_json::from_str(decision).unwrap();
            assert!(
                record["rule"].is_string() && record["gate"].is_string(),
                "{record}"
            );
        }
    }

    #[tokio::test]
    async fn a_renamed_source_with_the_same_id_observes_nothing_new() {
        let (db, _dir) = crate::testing::test_db();
        let http = Http::recorded(Arc::new(crate::fetch::Recorded::new().route(
            "https://ok.test/",
            200,
            &police_feed(),
        )));
        rounds(
            &http,
            &[feed("Local Police", "https://ok.test/rss")],
            &db,
            1,
        )
        .await;
        let before = db.observation_rows().await.unwrap().len();

        let mut renamed = feed("Police of the city", "https://ok.test/rss");
        renamed.id = Some("local-police".to_owned());
        rounds(&http, &[renamed], &db, 1).await;

        assert_eq!(db.observation_rows().await.unwrap().len(), before);
        assert!(db
            .observation_rows()
            .await
            .unwrap()
            .iter()
            .all(|row| row.0 == "local-police"));
    }
}
