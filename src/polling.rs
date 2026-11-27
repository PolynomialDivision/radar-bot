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
    if needs_article && item.article_text.is_none() {
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
    let mut source_tasks: JoinSet<(SourceConfig, Result<Vec<FeedItem>, SourceError>)> =
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
    while let Some(res) = source_tasks.join_next().await {
        let (source, result) = match res {
            Ok(v) => v,
            Err(e) => {
                warn!("Source fetch task panicked: {e}");
                summary.panicked += 1;
                continue;
            }
        };
        let items = match result {
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
        *total_by_source.entry(source.name.clone()).or_default() += items.len();
        summary.fetched += items.len();

        let guids = items.iter().map(|item| item.guid.clone()).collect();
        let seen = db.seen_guids(guids).await.unwrap_or_default();
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
        pending.extend(new.into_iter().map(|item| (source.clone(), item)));
    }
    report_source_health(db, sources, now).await;

    // Spawn one task per item: article fetches and geocoding run in parallel.
    // Geocoding tasks share the NominatimLimiter so total Nominatim traffic stays ≤ 1 req/s.
    let mut tasks: JoinSet<ProcessOutcome> = JoinSet::new();
    let n_pending = pending.len();
    summary.new_items = n_pending;
    for (source, item) in pending {
        let attempts = db.retry_attempts(&item.guid).await.unwrap_or(0);
        tasks.spawn(process_item(
            ctx.clone(),
            source,
            item,
            decide_now || attempts >= MAX_RETRY_ATTEMPTS,
        ));
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

    while let Some(res) = tasks.join_next().await {
        n_done_shared.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match res {
            Ok(ProcessOutcome::Queued(item)) => {
                *passed_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                db.insert_queued(&item).await.ok();
            }
            Ok(ProcessOutcome::Candidate { item, reason }) => {
                *candidate_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *candidate_reasons.entry(reason.clone()).or_default() += 1;
                db.insert_candidate(&item, &reason).await.ok();
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
            }
            Ok(ProcessOutcome::Retry { item, reason }) => {
                *retried_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *retry_reasons.entry(reason).or_default() += 1;
                db.record_retry(&item.guid).await.ok();
            }
            Err(e) => warn!("Item task panicked: {e}"),
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

pub(crate) async fn poll_loop(
    ctx: PollCtx,
    db: Db,
    sources: Vec<SourceConfig>,
    interval_mins: u64,
    bluesky: Option<sources::BlueskyContext>,
    alive: Alive,
) {
    let interval = Duration::from_secs(interval_mins * 60);
    alive.expect_every(interval);
    loop {
        alive.tick();
        poll_once(&ctx, &db, &sources, bluesky.as_ref(), false)
            .await
            .report(&alive);
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
            source_type: sources::SourceType::Rss,
            url: None,
            query: None,
            limit: None,
            max_age_hours,
            filter: true,
            base_implied_meters: base,
            required: Some(vec![]),
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
}
