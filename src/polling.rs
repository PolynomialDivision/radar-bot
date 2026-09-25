//! Polling sources and scoring/queueing new items.

use super::*;

// ── Polling loop ──────────────────────────────────────────────────────────────

pub(crate) enum ProcessOutcome {
    Queued(FeedItem),
    Candidate { item: FeedItem, reason: String },
    Dropped { item: FeedItem, reason: String },
    Retry { item: FeedItem, reason: String },
}

/// Process a single feed item: fetch article, keyword-check, geocode, score.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_item(
    http: reqwest::Client,
    source: SourceConfig,
    mut item: FeedItem,
    filter: Arc<FilterConfig>,
    ref_point: Option<(f64, f64)>,
    geocode_city: Arc<str>,
    geocode_cache: GeocodeCache,
    limiter: NominatimLimiter,
) -> ProcessOutcome {
    let source_name = source.name.clone();

    // Fetch article body (used for filtering + geocoding street extraction).
    // extract_article_body() isolates <article>/<main> to avoid navigation false-positives.
    let needs_article = source.filter || source.base_implied_meters.is_some();
    let mut article_fetch_failed = false;
    if needs_article && item.article_text.is_none() {
        if let Some(ref url) = item.link {
            tracing::debug!("fetching article [{}] {:?}", source_name, item.title);
            match fetch_article(&http, url, &item.title).await {
                Some(article) => {
                    item.article_text = Some(article.text);
                    if let Some(link) = article.resolved_link {
                        item.link = Some(link);
                    }
                    item.link_note = article.link_note;
                }
                None => {
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
        }
    }

    // ── 1. Keyword check → implied distance ───────────────────────────────────
    let effective_required: &[String] = source.required.as_deref().unwrap_or(&filter.required);
    let kw_evidence: Option<(f64, String)> = if source.filter {
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
            Some((implied, matched)) => {
                let implied = implied.or_else(|| {
                    source.base_implied_meters.map(|meters| {
                        let label = geocode_city
                            .split(',')
                            .next()
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .unwrap_or(&source_name)
                            .to_owned();
                        (meters, label)
                    })
                });
                if !matched.is_empty() {
                    info!(
                        "PASS [{}] {:?} keyword_implied={:?}m terms=[{}]",
                        source_name,
                        item.title,
                        implied.as_ref().map(|(m, _)| *m as u32),
                        matched.join(", ")
                    );
                }
                implied
            }
        }
    } else {
        source
            .base_implied_meters
            .map(|meters| (meters, source_name.clone()))
    };

    // ── 2. Geocode → actual distance ──────────────────────────────────────────
    // Actual distance and keyword-implied are combined by taking the closer result.
    let geocoded_evidence: Option<(f64, String)> = if let Some((ref_lat, ref_lon)) = ref_point {
        // If geocoding produces false positives from RSS footer boilerplate
        // (e.g. Polizei Berlin appends their HQ address to every description),
        // switch to article_text-only by uncommenting the two lines below and
        // removing the third:
        // let body = item.article_text.as_deref()
        //     .or(item.description.as_deref())
        //     .unwrap_or("");
        let candidates_count = extract_street_evidence(
            &item.title,
            item.description.as_deref(),
            item.article_text.as_deref(),
        )
        .len();
        info!(
            "  geocoding [{}] {:?} ({} candidates)",
            source_name, item.title, candidates_count
        );
        match find_nearest_distance(
            &http,
            &item.title,
            item.description.as_deref(),
            item.article_text.as_deref(),
            ref_lat,
            ref_lon,
            &geocode_city,
            &geocode_cache,
            &limiter,
        )
        .await
        {
            DistanceLookup::Found(dist, ref label) => {
                info!(
                    "  📍 [{}] {:?} → {} at {}",
                    source_name,
                    label,
                    item.title,
                    format_distance(dist)
                );
                item.distance_meters = Some(dist);
                Some((dist, label.clone()))
            }
            DistanceLookup::NoMatch => {
                info!("  no street geocoded [{}] {:?}", source_name, item.title);
                None
            }
            DistanceLookup::TransientFailure => {
                return ProcessOutcome::Retry {
                    item,
                    reason: "geocode transient failure before distance decision".to_owned(),
                };
            }
        }
    } else {
        None
    };

    let place_evidence = find_configured_place_distance(&item, &filter, ref_point);
    if let Some((dist, label)) = &place_evidence {
        info!(
            "  📍 [{}] configured place {:?} → {} at {}",
            source_name,
            label,
            item.title,
            format_distance(*dist)
        );
    }

    // ── 3. Final score ────────────────────────────────────────────────────────
    let final_location = [geocoded_evidence, place_evidence, kw_evidence]
        .into_iter()
        .flatten()
        .min_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let final_meters = final_location.as_ref().map(|(meters, _)| *meters);
    let score = final_meters.map(distance_score).unwrap_or(0);
    item.score = score;
    item.max_score = 5;
    item.distance_meters = final_meters;
    item.location_label = final_location.map(|(_, label)| label);

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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn poll_once(
    _client: &Bot,
    http: &reqwest::Client,
    sources: &[SourceConfig],
    filter: &FilterConfig,
    ref_point: Option<(f64, f64)>,
    db: &Db,
    geocode_cache: &GeocodeCache,
    bluesky: Option<&sources::BlueskyContext>,
    test_mode: bool,
) {
    let arc_filter = Arc::new(filter.clone());
    let geocode_city: Arc<str> = Arc::from(filter.geocode_city.as_deref().unwrap_or(""));
    let limiter: NominatimLimiter = Arc::new(Semaphore::new(1));

    // Fetch all feeds concurrently and collect new items, then process them in parallel.
    // Seen-check stays sequential to avoid races; article fetch + geocoding run concurrently.
    let mut pending: Vec<(SourceConfig, FeedItem)> = Vec::new();
    let mut total_by_source: HashMap<String, usize> = HashMap::new();
    let mut source_tasks: JoinSet<(SourceConfig, Vec<FeedItem>)> = JoinSet::new();
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

    while let Some(res) = source_tasks.join_next().await {
        let (source, items) = match res {
            Ok(v) => v,
            Err(e) => {
                warn!("Source fetch task panicked: {e}");
                continue;
            }
        };
        *total_by_source.entry(source.name.clone()).or_default() += items.len();

        for item in items {
            let is_new = test_mode || db.is_new(&item.guid).await.unwrap_or(true);
            if is_new {
                pending.push((source.clone(), item));
            }
        }
    }

    // Spawn one task per item — article fetches and geocoding run in parallel.
    // Geocoding tasks share the NominatimLimiter so total Nominatim traffic stays ≤ 1 req/s.
    let mut tasks: JoinSet<ProcessOutcome> = JoinSet::new();
    let n_pending = pending.len();
    for (source, item) in pending {
        tasks.spawn(process_item(
            http.clone(),
            source,
            item,
            arc_filter.clone(),
            ref_point,
            geocode_city.clone(),
            geocode_cache.clone(),
            limiter.clone(),
        ));
    }
    if n_pending > 0 {
        info!("Processing {} new item(s) in parallel...", n_pending);
    }

    // Heartbeat: log progress every 5s so the bot doesn't appear frozen during geocoding.
    let n_done_shared = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ticker = {
        let cache = geocode_cache.clone();
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
                if !test_mode {
                    db.insert_queued(&item).await.ok();
                }
            }
            Ok(ProcessOutcome::Candidate { item, reason }) => {
                *candidate_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *candidate_reasons.entry(reason.clone()).or_default() += 1;
                if !test_mode {
                    db.insert_candidate(&item, &reason).await.ok();
                }
            }
            Ok(ProcessOutcome::Dropped { item, reason }) => {
                *dropped_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *drop_reasons.entry(reason.clone()).or_default() += 1;
                if !test_mode {
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
            }
            Ok(ProcessOutcome::Retry { item, reason }) => {
                *retried_by_source
                    .entry(item.source_name.clone())
                    .or_default() += 1;
                *retry_reasons.entry(reason).or_default() += 1;
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
        source_stats.push(db::SourcePollStat {
            source_name: source.name.clone(),
            total,
            queued: passed,
            candidate: candidates,
            dropped,
            retry_later: retried,
        });
    }
    if !test_mode {
        if let Err(e) = db.record_source_stats(source_stats).await {
            warn!("Failed to record source stats: {e}");
        }
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
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn poll_loop(
    client: Bot,
    http: reqwest::Client,
    sources: Vec<SourceConfig>,
    filter: FilterConfig,
    ref_point: Option<(f64, f64)>,
    interval_mins: u64,
    db: Db,
    geocode_cache: GeocodeCache,
    bluesky: Option<sources::BlueskyContext>,
) {
    let interval = Duration::from_secs(interval_mins * 60);
    loop {
        poll_once(
            &client,
            &http,
            &sources,
            &filter,
            ref_point,
            &db,
            &geocode_cache,
            bluesky.as_ref(),
            false,
        )
        .await;
        info!("Next poll in {interval_mins}m");
        sleep(interval).await;
    }
}
