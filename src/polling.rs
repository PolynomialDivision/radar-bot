//! Polling sources and scoring/queueing new items.

use super::*;

// ── Polling loop ──────────────────────────────────────────────────────────────

/// After this many polls ending in "retry later" (article page or geocoder
/// unreachable), an item is decided with the information at hand — with the
/// default 30-minute interval that is three hours.
pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 6;

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
    final_attempt: bool,
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
            if article_fetch_failed && final_attempt {
                // Retried often enough — decide with feed title/description.
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
    if let Some((ref_lat, ref_lon)) = ref_point {
        match find_nearest_distance(
            &http,
            &item,
            &text,
            ref_lat,
            ref_lon,
            &geocode_city,
            &geocode_cache,
            &limiter,
        )
        .await
        {
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
        let attempts = if test_mode {
            0
        } else {
            db.retry_attempts(&item.guid).await.unwrap_or(0)
        };
        tasks.spawn(process_item(
            http.clone(),
            source,
            item,
            arc_filter.clone(),
            ref_point,
            geocode_city.clone(),
            geocode_cache.clone(),
            limiter.clone(),
            attempts >= MAX_RETRY_ATTEMPTS,
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
                if !test_mode {
                    db.record_retry(&item.guid).await.ok();
                }
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

#[cfg(test)]
mod tests {
    use super::*;

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
                terms: vec!["Friedrichshain".to_owned()],
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
            reqwest::Client::new(),
            source,
            item,
            Arc::new(filter()),
            None,
            Arc::from(""),
            Arc::new(DashMap::new()),
            Arc::new(Semaphore::new(1)),
            false,
        )
        .await
    }

    #[tokio::test]
    async fn city_wide_report_is_a_candidate_without_fake_location() {
        let item = report(
            "Mann bei Raub mit Messer verletzt",
            "Friedrichshain-Kreuzberg Nr. 1262 Ein Mann wurde in Kreuzberg verletzt.",
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
            "Nr. 1253 Bei einem Polizeieinsatz in Friedrichshain wurden zwei Bauarbeiter verletzt.",
        );

        let ProcessOutcome::Candidate { item, .. } = run(source(Some(9000.0), None), item).await
        else {
            panic!("expected a candidate below threshold 3");
        };

        assert_eq!(item.score, 2);
        assert_eq!(item.location_label.as_deref(), Some("Friedrichshain"));
    }

    #[tokio::test]
    async fn items_older_than_max_age_are_dropped() {
        let mut item = report("Einsatz in Friedrichshain", "Einsatz in Friedrichshain.");
        item.published_at = Some(chrono::Utc::now().timestamp() - 5 * 86_400);

        let outcome = run(source(None, Some(72)), item).await;

        assert!(
            matches!(outcome, ProcessOutcome::Dropped { reason, .. } if reason.contains("older than 72h"))
        );
    }

    #[test]
    fn items_filed_under_another_district_or_titled_elsewhere_are_flagged() {
        let mut filter = filter();
        filter.home_borough = Some("Friedrichshain-Kreuzberg".to_owned());
        filter.elsewhere = vec!["Neukölln".to_owned()];

        let mut filed = report("Raub", "");
        filed.district = Some("Lichtenberg".to_owned());
        let mut home = report("Raub", "");
        home.district = Some("Friedrichshain-Kreuzberg".to_owned());
        let mut citywide = report("Raub", "");
        citywide.district = Some("bezirksübergreifend".to_owned());

        assert!(located_elsewhere(&filed, &filter).is_some());
        assert!(located_elsewhere(&home, &filter).is_none());
        assert!(located_elsewhere(&citywide, &filter).is_none());
        assert!(located_elsewhere(&report("Messerangriff in Neukölln", ""), &filter).is_some());
        assert!(located_elsewhere(
            &report("Demo von Neukölln nach Friedrichshain", ""),
            &filter
        )
        .is_none());
    }

    #[tokio::test]
    async fn source_coordinates_count_as_measured_evidence() {
        let mut item = report("Konzert", "Ein Konzert.");
        item.coords = Some((52.5145, 13.4705));
        item.venue = Some("Scharnweberstraße 38".to_owned());

        // Offline: the venue address is a known geocoder miss.
        let cache: GeocodeCache = Arc::new(DashMap::new());
        cache.insert("Scharnweberstraße 38".to_owned(), Vec::new());

        let outcome = process_item(
            reqwest::Client::new(),
            source(Some(9000.0), None),
            item,
            Arc::new(filter()),
            Some((52.5136, 13.4705)),
            Arc::from(""),
            cache,
            Arc::new(Semaphore::new(1)),
            false,
        )
        .await;

        let ProcessOutcome::Queued(item) = outcome else {
            panic!("expected queued");
        };
        assert_eq!(item.score, 5);
        assert_eq!(item.location_label.as_deref(), Some("Scharnweberstraße 38"));
        assert!(item.distance_meters.is_some_and(|m| m < 200.0));
    }

    #[tokio::test]
    async fn unreachable_article_is_decided_after_the_last_retry() {
        let mut item = report("Einsatz", "");
        item.article_text = None;
        item.link = Some("http://127.0.0.1:9/unreachable".to_owned());
        let run = |final_attempt| {
            process_item(
                reqwest::Client::new(),
                source(Some(9000.0), None),
                item.clone(),
                Arc::new(filter()),
                None,
                Arc::from(""),
                Arc::new(DashMap::new()),
                Arc::new(Semaphore::new(1)),
                final_attempt,
            )
        };

        assert!(matches!(run(false).await, ProcessOutcome::Retry { .. }));
        assert!(matches!(run(true).await, ProcessOutcome::Candidate { .. }));
    }

    #[tokio::test]
    async fn retry_attempts_are_counted_and_cleared_on_decision() {
        let path = std::env::temp_dir().join(format!("radar-retry-test-{}.db", std::process::id()));
        let db = Db::open(&path).unwrap();
        let item = report("Einsatz", "");

        db.record_retry(&item.guid).await.unwrap();
        db.record_retry(&item.guid).await.unwrap();
        assert_eq!(db.retry_attempts(&item.guid).await.unwrap(), 2);

        db.insert_candidate(&item, "test").await.unwrap();
        assert_eq!(db.retry_attempts(&item.guid).await.unwrap(), 0);

        drop(db);
        let _ = std::fs::remove_file(path);
    }
}
