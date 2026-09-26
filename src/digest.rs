//! Digest formatting, duplicate clustering, chunking and the digest loop.

use super::*;

// ── Message formatting ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct DigestItem {
    pub(crate) guids: Vec<String>,
    pub(crate) source_name: String,
    pub(crate) source_count: usize,
    pub(crate) title: String,
    pub(crate) link: Option<String>,
    pub(crate) link_note: Option<String>,
    pub(crate) score: i32,
    pub(crate) max_score: i32,
    pub(crate) distance_meters: Option<f64>,
    pub(crate) location_label: Option<String>,
}

impl DigestItem {
    pub(crate) fn from_db(item: db::DbItem) -> Self {
        Self {
            guids: vec![item.guid],
            source_name: item.source_name,
            source_count: 1,
            title: item.title,
            link: item.link,
            link_note: item.link_note,
            score: item.score,
            max_score: item.max_score,
            distance_meters: item.distance_meters,
            location_label: item.location_label,
        }
    }
}

pub(crate) fn format_digest(items: &[DigestItem], header: &str) -> (String, String) {
    let mut plain = vec![format!("📡 {header}\n")];
    let mut html = vec![format!("📡 <strong>{}</strong><br>", html_escape(header))];

    for item in items {
        let src = &item.source_name;

        let mut info_parts: Vec<String> = Vec::new();
        if item.max_score > 0 {
            info_parts.push(format!("{}/{}", item.score, item.max_score));
        }
        if item.source_count > 1 {
            info_parts.push(format!("{} sources", item.source_count));
        }
        let label = item
            .location_label
            .as_deref()
            .map(str::trim)
            .filter(|label| !label.is_empty());
        match (item.distance_meters, label) {
            (Some(d), Some(label)) => {
                info_parts.push(format!("{} · {}", format_distance(d), label))
            }
            (Some(d), None) => info_parts.push(format_distance(d)),
            (None, Some(label)) => info_parts.push(label.to_owned()),
            (None, None) => {}
        }
        let info_plain = if info_parts.is_empty() {
            String::new()
        } else {
            format!(" [{}]", info_parts.join(" "))
        };
        let info_html = if info_parts.is_empty() {
            String::new()
        } else {
            format!(" <em>[{}]</em>", html_escape(&info_parts.join(" ")))
        };

        let link_p = item
            .link
            .as_deref()
            .map(|l| format!(" — {l}"))
            .unwrap_or_default();
        let link_h = item
            .link
            .as_deref()
            .map(|l| format!(" — <a href=\"{}\">link</a>", html_escape(l)))
            .unwrap_or_default();
        let note_p = item
            .link_note
            .as_deref()
            .map(|note| format!(" ({note})"))
            .unwrap_or_default();
        let note_h = item
            .link_note
            .as_deref()
            .map(|note| format!(" <em>({})</em>", html_escape(note)))
            .unwrap_or_default();
        let color = score_color(item.score);
        plain.push(format!(
            "{color} [{src}] {}{info_plain}{link_p}{note_p}",
            item.title
        ));
        html.push(format!(
            "{color} <em>[{}]</em> {}{info_html}{link_h}{note_h}<br>",
            html_escape(src),
            html_escape(&item.title)
        ));
    }

    let html_out = html.join("");
    let html_out = html_out.trim_end_matches("<br>").to_owned();
    (plain.join("\n").trim_end().to_owned(), html_out)
}

pub(crate) fn unique_source_names(names: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for name in names {
        if seen.insert(name.to_lowercase()) {
            out.push(name.clone());
        }
    }
    out
}

pub(crate) fn normalized_location_label(item: &db::DbItem) -> Option<String> {
    item.location_label
        .as_deref()
        .map(normalize)
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

pub(crate) fn locations_compatible(a: &db::DbItem, b: &db::DbItem) -> bool {
    match (normalized_location_label(a), normalized_location_label(b)) {
        (Some(la), Some(lb)) if la == lb => return true,
        _ => {}
    }

    match (a.distance_meters, b.distance_meters) {
        (Some(da), Some(db)) => (da - db).abs() <= 300.0,
        _ => false,
    }
}

/// Shared significant title words, and the word count of the shorter title.
pub(crate) fn title_overlap(a: &str, b: &str) -> (usize, usize) {
    let a_words: HashSet<String> = significant_words(a).into_iter().collect();
    let b_words: HashSet<String> = significant_words(b).into_iter().collect();
    (
        a_words.intersection(&b_words).count(),
        a_words.len().min(b_words.len()),
    )
}

/// Titles about the same subject: at least two shared words making up half
/// of the shorter title. Two "Brand in Wohnung in <district>" headlines
/// qualify, so this only counts together with a matching location.
pub(crate) fn titles_related(a: &str, b: &str) -> bool {
    let (shared, shorter) = title_overlap(a, b);
    shared >= 2 && shared * 2 >= shorter
}

/// Titles so alike that they are the same story whatever location each
/// report resolved to: three or more shared words, two thirds of the shorter.
pub(crate) fn titles_same_story(a: &str, b: &str) -> bool {
    let (shared, shorter) = title_overlap(a, b);
    shared >= 3 && shared * 3 >= shorter * 2
}

/// Link identity ignoring scheme, "www.", fragments, trailing slashes and
/// tracking parameters.
pub(crate) fn canonical_link(link: &str) -> String {
    let link = link.trim();
    let link = link.split('#').next().unwrap_or(link);
    let (base, query) = link.split_once('?').unwrap_or((link, ""));
    let base = base
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("www.")
        .trim_end_matches('/')
        .to_lowercase();
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter(|pair| {
            let key = pair.split('=').next().unwrap_or("").to_lowercase();
            !(key.starts_with("utm_")
                || key.starts_with("at_")
                || matches!(
                    key.as_str(),
                    "fbclid" | "gclid" | "ref" | "source" | "wt_mc"
                ))
        })
        .collect();
    if kept.is_empty() {
        base
    } else {
        format!("{base}?{}", kept.join("&"))
    }
}

pub(crate) fn same_link(a: &db::DbItem, b: &db::DbItem) -> bool {
    match (a.link.as_deref(), b.link.as_deref()) {
        (Some(la), Some(lb)) => canonical_link(la) == canonical_link(lb),
        _ => false,
    }
}

pub(crate) fn normalized_source_name(name: &str) -> String {
    normalize(name).trim().to_owned()
}

pub(crate) fn same_source_family(a: &db::DbItem, b: &db::DbItem) -> bool {
    normalized_source_name(&a.source_name) == normalized_source_name(&b.source_name)
}

pub(crate) fn title_terms(s: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut current = String::new();
    for ch in normalize(s).chars() {
        if ch.is_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            terms.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        terms.push(current);
    }
    terms
}

pub(crate) fn contains_any_term(terms: &[String], needles: &[&str]) -> bool {
    terms
        .iter()
        .any(|term| needles.iter().any(|needle| term == needle))
}

pub(crate) fn title_date_signals(s: &str) -> HashSet<String> {
    let terms = title_terms(s);
    let mut dates = HashSet::new();

    for window in terms.windows(3) {
        let [day, month, year] = window else {
            continue;
        };
        if day.len() <= 2
            && month.len() <= 2
            && year.len() == 4
            && day.chars().all(|c| c.is_ascii_digit())
            && month.chars().all(|c| c.is_ascii_digit())
            && year.chars().all(|c| c.is_ascii_digit())
        {
            dates.insert(format!("{:0>2}.{:0>2}.{}", day, month, year));
        }
    }

    dates
}

pub(crate) fn title_event_class(s: &str) -> Option<&'static str> {
    let terms = title_terms(s);
    if contains_any_term(
        &terms,
        &[
            "ankündigung",
            "ankuendigung",
            "announcement",
            "announce",
            "announcing",
        ],
    ) {
        return Some("announcement");
    }
    if contains_any_term(
        &terms,
        &["demo", "demonstration", "kundgebung", "protest", "rally"],
    ) {
        return Some("protest");
    }
    if contains_any_term(
        &terms,
        &[
            "veranstaltung",
            "event",
            "termin",
            "date",
            "konzert",
            "concert",
        ],
    ) {
        return Some("event");
    }
    None
}

pub(crate) fn translated_event_duplicate(a: &db::DbItem, b: &db::DbItem) -> bool {
    if !same_source_family(a, b) || !locations_compatible(a, b) {
        return false;
    }

    let Some(class_a) = title_event_class(&a.title) else {
        return false;
    };
    let Some(class_b) = title_event_class(&b.title) else {
        return false;
    };
    if class_a != class_b {
        return false;
    }

    let dates_a = title_date_signals(&a.title);
    let dates_b = title_date_signals(&b.title);
    !dates_a.is_empty() && !dates_b.is_empty() && !dates_a.is_disjoint(&dates_b)
}

pub(crate) fn likely_same_incident(item: &db::DbItem, cluster: &[db::DbItem]) -> bool {
    cluster.iter().any(|other| {
        same_link(item, other)
            || translated_event_duplicate(item, other)
            || titles_same_story(&item.title, &other.title)
            || (locations_compatible(item, other) && titles_related(&item.title, &other.title))
    })
}

/// How long a posted story suppresses re-reports of itself.
pub(crate) const ALREADY_POSTED_WINDOW_SECS: i64 = 48 * 3600;

/// Split digest items into new ones and repeats of a recently posted story
/// (same article, or a clearly identical headline). Looser matches stay, so
/// a follow-up ("…: Tatverdächtiger festgenommen") is still reported.
pub(crate) fn split_already_posted(
    items: Vec<db::DbItem>,
    posted: &[db::DbItem],
) -> (Vec<db::DbItem>, Vec<db::DbItem>) {
    items.into_iter().partition(|item| {
        !posted
            .iter()
            .any(|old| same_link(item, old) || titles_same_story(&item.title, &old.title))
    })
}

pub(crate) fn cluster_digest_items(items: Vec<db::DbItem>) -> Vec<DigestItem> {
    let mut sorted = items;
    sorted.sort_by_key(|item| std::cmp::Reverse(item.score));

    let mut clusters: Vec<Vec<db::DbItem>> = Vec::new();
    for item in sorted {
        if let Some(cluster) = clusters
            .iter_mut()
            .find(|cluster| likely_same_incident(&item, cluster))
        {
            cluster.push(item);
        } else {
            clusters.push(vec![item]);
        }
    }

    clusters
        .into_iter()
        .map(|cluster| {
            let mut iter = cluster.into_iter();
            let first = iter.next().expect("cluster is never empty");
            let mut digest = DigestItem::from_db(first.clone());
            let mut source_names = vec![first.source_name.clone()];
            let mut best_score = digest.score;

            for item in iter {
                digest.guids.push(item.guid.clone());
                source_names.push(item.source_name.clone());
                if item.score > best_score {
                    best_score = item.score;
                    digest.title = item.title.clone();
                    digest.link = item.link.clone();
                    digest.link_note = item.link_note.clone();
                    digest.distance_meters = item.distance_meters;
                    digest.location_label = item.location_label.clone();
                    digest.max_score = item.max_score;
                }
            }

            let unique_sources = unique_source_names(&source_names);
            digest.source_count = unique_sources.len();
            digest.source_name = unique_sources.join(" + ");
            digest.score = if digest.source_count > 1 {
                (best_score + 1).min(5)
            } else {
                best_score
            };
            digest
        })
        .collect()
}

pub(crate) fn cluster_db_items(items: Vec<db::DbItem>) -> Vec<Vec<db::DbItem>> {
    let mut sorted = items;
    sorted.sort_by_key(|item| std::cmp::Reverse(item.score));

    let mut clusters: Vec<Vec<db::DbItem>> = Vec::new();
    for item in sorted {
        if let Some(cluster) = clusters
            .iter_mut()
            .find(|cluster| likely_same_incident(&item, cluster))
        {
            cluster.push(item);
        } else {
            clusters.push(vec![item]);
        }
    }
    clusters
}

pub(crate) async fn promote_candidate_clusters(
    db: &Db,
    min_score: i32,
    candidate_min_sources: usize,
    candidate_retention_hours: u64,
) -> Result<usize> {
    let retention_secs = (candidate_retention_hours.max(1) as i64) * 3600;
    let expired = db.prune_old_candidates(retention_secs).await?;
    if expired > 0 {
        info!("Expired {expired} old news candidate(s) before digest");
    }

    let pending = db.list_pending_for_clustering(retention_secs).await?;
    if pending.is_empty() {
        return Ok(0);
    }

    let candidate_guids: HashSet<String> = pending
        .iter()
        .filter(|p| p.state == "candidate")
        .map(|p| p.item.guid.clone())
        .collect();
    if candidate_guids.is_empty() {
        return Ok(0);
    }

    let clusters = cluster_db_items(pending.into_iter().map(|p| p.item).collect());
    let mut to_promote = Vec::new();
    for cluster in clusters {
        let best_score = cluster.iter().map(|item| item.score).max().unwrap_or(0);
        let sources: Vec<String> = cluster
            .iter()
            .map(|item| item.source_name.clone())
            .collect();
        let unique_sources = unique_source_names(&sources);
        let corroborated = unique_sources.len() >= candidate_min_sources.max(2);
        let strong_after_boost = if unique_sources.len() > 1 {
            (best_score + 1).min(5) >= min_score
        } else {
            best_score >= min_score
        };

        if corroborated && strong_after_boost {
            to_promote.extend(
                cluster
                    .iter()
                    .filter(|item| candidate_guids.contains(&item.guid))
                    .map(|item| item.guid.clone()),
            );
        }
    }

    let promoted = db
        .promote_candidates(
            &to_promote,
            min_score,
            "promoted by duplicate-source clustering",
        )
        .await?;
    if promoted > 0 {
        info!("Promoted {promoted} news candidate(s) after duplicate-source clustering");
    }
    Ok(promoted)
}

// ── Digest chunking ───────────────────────────────────────────────────────────

pub(crate) const MAX_DIGEST_BYTES: usize = 8_000;
pub(crate) const MAX_DIGEST_ITEMS: usize = 15;

/// Sort by score descending then split into chunks under size and item-count limits.
pub(crate) fn chunk_digest(items: &[DigestItem]) -> Vec<Vec<DigestItem>> {
    let mut sorted = items.to_vec();
    sorted.sort_by_key(|item| std::cmp::Reverse(item.score));

    let mut chunks: Vec<Vec<DigestItem>> = Vec::new();
    let mut current: Vec<DigestItem> = Vec::new();
    let mut current_bytes: usize = 0;

    for item in sorted {
        let item_bytes = item.title.len()
            + item.source_name.len()
            + item.link.as_deref().map_or(0, |l| l.len())
            + item.source_count * 8
            + 50;
        let would_overflow =
            current_bytes + item_bytes > MAX_DIGEST_BYTES || current.len() >= MAX_DIGEST_ITEMS;
        if would_overflow && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += item_bytes;
        current.push(item);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

// ── Digest loop ───────────────────────────────────────────────────────────────

pub(crate) async fn digest_loop(
    client: Bot,
    digest_times: Vec<String>,
    db: Db,
    min_score: i32,
    candidate_min_sources: usize,
    candidate_retention_hours: u64,
) {
    let targets: Vec<NaiveTime> = digest_times
        .iter()
        .filter_map(|s| {
            let mut p = s.splitn(2, ':');
            let h: u32 = p.next()?.parse().ok()?;
            let m: u32 = p.next()?.parse().ok()?;
            NaiveTime::from_hms_opt(h, m, 0)
        })
        .collect();

    if targets.is_empty() {
        warn!("No valid digest_times configured — digest loop disabled");
        return;
    }

    loop {
        let now = Local::now();
        let today = now.date_naive();
        let next_dt = targets
            .iter()
            .map(|t| {
                if now.time() < *t {
                    today.and_time(*t)
                } else {
                    (today + chrono::Duration::days(1)).and_time(*t)
                }
            })
            .min()
            .unwrap();
        let secs = (next_dt - now.naive_local()).num_seconds().max(0) as u64;
        info!("Next digest in {secs}s (at {})", next_dt.format("%H:%M"));
        sleep(Duration::from_secs(secs)).await;

        if let Err(e) = promote_candidate_clusters(
            &db,
            min_score,
            candidate_min_sources,
            candidate_retention_hours,
        )
        .await
        {
            warn!("Digest: candidate promotion failed, continuing with queued items: {e}");
        }

        let items = match db.take_for_digest(min_score).await {
            Ok(v) => v,
            Err(e) => {
                error!("Digest: failed to query DB: {e}");
                continue;
            }
        };

        let posted = match db.list_recent_posted(ALREADY_POSTED_WINDOW_SECS).await {
            Ok(v) => v,
            Err(e) => {
                warn!("Digest: failed to load recently posted items: {e}");
                Vec::new()
            }
        };
        let (items, repeats) = split_already_posted(items, &posted);
        if !repeats.is_empty() {
            info!(
                "Digest: {} item(s) repeat an already posted story — not posting",
                repeats.len()
            );
            let guids: Vec<String> = repeats.into_iter().map(|item| item.guid).collect();
            if let Err(e) = db
                .drop_processing(&guids, "same story already posted")
                .await
            {
                warn!("Digest: failed to drop repeated items: {e}");
            }
        }

        if items.is_empty() {
            info!("Digest: no pending items — staying silent");
            continue;
        }

        let day_str = Local::now().format("%A, %d %b %Y").to_string();
        let header = format!("Digest — {day_str}");
        let clustered_items = cluster_digest_items(items);
        info!(
            "Posting digest with {} clustered item(s)",
            clustered_items.len()
        );
        let chunks = chunk_digest(&clustered_items);
        let total = chunks.len();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let chunk_header = if total > 1 {
                format!("{header} ({}/{})", i + 1, total)
            } else {
                header.clone()
            };
            let (plain, html) = format_digest(&chunk, &chunk_header);
            let guids: Vec<String> = chunk
                .iter()
                .flat_map(|it| it.guids.iter().cloned())
                .collect();
            if post_to_rooms(&client, &plain, &html).await {
                if let Err(e) = db.mark_posted(&guids).await {
                    error!("Digest: failed to mark items posted: {e}");
                }
            } else {
                warn!(
                    "Digest: post failed — requeueing {} item(s) for retry",
                    guids.len()
                );
                db.requeue_failed(&guids, "post_to_rooms failed").await.ok();
            }
            if total > 1 {
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db_item(guid: &str, source: &str, title: &str, link: &str) -> db::DbItem {
        db::DbItem {
            guid: guid.to_owned(),
            source_name: source.to_owned(),
            title: title.to_owned(),
            link: Some(link.to_owned()),
            link_note: None,
            score: 1,
            max_score: 5,
            distance_meters: None,
            location_label: None,
        }
    }

    #[test]
    fn same_story_merges_even_when_locations_resolved_differently() {
        // Posted three times on 2026-09-25: one rbb version had a place
        // mention, the other only the city-wide default.
        let mut a = db_item(
            "a",
            "rbb Berlin",
            "Kameras werden am Kottbusser Tor installiert - Testbetrieb startet in vier Wochen",
            "https://rbb.test/1",
        );
        a.distance_meters = Some(1231.0);
        a.location_label = Some("Warschauer Brücke".to_owned());
        let b = db_item(
            "b",
            "rbb Berlin",
            "Kameras am Kottbusser Tor werden eingerichtet - Testbetrieb in vier Wochen",
            "https://rbb.test/2",
        );

        assert_eq!(cluster_digest_items(vec![a, b]).len(), 1);
    }

    #[test]
    fn city_wide_items_with_related_titles_stay_separate() {
        // Both only carry the source default — nothing says they happened at
        // the same place, so "Brand in Wohnung" alone must not merge them.
        let a = db_item(
            "a",
            "Polizei Berlin",
            "Brand in Wohnung in Neukölln",
            "https://p.test/1",
        );
        let b = db_item(
            "b",
            "rbb Berlin",
            "Brand in Wohnung in Spandau",
            "https://r.test/2",
        );

        assert_eq!(cluster_digest_items(vec![a, b]).len(), 2);
    }

    #[test]
    fn links_compare_without_tracking_noise() {
        assert_eq!(
            canonical_link("https://www.rbb24.de/panorama/a.html?utm_source=rss#top"),
            canonical_link("http://rbb24.de/panorama/a.html/")
        );
        assert_ne!(
            canonical_link("https://example.test/article?id=1"),
            canonical_link("https://example.test/article?id=2")
        );
    }

    #[test]
    fn repeats_of_posted_stories_are_split_off_but_follow_ups_stay() {
        let posted = vec![db_item(
            "old",
            "Morgenpost",
            "Messerangriff in der Rigaer Straße",
            "https://m.test/1",
        )];
        let repeat = db_item(
            "r",
            "taz",
            "Messerangriff in der Rigaer Straße",
            "https://www.m.test/1?utm_medium=feed",
        );
        let follow_up = db_item(
            "f",
            "rbb Berlin",
            "Nach Messerangriff in der Rigaer Straße: Tatverdächtiger festgenommen",
            "https://r.test/9",
        );

        let (fresh, repeats) = split_already_posted(vec![repeat, follow_up], &posted);

        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].guid, "f");
        assert_eq!(repeats[0].guid, "r");
    }

    #[test]
    fn digest_shows_a_keyword_label_without_a_distance() {
        let item = DigestItem {
            guids: vec!["1".to_owned()],
            source_name: "Polizei Berlin".to_owned(),
            source_count: 1,
            title: "Einsatz".to_owned(),
            link: None,
            link_note: None,
            score: 2,
            max_score: 5,
            distance_meters: None,
            location_label: Some("Friedrichshain".to_owned()),
        };

        let (plain, _) = format_digest(&[item], "Test");

        assert!(plain.contains("[2/5 Friedrichshain]"));
    }
}
