//! Geocoding (Nominatim), distances and street/place evidence.

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct GeocodeHit {
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    pub(crate) display_name: Option<String>,
}

pub(crate) type GeocodeCache = Arc<DashMap<String, Option<GeocodeHit>>>;

#[derive(Debug, Clone)]
pub(crate) enum DistanceLookup {
    Found(f64, String),
    NoMatch,
    TransientFailure,
}
// ── Geocoding + distance ──────────────────────────────────────────────────────

pub(crate) fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    R * 2.0 * a.sqrt().asin()
}

// Distance tier boundaries (metres) used by distance_score() and geocoding early-exit.
pub(crate) const DIST_SCORE_5: f64 = 200.0; // 🔴 very close
pub(crate) const DIST_SCORE_4: f64 = 500.0; // 🟠
pub(crate) const DIST_SCORE_3: f64 = 1_000.0; // 🟡
pub(crate) const DIST_SCORE_2: f64 = 2_000.0; // 🟢
pub(crate) const DIST_SCORE_1: f64 = 10_000.0; // 🔵 city-wide

pub(crate) fn format_distance(m: f64) -> String {
    if m < 1_000.0 {
        format!("~{}m", m.round() as u32)
    } else {
        format!("~{:.1}km", m / 1_000.0)
    }
}

pub(crate) fn distance_score(m: f64) -> i32 {
    if m < DIST_SCORE_5 {
        5
    } else if m < DIST_SCORE_4 {
        4
    } else if m < DIST_SCORE_3 {
        3
    } else if m < DIST_SCORE_2 {
        2
    } else if m < DIST_SCORE_1 {
        1
    } else {
        0
    }
}

pub(crate) fn score_color(score: i32) -> &'static str {
    match score {
        s if s >= 5 => "🔴",
        4 => "🟠",
        3 => "🟡",
        2 => "🟢",
        1 => "🔵",
        _ => "⚪",
    }
}

/// Extract candidate German street names from plain text.
pub(crate) fn extract_street_candidates(text: &str) -> Vec<String> {
    const SUFFIXES: &[&str] = &[
        "straße", "strasse", "str.", "allee", "weg", "platz", "ring", "damm", "gasse", "chaussee",
        "ufer", "brücke", "brucke", "steg",
    ];
    // Words that cannot be the first word of a street name
    const SKIP_LEADING: &[&str] = &[
        "auf",
        "der",
        "die",
        "das",
        "dem",
        "den",
        "des",
        "ein",
        "eine",
        "einen",
        "am",
        "im",
        "zur",
        "zum",
        "vom",
        "von",
        "an",
        "in",
        "zu",
        "bei",
        "nach",
        "über",
        "unter",
        "vor",
        "durch",
        "entlang",
        "bis",
        "um",
        "seit",
        "ab",
        "außer",
        "gegenüber",
        "nahe",
        // conjunctions — prevent "und Erreichbarkeit Platz" style false positives
        "und",
        "oder",
        "sowie",
        "bzw",
        // indefinite articles (cases not covered above)
        "einem",
        "einer",
        "eines",
    ];

    let words: Vec<&str> = text.split_whitespace().collect();
    let n = words.len();
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for i in 0..n {
        let lc = words[i].to_lowercase();
        let clean = lc.trim_end_matches(',').trim_end_matches('.');
        if !SUFFIXES.iter().any(|s| clean.ends_with(s) || clean == *s) {
            continue;
        }

        // Take up to 2 preceding words + current, then strip leading prepositions/articles
        let start = i.saturating_sub(2);
        let raw: Vec<&str> = words[start..=i].to_vec();
        let parts: Vec<&str> = {
            let mut p = raw.as_slice();
            while !p.is_empty() {
                let head = p[0].to_lowercase();
                let head = head.trim_end_matches(',').trim_end_matches('.');
                if SKIP_LEADING.contains(&head) {
                    p = &p[1..];
                } else {
                    break;
                }
            }
            p.to_vec()
        };

        if parts.is_empty() {
            continue;
        }

        // Skip bare suffix words with no name (e.g. just "Straße", "Platz", "Weg")
        if parts.len() == 1 {
            let bare = parts[0].to_lowercase();
            let bare = bare.trim_end_matches(',').trim_end_matches('.');
            if SUFFIXES.contains(&bare) {
                continue;
            }
        }

        // Append house number (max 4 digits to avoid postal codes like "612101")
        let mut all_parts = parts.clone();
        if i + 1 < n {
            let next = words[i + 1].trim_end_matches(',');
            if next.len() <= 4 && next.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                all_parts.push(next);
            }
        }

        // Strip trailing punctuation from each part so "Luftbrücke," and
        // "Luftbrücke" produce the same candidate string and cache key.
        let candidate = all_parts
            .iter()
            .map(|w| w.trim_end_matches([',', '.', ';', ':', '!', '?']))
            .collect::<Vec<_>>()
            .join(" ");
        if candidate.len() >= 5 && seen.insert(candidate.to_lowercase()) {
            out.push(candidate);
        }
    }
    out
}

#[derive(Debug, Clone)]
pub(crate) struct StreetEvidence {
    pub(crate) candidate: String,
    pub(crate) confidence: i32,
}

pub(crate) fn contains_any_normalized(text: &str, terms: &[&str]) -> bool {
    let n = normalize(text);
    terms.iter().any(|term| n.contains(&normalize(term)))
}

pub(crate) fn first_relevant_article_text(article: Option<&str>) -> &str {
    let Some(article) = article else { return "" };
    let lower = article.to_lowercase();
    let mut cutoff = [
        "impressum",
        "kontakt",
        "datenschutz",
        "newsletter",
        "pressekontakt",
    ]
    .iter()
    .filter_map(|needle| lower.find(needle))
    .min()
    .unwrap_or(article.len());
    while cutoff > 0 && !article.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    &article[..cutoff]
}

pub(crate) fn street_confidence(section: &str, text: &str) -> i32 {
    const EVENT_TERMS: &[&str] = &[
        "brand",
        "feuer",
        "unfall",
        "verletz",
        "polizei",
        "sperr",
        "einsatz",
        "warnung",
        "raub",
        "diebstahl",
        "überfall",
        "messer",
        "schuss",
        "verkehr",
        "störung",
        "rettung",
        "evaku",
        "gefähr",
    ];
    let mut score = match section {
        "title" => 4,
        "description" => 3,
        _ => 2,
    };
    if contains_any_normalized(text, EVENT_TERMS) {
        score += 2;
    }
    score
}

pub(crate) fn extract_street_evidence(
    title: &str,
    description: Option<&str>,
    article: Option<&str>,
) -> Vec<StreetEvidence> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let sections = [
        ("title", title),
        ("description", description.unwrap_or("")),
        ("article", first_relevant_article_text(article)),
    ];

    for (section, text) in sections {
        if text.trim().is_empty() {
            continue;
        }
        let confidence = street_confidence(section, text);
        for candidate in extract_street_candidates(text) {
            let key = candidate.to_lowercase();
            if seen.insert(key) {
                out.push(StreetEvidence {
                    candidate,
                    confidence,
                });
            }
        }
    }

    out
}

pub(crate) fn short_location_label(display_name: &str) -> Option<String> {
    let parts: Vec<&str> = display_name
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    if parts.is_empty() {
        return None;
    }
    let label = parts.iter().take(3).copied().collect::<Vec<_>>().join(", ");
    if label.is_empty() {
        None
    } else {
        Some(label)
    }
}

pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Geocode a query string via Nominatim. If `city` is non-empty it is appended
/// to anchor results (e.g. street lookups). Pass "" for a full address query.
///
/// Returns `Ok(Some(coords))` on hit, `Ok(None)` when Nominatim confirms the
/// address doesn't exist, and `Err(())` for transient failures (network,
/// timeout, parse). Only `Ok(_)` results should be cached.
pub(crate) async fn geocode_location(
    http: &reqwest::Client,
    query: &str,
    city: &str,
) -> Result<Option<GeocodeHit>, ()> {
    let full = if city.is_empty() {
        query.to_owned()
    } else {
        format!("{query}, {city}")
    };
    let q = url_encode(&full);
    let url = format!(
        "https://nominatim.openstreetmap.org/search?q={q}&format=json&addressdetails=1&limit=5"
    );
    let resp: reqwest::Response = tokio::time::timeout(
        NOMINATIM_TIMEOUT,
        http.get(&url).header("Accept-Language", "de").send(),
    )
    .await
    .map_err(|_| ())?
    .map_err(|_| ())?;
    let body = tokio::time::timeout(NOMINATIM_TIMEOUT, resp.text())
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    let arr: serde_json::Value = serde_json::from_str(&body).map_err(|_| ())?;
    let Some(results) = arr.as_array() else {
        return Ok(None);
    };
    let city_anchor = city
        .split(',')
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    for result in results {
        let display_name = result["display_name"].as_str().map(str::to_owned);
        if let Some(anchor) = city_anchor {
            let haystack = normalize(&format!(
                "{} {}",
                display_name.as_deref().unwrap_or(""),
                result["address"]
            ));
            if !haystack.contains(&normalize(anchor)) {
                continue;
            }
        }
        let lat: f64 = match result["lat"].as_str().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };
        let lon: f64 = match result["lon"].as_str().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => continue,
        };
        return Ok(Some(GeocodeHit {
            lat,
            lon,
            display_name,
        }));
    }

    Ok(None)
}

// ── Nominatim rate limiter ────────────────────────────────────────────────────

pub(crate) const NOMINATIM_INTERVAL: Duration = Duration::from_millis(1_100);
pub(crate) const NOMINATIM_TIMEOUT: Duration = Duration::from_secs(8);
pub(crate) const MAX_GEOCODE_CANDIDATES: usize = 60;
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// One-permit semaphore: only one geocode request runs at a time.
/// The holder sleeps NOMINATIM_INTERVAL after the request before releasing,
/// so the rate never exceeds 1 req/s regardless of how many tasks are waiting.
pub(crate) type NominatimLimiter = Arc<Semaphore>;

/// Find the closest geocodable street mention in `text` to `(ref_lat, ref_lon)`.
/// Transient lookup failures stay separate from "no geocodable street" so the
/// caller can retry instead of permanently dropping a potentially relevant item.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn find_nearest_distance(
    http: &reqwest::Client,
    title: &str,
    description: Option<&str>,
    article: Option<&str>,
    ref_lat: f64,
    ref_lon: f64,
    city: &str,
    cache: &GeocodeCache,
    limiter: &NominatimLimiter,
) -> DistanceLookup {
    let candidates = extract_street_evidence(title, description, article);
    if candidates.is_empty() {
        return DistanceLookup::NoMatch;
    }
    let mut best: Option<(i32, f64, String)> = None;
    let mut transient_failure = false;

    for evidence in candidates.into_iter().take(MAX_GEOCODE_CANDIDATES) {
        if evidence.confidence < 3 {
            continue;
        }
        let candidate = evidence.candidate;
        // Check cache first — Some(Some(coords)) = hit, Some(None) = confirmed miss.
        let cached: Option<Option<GeocodeHit>> = cache.get(&candidate).map(|v| v.clone());
        let coords = match cached {
            Some(v) => v,
            None => {
                // Acquire the single geocoding permit. Tasks queue here instead of
                // pre-reserving time slots, so wait time stays bounded.
                let _permit = limiter.acquire().await.unwrap();
                // Re-check: another task may have geocoded this while we waited.
                if let Some(v) = cache.get(&candidate).map(|v| v.clone()) {
                    v // cache hit — release permit immediately, no sleep needed
                } else {
                    let result = geocode_location(http, &candidate, city).await;
                    sleep(NOMINATIM_INTERVAL).await; // enforce rate limit before releasing permit
                    match result {
                        Ok(coords) => {
                            cache.insert(candidate.clone(), coords.clone());
                            coords
                        }
                        Err(()) => {
                            warn!(
                                "geocode transient failure for {:?} — will retry next poll",
                                candidate
                            );
                            transient_failure = true;
                            None
                        }
                    }
                }
            }
        };
        if let Some(hit) = coords {
            let dist = haversine_meters(ref_lat, ref_lon, hit.lat, hit.lon);
            if dist < 20_000.0 {
                let label = hit
                    .display_name
                    .as_deref()
                    .and_then(short_location_label)
                    .unwrap_or(candidate);
                let replace = best.as_ref().is_none_or(|(best_conf, best_dist, _)| {
                    evidence.confidence > *best_conf
                        || (evidence.confidence == *best_conf && dist < *best_dist)
                });
                if replace {
                    best = Some((evidence.confidence, dist, label));
                }
            }
        }
    }
    if let Some((_, dist, label)) = best {
        DistanceLookup::Found(dist, label)
    } else if transient_failure {
        DistanceLookup::TransientFailure
    } else {
        DistanceLookup::NoMatch
    }
}

pub(crate) fn find_configured_place_distance(
    item: &FeedItem,
    filter: &FilterConfig,
    ref_point: Option<(f64, f64)>,
) -> Option<(f64, String)> {
    let (ref_lat, ref_lon) = ref_point?;
    let text = normalize(&format!(
        "{} {} {}",
        item.title,
        item.description.as_deref().unwrap_or(""),
        first_relevant_article_text(item.article_text.as_deref())
    ));

    let mut best: Option<(f64, String)> = None;
    for place in &filter.places {
        let mut terms = Vec::with_capacity(place.aliases.len() + 1);
        terms.push(place.name.as_str());
        terms.extend(place.aliases.iter().map(String::as_str));

        if terms
            .iter()
            .any(|term| contains_normalized_term(&text, &normalize(term)))
        {
            let dist = haversine_meters(ref_lat, ref_lon, place.lat, place.lon);
            if best.as_ref().is_none_or(|(best_dist, _)| dist < *best_dist) {
                best = Some((dist, place.name.clone()));
            }
        }
    }
    best
}

pub(crate) fn contains_normalized_term(text: &str, term: &str) -> bool {
    if term.trim().is_empty() {
        return false;
    }

    let mut search_start = 0;
    while let Some(offset) = text[search_start..].find(term) {
        let start = search_start + offset;
        let end = start + term.len();

        let before = text[..start].chars().next_back();
        let after = text[end..].chars().next();
        if !before.is_some_and(char::is_alphanumeric) && !after.is_some_and(char::is_alphanumeric) {
            return true;
        }

        search_start = end;
    }

    false
}
