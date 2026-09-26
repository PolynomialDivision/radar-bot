//! Geocoding (Nominatim), distances and street/place evidence.

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct GeocodeHit {
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    pub(crate) display_name: Option<String>,
    /// Ortsteil and Bezirk from Nominatim's address details, used to pick the
    /// right one of several same-named streets.
    pub(crate) suburb: Option<String>,
    pub(crate) borough: Option<String>,
}

/// Candidate → validated hits, best first. Empty = confirmed miss.
pub(crate) type GeocodeCache = Arc<DashMap<String, Vec<GeocodeHit>>>;

#[derive(Debug, Clone)]
pub(crate) enum DistanceLookup {
    Found(LocationEvidence),
    NoMatch,
    TransientFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EvidenceKind {
    /// The source's `base_implied_meters` prior — says nothing about where
    /// in the city the item is.
    SourceDefault,
    /// An `[[filter.area]]` term with its implied distance.
    Keyword,
    /// An `[[filter.places]]` entry with real coordinates.
    Place,
    /// A street mention geocoded via Nominatim.
    Geocoded,
}

/// One reason to believe an item is at a certain distance.
#[derive(Debug, Clone)]
pub(crate) struct LocationEvidence {
    pub(crate) meters: f64,
    pub(crate) label: String,
    pub(crate) kind: EvidenceKind,
    /// Found in the title, description or article lead rather than only
    /// further down the article body.
    pub(crate) prominent: bool,
}

impl LocationEvidence {
    /// Distance score, one tier lower for a mention buried in the article
    /// body — those are mostly asides, lists of places or teasers.
    pub(crate) fn score(&self) -> i32 {
        let score = distance_score(self.meters);
        if self.prominent {
            score
        } else {
            (score - 1).max(0)
        }
    }

    /// Real coordinates (as opposed to an implied or default distance).
    pub(crate) fn is_measured(&self) -> bool {
        matches!(self.kind, EvidenceKind::Place | EvidenceKind::Geocoded)
    }
}

/// The evidence to score an item by: highest score, then the more precise
/// kind, then the smaller distance.
pub(crate) fn best_evidence(evidence: Vec<LocationEvidence>) -> Option<LocationEvidence> {
    evidence.into_iter().max_by(|a, b| {
        a.score()
            .cmp(&b.score())
            .then(a.kind.cmp(&b.kind))
            .then(b.meters.total_cmp(&a.meters))
    })
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

const STREET_SUFFIXES: &[&str] = &[
    "straße", "strasse", "str.", "str", "allee", "weg", "platz", "ring", "damm", "gasse",
    "chaussee", "ufer", "brücke", "brucke", "steg",
];

/// Words that cannot be part of a street name in front of the suffix word.
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
    "einem",
    "einer",
    "eines",
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
    "neben",
    "hinter",
    "zwischen",
    "und",
    "oder",
    "sowie",
    "bzw",
    "diese",
    "dieser",
    "diesem",
    "diesen",
    "jede",
    "jeder",
    "jedem",
    "ecke",
    "höhe",
    "kreuzung",
    "richtung",
];

/// Adjectives that belong to compound street names ("Neue Bahnhofstraße").
const STREET_NAME_ADJECTIVES: &[&str] = &[
    "neue", "neuer", "alte", "alter", "kleine", "kleiner", "große", "grosse", "großer", "grosser",
    "lange", "langer", "hohe", "hoher", "obere", "oberer", "untere", "unterer",
];

fn trim_word(word: &str) -> &str {
    word.trim_start_matches(['(', '"', '\'', '„', '“', '‚', '»', '«', '[', '#'])
        .trim_end_matches([
            ',', '.', ';', ':', '!', '?', ')', '"', '\'', '“', '”', '«', '»', ']',
        ])
}

fn starts_uppercase(word: &str) -> bool {
    word.chars().next().is_some_and(char::is_uppercase)
}

/// Extract candidate German street names (with house number when given) from
/// plain text: "Rigaer Straße 94", "Scharnweberstraße", "Karl Marx Allee".
///
/// The name is taken as short as plausible — a compound word alone (plus a
/// leading "Neue"/"Alte"…), or up to two capitalized words before a separate
/// suffix word, stopping after an "-er"/"-sche" adjective like "Rigaer" — so
/// "Feuerwehreinsatz Rigaer Straße" yields "Rigaer Straße". Slashes separate
/// streets ("Frankfurter Allee/Warschauer Straße").
pub(crate) fn extract_street_candidates(text: &str) -> Vec<String> {
    let words: Vec<&str> = text
        .split(|c: char| c.is_whitespace() || c == '/')
        .map(trim_word)
        .filter(|w| !w.is_empty())
        .collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for (i, &word) in words.iter().enumerate() {
        let lc = word.to_lowercase();
        if !starts_uppercase(word) || !STREET_SUFFIXES.iter().any(|s| lc.ends_with(s)) {
            continue;
        }
        let bare = STREET_SUFFIXES.contains(&lc.as_str());
        let mut parts = vec![word];
        let mut j = i;
        while j > 0 && parts.len() < 3 {
            let prev = words[j - 1];
            let prev_lc = prev.to_lowercase();
            if !starts_uppercase(prev) || SKIP_LEADING.contains(&prev_lc.as_str()) {
                break;
            }
            if !bare && !STREET_NAME_ADJECTIVES.contains(&prev_lc.as_str()) {
                break;
            }
            parts.insert(0, prev);
            j -= 1;
            if !bare
                || prev_lc.ends_with("er")
                || prev_lc.ends_with("sche")
                || prev_lc.ends_with("sch")
            {
                break;
            }
        }
        if bare && parts.len() == 1 {
            continue; // just "Straße", "Platz", …
        }

        // Append house number (max 4 chars to avoid postal codes like "10247")
        if let Some(next) = words.get(i + 1) {
            if next.len() <= 4 && next.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                parts.push(next);
            }
        }

        let candidate = parts.join(" ");
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
    pub(crate) prominent: bool,
}

pub(crate) fn contains_any_normalized(text: &str, terms: &[&str]) -> bool {
    let n = normalize(text);
    terms.iter().any(|term| n.contains(&normalize(term)))
}

/// Where the article proper ends: page furniture, press-office contact
/// blocks — which carry the office's own address, e.g. "Hinweise nimmt …
/// Wedekindstraße 10, 10243 Berlin" under every witness appeal — and
/// related-article teasers.
const ARTICLE_END_MARKERS: &[&str] = &[
    "impressum",
    "datenschutz",
    "newsletter",
    "pressekontakt",
    "rückfragen bitte an",
    "rückfragen an",
    "hinweise nimmt",
    "hinweise nehmen",
    "hinweise bitte an",
    "original-content von",
    "mehr zum thema",
    "das könnte sie auch interessieren",
    "weitere artikel",
    "weitere meldungen",
    "auch interessant",
    "meistgelesen",
];

pub(crate) fn first_relevant_article_text(article: Option<&str>) -> &str {
    let Some(article) = article else { return "" };
    let lower = article.to_lowercase();
    let mut cutoff = ARTICLE_END_MARKERS
        .iter()
        .filter_map(|marker| marker_position(&lower, marker))
        .min()
        .unwrap_or(article.len())
        .min(article.len());
    while cutoff > 0 && !article.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    &article[..cutoff]
}

/// First position of `marker` as whole words in `lower`.
fn marker_position(lower: &str, marker: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(offset) = lower[from..].find(marker) {
        let start = from + offset;
        let end = start + marker.len();
        from = end;
        let before_ok = !lower[..start]
            .chars()
            .next_back()
            .is_some_and(char::is_alphanumeric);
        let after_ok = !lower[end..]
            .chars()
            .next()
            .is_some_and(char::is_alphanumeric);
        if before_ok && after_ok {
            return Some(start);
        }
    }
    None
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
        "description" | "lead" => 3,
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
    let (lead, rest) = split_lead(first_relevant_article_text(article));
    let sections = [
        ("title", title),
        ("description", description.unwrap_or("")),
        ("lead", lead),
        ("article", rest),
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
                    prominent: section != "article",
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
/// to anchor results, and the query is treated as a street mention from an
/// article: only results inside that city whose street or name actually is
/// the queried street are kept (see `validated_hits`). Pass "" for a full
/// address query, which returns Nominatim's results unfiltered.
///
/// Returns `Ok(hits)` (empty when nothing matched) and `Err(())` for
/// transient failures (network, timeout, parse). Only `Ok(_)` results should
/// be cached.
pub(crate) async fn geocode_location(
    http: &reqwest::Client,
    query: &str,
    city: &str,
) -> Result<Vec<GeocodeHit>, ()> {
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
    let results: serde_json::Value = serde_json::from_str(&body).map_err(|_| ())?;
    Ok(validated_hits(&results, query, city))
}

/// Address fields that name a street-like thing a mention can refer to.
const STREET_ADDRESS_FIELDS: &[&str] = &[
    "road",
    "pedestrian",
    "footway",
    "path",
    "square",
    "cycleway",
    "bridge",
    "place",
];

/// Address fields that name the municipality (or city-state) of a result.
const CITY_ADDRESS_FIELDS: &[&str] =
    &["city", "town", "village", "municipality", "state", "county"];

/// Filter Nominatim results. With a `city` anchor, a result must lie in that
/// city by its address fields — a "Berliner Straße" in Potsdam contains
/// "Berlin" in its display name but is not in Berlin — and must be the
/// street that was asked for: Nominatim happily answers "Parkplatz" or
/// "Verkäufer" with some unrelated POI, which used to score as a real place.
pub(crate) fn validated_hits(
    results: &serde_json::Value,
    query: &str,
    city: &str,
) -> Vec<GeocodeHit> {
    let Some(results) = results.as_array() else {
        return Vec::new();
    };
    let city_anchor = city
        .split(',')
        .next()
        .map(normalize)
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    let street = street_without_house_number(query);

    results
        .iter()
        .filter(|result| {
            let Some(anchor) = &city_anchor else {
                return true;
            };
            let address = &result["address"];
            let in_city = CITY_ADDRESS_FIELDS
                .iter()
                .filter_map(|field| address[*field].as_str())
                .any(|value| normalize(value).trim() == anchor);
            let names_street = std::iter::once(&result["name"])
                .chain(STREET_ADDRESS_FIELDS.iter().map(|field| &address[*field]))
                .filter_map(serde_json::Value::as_str)
                .any(|name| street_name_matches(street, name));
            in_city && names_street
        })
        .filter_map(|result| {
            let lat = result["lat"].as_str()?.parse().ok()?;
            let lon = result["lon"].as_str()?.parse().ok()?;
            let address = &result["address"];
            let field = |name: &str| address[name].as_str().map(str::to_owned);
            Some(GeocodeHit {
                lat,
                lon,
                display_name: result["display_name"].as_str().map(str::to_owned),
                suburb: field("suburb").or_else(|| field("quarter")),
                borough: field("borough").or_else(|| field("city_district")),
            })
        })
        .collect()
}

fn street_without_house_number(candidate: &str) -> &str {
    match candidate.rsplit_once(' ') {
        Some((street, last)) if last.chars().next().is_some_and(|c| c.is_ascii_digit()) => street,
        _ => candidate,
    }
}

fn canonical_street(name: &str) -> String {
    normalize(name)
        .replace('-', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether OSM `name` is the street in `candidate`. Extra leading words in
/// the candidate are tolerated ("Feuerwehreinsatz Rigaer Straße" names
/// "Rigaer Straße"); spelling variants (ß/ss, "Str."/"Straße", hyphens) are
/// normalized away.
pub(crate) fn street_name_matches(candidate: &str, name: &str) -> bool {
    let candidate = canonical_street(candidate);
    let name = canonical_street(name);
    // A bare "Allee"/"Straße" names nothing in particular.
    let specific = !name.is_empty() && !STREET_SUFFIXES.contains(&name.as_str());
    specific && (candidate == name || candidate.ends_with(&format!(" {name}")))
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

/// Find the most trustworthy geocodable street mention in the item and its
/// distance to `(ref_lat, ref_lon)`. Transient lookup failures stay separate
/// from "no geocodable street" so the caller can retry instead of permanently
/// dropping a potentially relevant item.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn find_nearest_distance(
    http: &reqwest::Client,
    item: &FeedItem,
    text: &ItemText,
    ref_lat: f64,
    ref_lon: f64,
    city: &str,
    cache: &GeocodeCache,
    limiter: &NominatimLimiter,
) -> DistanceLookup {
    let candidates = extract_street_evidence(
        &item.title,
        item.description.as_deref(),
        item.article_text.as_deref(),
    );
    if candidates.is_empty() {
        return DistanceLookup::NoMatch;
    }
    let mut best: Option<(i32, LocationEvidence)> = None;
    let mut transient_failure = false;

    for evidence in candidates.into_iter().take(MAX_GEOCODE_CANDIDATES) {
        if evidence.confidence < 3 {
            continue;
        }
        let candidate = evidence.candidate;
        let cached: Option<Vec<GeocodeHit>> = cache.get(&candidate).map(|v| v.clone());
        let hits = match cached {
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
                        Ok(hits) => {
                            cache.insert(candidate.clone(), hits.clone());
                            hits
                        }
                        Err(()) => {
                            warn!(
                                "geocode transient failure for {:?} — will retry next poll",
                                candidate
                            );
                            transient_failure = true;
                            Vec::new()
                        }
                    }
                }
            }
        };
        let Some(hit) = pick_hit(&hits, text) else {
            continue;
        };
        let dist = haversine_meters(ref_lat, ref_lon, hit.lat, hit.lon);
        if dist >= 20_000.0 {
            continue;
        }
        let replace = best.as_ref().is_none_or(|(best_conf, best_ev)| {
            evidence.confidence > *best_conf
                || (evidence.confidence == *best_conf && dist < best_ev.meters)
        });
        if replace {
            let label = hit
                .display_name
                .as_deref()
                .and_then(short_location_label)
                .unwrap_or(candidate);
            best = Some((
                evidence.confidence,
                LocationEvidence {
                    meters: dist,
                    label,
                    kind: EvidenceKind::Geocoded,
                    prominent: evidence.prominent,
                },
            ));
        }
    }
    if let Some((_, evidence)) = best {
        DistanceLookup::Found(evidence)
    } else if transient_failure {
        DistanceLookup::TransientFailure
    } else {
        DistanceLookup::NoMatch
    }
}

/// Nominatim's best hit, unless the item names the Ortsteil or Bezirk of
/// another one — many street names ("Bahnhofstraße", "Dorfstraße") exist
/// several times in one city.
pub(crate) fn pick_hit<'a>(hits: &'a [GeocodeHit], text: &ItemText) -> Option<&'a GeocodeHit> {
    hits.iter()
        .find(|hit| {
            [&hit.suburb, &hit.borough]
                .into_iter()
                .flatten()
                .any(|district| text.contains(district))
        })
        .or_else(|| hits.first())
}

/// Every `[[filter.places]]` entry the item mentions, by name or alias.
pub(crate) fn find_configured_places(
    text: &ItemText,
    filter: &FilterConfig,
    ref_point: Option<(f64, f64)>,
) -> Vec<LocationEvidence> {
    let Some((ref_lat, ref_lon)) = ref_point else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for place in &filter.places {
        let terms = std::iter::once(&place.name).chain(&place.aliases);
        let mut mention: Option<bool> = None;
        for term in terms {
            let term = normalize(term);
            if contains_normalized_term(&text.prominent, &term) {
                mention = Some(true);
                break;
            }
            if contains_normalized_term(&text.rest, &term) {
                mention = Some(false);
            }
        }
        if let Some(prominent) = mention {
            out.push(LocationEvidence {
                meters: haversine_meters(ref_lat, ref_lon, place.lat, place.lon),
                label: place.name.clone(),
                kind: EvidenceKind::Place,
                prominent,
            });
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn street_candidates_take_the_name_not_the_words_before_it() {
        let c = extract_street_candidates;

        assert_eq!(
            c("Feuerwehreinsatz Rigaer Straße 94"),
            vec!["Rigaer Straße 94"]
        );
        assert_eq!(
            c("Brand in der Scharnweberstraße."),
            vec!["Scharnweberstraße"]
        );
        assert_eq!(
            c("Kreuzung Frankfurter Allee/Warschauer Straße"),
            vec!["Frankfurter Allee", "Warschauer Straße"]
        );
        assert_eq!(c("Demo über die Karl Marx Allee"), vec!["Karl Marx Allee"]);
        assert_eq!(
            c("Unfall (Neue Bahnhofstraße 5)"),
            vec!["Neue Bahnhofstraße 5"]
        );
        assert!(c("das Risiko ist gering").is_empty());
        assert!(c("an der Straße").is_empty());
    }

    fn nominatim(name: &str, class: &str, address: serde_json::Value) -> serde_json::Value {
        json!({"lat": "52.5", "lon": "13.4", "class": class, "name": name,
               "display_name": name, "address": address})
    }

    #[test]
    fn geocode_results_must_be_the_named_street_in_the_city() {
        let results = json!([
            // A POI Nominatim returns for a generic word — not a street.
            nominatim(
                "",
                "amenity",
                json!({"road": "Grünberger Straße", "city": "Berlin"})
            ),
            // Right name, wrong city.
            nominatim(
                "Rigaer Straße",
                "highway",
                json!({"road": "Rigaer Straße", "city": "Potsdam", "state": "Brandenburg"})
            ),
            nominatim(
                "Rigaer Straße",
                "highway",
                json!({"road": "Rigaer Straße", "suburb": "Friedrichshain",
                                                         "borough": "Friedrichshain-Kreuzberg", "city": "Berlin"})
            ),
            // House-number hit: no name, but on the street.
            nominatim(
                "",
                "place",
                json!({"house_number": "94", "road": "Rigaer Straße", "city": "Berlin"})
            ),
        ]);

        let hits = validated_hits(
            &results,
            "Feuerwehreinsatz Rigaer Straße 94",
            "Berlin, Germany",
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].suburb.as_deref(), Some("Friedrichshain"));

        assert!(validated_hits(&results, "Parkplatz", "Berlin, Germany").is_empty());
        // The reference address is geocoded without anchor and kept as-is.
        assert_eq!(
            validated_hits(&results, "Scharnweberstraße 38, 10247 Berlin", "").len(),
            4
        );
    }

    #[test]
    fn street_names_match_across_spelling_variants() {
        assert!(street_name_matches("Rigaer Str", "Rigaer Straße"));
        assert!(street_name_matches("Karl Marx Allee", "Karl-Marx-Allee"));
        assert!(street_name_matches(
            "Scharnweberstrasse",
            "Scharnweberstraße"
        ));
        assert!(!street_name_matches("Parkplatz", "Grünberger Straße"));
        assert!(!street_name_matches("Große Frankfurter Allee", "Allee"));
    }

    #[test]
    fn a_mentioned_district_picks_between_same_named_streets() {
        let hit = |suburb: &str, lat: f64| GeocodeHit {
            lat,
            lon: 13.4,
            display_name: None,
            suburb: Some(suburb.to_owned()),
            borough: None,
        };
        let hits = vec![hit("Mahlsdorf", 52.51), hit("Köpenick", 52.44)];
        let text = |title: &str| ItemText {
            prominent: normalize(title),
            rest: String::new(),
        };

        assert_eq!(
            pick_hit(&hits, &text("Brand in der Bahnhofstraße in Köpenick"))
                .unwrap()
                .lat,
            52.44
        );
        assert_eq!(
            pick_hit(&hits, &text("Brand in der Bahnhofstraße"))
                .unwrap()
                .lat,
            52.51
        );
    }

    #[test]
    fn best_evidence_prefers_score_then_precision() {
        let ev = |meters, kind, prominent| LocationEvidence {
            meters,
            label: String::new(),
            kind,
            prominent,
        };

        let best = best_evidence(vec![
            ev(9_000.0, EvidenceKind::SourceDefault, true),
            ev(1_400.0, EvidenceKind::Geocoded, false),
            ev(1_500.0, EvidenceKind::Keyword, true),
        ])
        .unwrap();
        assert_eq!(best.kind, EvidenceKind::Keyword);

        let best = best_evidence(vec![
            ev(1_500.0, EvidenceKind::Keyword, true),
            ev(1_900.0, EvidenceKind::Place, true),
        ])
        .unwrap();
        assert_eq!(best.kind, EvidenceKind::Place);
    }

    #[test]
    fn article_ends_at_contact_block_but_not_at_inner_words() {
        let text = "Brand in der Rigaer Straße. Der Kontaktbereichsbeamte war vor Ort. \
                    Rückfragen bitte an: Pressestelle, Platz der Luftbrücke 6";

        let relevant = first_relevant_article_text(Some(text));

        assert!(relevant.contains("Kontaktbereichsbeamte"));
        assert!(!relevant.contains("Luftbrücke"));
    }
}
