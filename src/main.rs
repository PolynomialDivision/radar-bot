mod sources;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use dashmap::DashMap;
use tokio::sync::{Mutex, Semaphore};

use anyhow::{Context, Result};
use chrono::{Local, NaiveTime};
use futures_util::StreamExt;
use matrix_sdk::{
    Client, Room, RoomState, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    encryption::verification::{
        SasState, Verification, VerificationRequest, VerificationRequestState,
    },
    ruma::{
        OwnedDeviceId, OwnedServerName, OwnedUserId, RoomOrAliasId,
        api::client::filter::FilterDefinition,
        events::{
            key::verification::request::ToDeviceKeyVerificationRequestEvent,
            room::{
                member::StrippedRoomMemberEvent,
                message::{MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent},
            },
        },
    },
};
use matrix_sdk_crypto::CollectStrategy;

type GeocodeCache = Arc<DashMap<String, Option<(f64, f64)>>>;
use serde::{Deserialize, Serialize};
use tokio::{fs, task::JoinSet, time::sleep, time::Duration};
use tracing::{error, info, warn};

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Config {
    matrix: MatrixConfig,
    #[serde(default)]
    security: SecurityConfig,
    #[serde(default)]
    schedule: ScheduleConfig,
    #[serde(default)]
    filter: FilterConfig,
    #[serde(default)]
    sources: Vec<SourceConfig>,
    #[serde(default)]
    bluesky: BlueskyConfig,
}

#[derive(Deserialize, Default)]
struct BlueskyConfig {
    identifier: Option<String>,
    password: Option<String>,
}

#[derive(Deserialize)]
struct MatrixConfig {
    homeserver: String,
    user_id: String,
    access_token: String,
    device_id: String,
    recovery_key: Option<String>,
}

#[derive(Deserialize, Clone)]
struct SourceConfig {
    /// Display name shown in posted messages.
    name: String,
    /// Source type. Defaults to "rss" so existing configs need no changes.
    #[serde(rename = "type", default)]
    source_type: sources::SourceType,
    // ── RSS / Atom ────────────────────────────────────────────────────────────
    /// Feed URL (required for type = "rss").
    #[serde(default)]
    url: Option<String>,
    // ── Bluesky ───────────────────────────────────────────────────────────────
    /// Keyword or hashtag query (required for type = "bluesky"), e.g. "#Berlin".
    #[serde(default)]
    query: Option<String>,
    /// Max posts per poll for Bluesky (default 25, API max 100).
    #[serde(default)]
    limit: Option<usize>,
    /// Only fetch posts newer than this many hours (Bluesky only, default 24).
    #[serde(default)]
    max_age_hours: Option<u64>,
    // ── Shared ────────────────────────────────────────────────────────────────
    /// Apply location filter (default true). Set false for hyper-local sources.
    #[serde(default = "default_true")]
    filter: bool,
    /// Implied distance (metres) used as fallback when neither area keywords
    /// nor geocoding produce a result.
    #[serde(default)]
    base_implied_meters: Option<f64>,
    /// Per-source required keywords. When set, overrides the global filter.required.
    /// Set to [] to skip the required check for city-specific sources.
    #[serde(default)]
    required: Option<Vec<String>>,
}

/// Keyword group with an implied distance. When no street can be geocoded,
/// the closest matching group's implied_meters is used as the fallback distance.
#[derive(Deserialize, Clone)]
struct AreaGroup {
    /// Assumed distance from the reference point in metres (e.g. 300 for your street,
    /// 1500 for your district). Lower = more relevant.
    implied_meters: f64,
    #[serde(default)]
    terms: Vec<String>,
}

/// Filtering and scoring configuration.
///   - blocklist match → always drop
///   - required: at least one must match (or list is empty)
///   - area groups: find closest matching group → implied distance fallback
///   - geocoding can override with an actual distance (always takes the closer result)
///   - distance_score(final_meters) < digest_threshold → drop
#[derive(Deserialize, Default, Clone)]
struct FilterConfig {
    #[serde(default)]
    required: Vec<String>,
    #[serde(default)]
    blocklist: Vec<String>,
    #[serde(default = "default_digest_threshold")]
    digest_threshold: i32,
    #[serde(default)]
    area: Vec<AreaGroup>,
    /// Full address used as the reference point for distance scoring.
    /// Geocoded once at startup via Nominatim.
    reference_address: Option<String>,
    /// City string appended to Nominatim queries when geocoding street mentions
    /// from articles (e.g. "Berlin, Germany"). Anchors results to the right city.
    #[serde(default)]
    geocode_city: Option<String>,
}

fn default_digest_threshold() -> i32 { 1 }

#[derive(Deserialize, Clone)]
struct ScheduleConfig {
    /// How often to poll all sources (minutes).
    #[serde(default = "default_poll_interval")]
    poll_interval_minutes: u64,
    /// Times of day to post a digest (HH:MM, 24h). Each fires independently;
    /// the queue is drained on every posting so digests never overlap.
    #[serde(default = "default_digest_times")]
    digest_times: Vec<String>,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            poll_interval_minutes: default_poll_interval(),
            digest_times: default_digest_times(),
        }
    }
}

fn default_poll_interval() -> u64 { 30 }
fn default_digest_times() -> Vec<String> { vec!["08:00".to_owned()] }
fn default_true() -> bool { true }

#[derive(Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum EncryptionStrategy {
    AllDevices,
    #[default]
    IdentityBased,
    OnlyTrusted,
}

impl From<EncryptionStrategy> for CollectStrategy {
    fn from(s: EncryptionStrategy) -> Self {
        match s {
            EncryptionStrategy::AllDevices => CollectStrategy::AllDevices,
            EncryptionStrategy::IdentityBased => CollectStrategy::IdentityBasedStrategy,
            EncryptionStrategy::OnlyTrusted => CollectStrategy::OnlyTrustedDevices,
        }
    }
}

#[derive(Deserialize, Default)]
struct SecurityConfig {
    #[serde(default)]
    allowed_inviters: Vec<String>,
    #[serde(default)]
    admin_users: Vec<String>,
    #[serde(default)]
    encryption_strategy: EncryptionStrategy,
}

// ── Bot state ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct BotState {
    bot_user_id: OwnedUserId,
    allowed_inviters: HashSet<OwnedUserId>,
    admin_users: HashSet<OwnedUserId>,
    reset_allowed: Arc<Mutex<HashSet<OwnedUserId>>>,
}

// ── Feed item ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FeedItem {
    pub(crate) guid: String,
    pub(crate) title: String,
    pub(crate) link: Option<String>,
    /// Short snippet from the RSS/social feed — shown to the user.
    pub(crate) description: Option<String>,
    /// Full stripped text used only for filtering/geocoding. Pre-filled by
    /// adapters that already have the full text (e.g. Bluesky posts).
    #[serde(default)]
    pub(crate) article_text: Option<String>,
    pub(crate) source_name: String,
    #[serde(default)]
    pub(crate) score: i32,
    /// Sum of all possible area group scores (from config).
    #[serde(default)]
    pub(crate) max_score: i32,
    /// Distance in metres from the reference point (if geocoded).
    #[serde(default)]
    pub(crate) distance_meters: Option<f64>,
}

// ── HTML utilities ────────────────────────────────────────────────────────────

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Strip HTML tags and decode entities from RSS descriptions.
fn strip_html(html: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    let out = decode_entities(&out);
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn decode_entities(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        if let Some(semi) = rest.find(';') {
            let entity = &rest[1..semi]; // between & and ;
            let replaced = if let Some(hex) = entity.strip_prefix("#x").or_else(|| entity.strip_prefix("#X")) {
                u32::from_str_radix(hex, 16).ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
            } else if let Some(dec) = entity.strip_prefix('#') {
                dec.parse::<u32>().ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
            } else {
                match entity {
                    "amp"    => Some("&".into()),
                    "lt"     => Some("<".into()),
                    "gt"     => Some(">".into()),
                    "quot"   => Some("\"".into()),
                    "apos"   => Some("'".into()),
                    "nbsp"   => Some(" ".into()),
                    "shy"    => Some("".into()),
                    // dashes & ellipsis
                    "mdash"  => Some("—".into()),
                    "ndash"  => Some("–".into()),
                    "hellip" => Some("…".into()),
                    // typographic quotes
                    "ldquo"  => Some("\u{201C}".into()),
                    "rdquo"  => Some("\u{201D}".into()),
                    "lsquo"  => Some("\u{2018}".into()),
                    "rsquo"  => Some("\u{2019}".into()),
                    "laquo"  => Some("«".into()),
                    "raquo"  => Some("»".into()),
                    // German-specific
                    "auml"   => Some("ä".into()),
                    "ouml"   => Some("ö".into()),
                    "uuml"   => Some("ü".into()),
                    "Auml"   => Some("Ä".into()),
                    "Ouml"   => Some("Ö".into()),
                    "Uuml"   => Some("Ü".into()),
                    "szlig"  => Some("ß".into()),
                    // other common
                    "eacute" => Some("é".into()),
                    "egrave" => Some("è".into()),
                    "ecirc"  => Some("ê".into()),
                    "euro"   => Some("€".into()),
                    "pound"  => Some("£".into()),
                    "copy"   => Some("©".into()),
                    "reg"    => Some("®".into()),
                    "trade"  => Some("™".into()),
                    "bull"   => Some("•".into()),
                    "middot" => Some("·".into()),
                    _        => None,
                }
            };
            if let Some(r) = replaced {
                out.push_str(&r);
                rest = &rest[semi + 1..];
            } else {
                // unknown entity — emit as-is
                out.push('&');
                rest = &rest[1..];
            }
        } else {
            // no closing semicolon — emit literally
            out.push('&');
            rest = &rest[1..];
        }
    }
    out.push_str(rest);
    out
}

// ── RSS / Atom parser ─────────────────────────────────────────────────────────

fn extract_between<'a>(s: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = s.find(open)? + open.len();
    let end = s[start..].find(close).map(|i| start + i)?;
    Some(&s[start..end])
}

fn extract_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    extract_between(xml, &format!("<{tag}>"), &format!("</{tag}>"))
}

fn unwrap_cdata(raw: &str) -> &str {
    let s = raw.trim();
    if s.starts_with("<![CDATA[") && s.ends_with("]]>") {
        &s[9..s.len() - 3]
    } else {
        s
    }
}

fn extract_text(xml: &str, tag: &str) -> Option<String> {
    let raw = extract_tag(xml, tag)?;
    let text = strip_html(unwrap_cdata(raw)).trim().to_owned();
    if text.is_empty() { None } else { Some(text) }
}

/// Extract href="..." from an Atom <link> element.
fn extract_atom_link(xml: &str) -> Option<String> {
    let mut pos = 0;
    while let Some(rel) = xml[pos..].find("<link") {
        let start = pos + rel;
        let end = xml[start..].find('>').map(|i| start + i + 1).unwrap_or(xml.len());
        let element = &xml[start..end];
        if let Some(href_pos) = element.find("href=\"") {
            let val_start = href_pos + 6;
            if let Some(val_end) = element[val_start..].find('"').map(|i| val_start + i) {
                return Some(element[val_start..val_end].to_owned());
            }
        }
        pos = end;
    }
    None
}

pub(crate) fn parse_feed(xml: &str, source_name: &str) -> Vec<FeedItem> {
    // RSS 2.0 feeds often declare xmlns:atom="..." for <atom:link rel="self"> — only
    // treat as Atom when the root element is <feed>, not <rss>
    let is_atom = !xml.contains("<rss") && xml.contains("http://www.w3.org/2005/Atom");
    // Match "<item" not "<item>" to handle attributes like <item rdf:about="...">
    let (open_tag, close_tag) = if is_atom { ("<entry", "</entry>") } else { ("<item", "</item>") };

    let mut items = Vec::new();
    let mut pos = 0;

    while let Some(rel) = xml[pos..].find(open_tag) {
        let start = pos + rel;
        let end = match xml[start..].find(close_tag) {
            Some(i) => start + i + close_tag.len(),
            None => break,
        };
        let block = &xml[start..end];
        pos = end;

        let title = match extract_text(block, "title") {
            Some(t) => t,
            None => continue,
        };

        let link = if is_atom {
            extract_atom_link(block).or_else(|| extract_text(block, "link"))
        } else {
            extract_text(block, "link")
        };

        let description = if is_atom {
            extract_text(block, "summary").or_else(|| extract_text(block, "content"))
        } else {
            extract_text(block, "description")
                .or_else(|| extract_text(block, "content:encoded"))
        };

        let guid = if is_atom { extract_text(block, "id") } else { extract_text(block, "guid") }
            .or_else(|| link.clone())
            .unwrap_or_else(|| format!("{source_name}::{title}"));

        items.push(FeedItem { guid, title, link, description, article_text: None, source_name: source_name.to_owned(), score: 0, max_score: 0, distance_meters: None });
    }

    items
}

// ── Article fetcher ───────────────────────────────────────────────────────────

/// Return the inner HTML of the first `<tag_name …>…</tag_name>` block.
fn find_tag_inner<'a>(html: &'a str, tag_name: &str) -> Option<&'a str> {
    let lower = html.to_lowercase();
    let open_str = format!("<{tag_name}");
    let close_str = format!("</{tag_name}>");
    let s = lower.find(&open_str)?;
    let gt = html[s..].find('>')?;
    let content_start = s + gt + 1;
    let e = lower[content_start..].find(&close_str)?;
    Some(&html[content_start..content_start + e])
}

/// Remove all `<tag>…</tag>` blocks from html (nav, header, footer, aside, script, style).
fn remove_blocks(html: &str, tags: &[&str]) -> String {
    let mut result = html.to_owned();
    for tag in tags {
        let open_str = format!("<{tag}");
        let close_str = format!("</{tag}>");
        // Keep both original and lowercase in sync so to_lowercase() runs once per removal,
        // not once per loop iteration over the whole string.
        let mut lower = result.to_lowercase();
        loop {
            match (lower.find(&open_str), lower.find(&close_str)) {
                (Some(s), Some(e)) if s < e => {
                    let end = e + close_str.len();
                    result = format!("{}{}", &result[..s], &result[end..]);
                    lower  = format!("{}{}", &lower[..s],  &lower[end..]);
                }
                _ => break,
            }
        }
    }
    result
}

/// Extract only the article body from a full HTML page.
/// Tries semantic containers first, then strips navigation blocks.
fn extract_article_body(html: &str) -> String {
    for tag in &["article", "main"] {
        if let Some(inner) = find_tag_inner(html, tag) {
            let t = strip_html(inner);
            if t.trim().len() > 200 {
                return t;
            }
        }
    }
    let cleaned = remove_blocks(html, &["script", "style", "nav", "header", "footer", "aside"]);
    strip_html(&cleaned)
}

/// Fetch a URL and return the article body as plain text.
async fn fetch_article_text(http: &reqwest::Client, url: &str) -> Option<String> {
    let resp = tokio::time::timeout(
        Duration::from_secs(10),
        http.get(url).send(),
    )
    .await.ok()?.ok()?;

    let html = tokio::time::timeout(
        Duration::from_secs(10),
        resp.text(),
    )
    .await.ok()?.ok()?;

    // extract_article_body calls remove_blocks which is CPU-intensive on large HTML pages.
    // Run it on the blocking thread pool so it doesn't stall the async runtime.
    let text = tokio::task::spawn_blocking(move || {
        let t = extract_article_body(&html);
        let t = t.trim().to_owned();
        if t.is_empty() { None } else { Some(t) }
    }).await.ok()??;

    Some(text)
}

// ── Geocoding + distance ──────────────────────────────────────────────────────

fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    R * 2.0 * a.sqrt().asin()
}

// Distance tier boundaries (metres) used by distance_score() and geocoding early-exit.
const DIST_SCORE_5: f64 =    200.0; // 🔴 very close
const DIST_SCORE_4: f64 =    500.0; // 🟠
const DIST_SCORE_3: f64 =  1_000.0; // 🟡
const DIST_SCORE_2: f64 =  2_000.0; // 🟢
const DIST_SCORE_1: f64 = 10_000.0; // 🔵 city-wide

fn format_distance(m: f64) -> String {
    if m < 1_000.0 { format!("~{}m", m.round() as u32) }
    else { format!("~{:.1}km", m / 1_000.0) }
}

fn distance_score(m: f64) -> i32 {
    if m < DIST_SCORE_5 { 5 }
    else if m < DIST_SCORE_4 { 4 }
    else if m < DIST_SCORE_3 { 3 }
    else if m < DIST_SCORE_2 { 2 }
    else if m < DIST_SCORE_1 { 1 }
    else { 0 }
}

fn score_color(score: i32) -> &'static str {
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
fn extract_street_candidates(text: &str) -> Vec<String> {
    const SUFFIXES: &[&str] = &[
        "straße", "strasse", "str.", "allee", "weg", "platz", "ring",
        "damm", "gasse", "chaussee", "ufer", "brücke", "brucke", "steg",
    ];
    // Words that cannot be the first word of a street name
    const SKIP_LEADING: &[&str] = &[
        "auf", "der", "die", "das", "dem", "den", "des", "ein", "eine", "einen",
        "am", "im", "zur", "zum", "vom", "von", "an", "in", "zu", "bei", "nach",
        "über", "unter", "vor", "durch", "entlang", "bis", "um", "seit", "ab",
        "außer", "gegenüber", "nahe",
        // conjunctions — prevent "und Erreichbarkeit Platz" style false positives
        "und", "oder", "sowie", "bzw",
        // indefinite articles (cases not covered above)
        "einem", "einer", "eines",
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
                if SKIP_LEADING.contains(&head) { p = &p[1..]; } else { break; }
            }
            p.to_vec()
        };

        if parts.is_empty() { continue; }

        // Skip bare suffix words with no name (e.g. just "Straße", "Platz", "Weg")
        if parts.len() == 1 {
            let bare = parts[0].to_lowercase();
            let bare = bare.trim_end_matches(',').trim_end_matches('.');
            if SUFFIXES.contains(&bare) { continue; }
        }

        // Append house number (max 4 digits to avoid postal codes like "612101")
        let mut all_parts = parts.clone();
        if i + 1 < n {
            let next = words[i + 1].trim_end_matches(',');
            if next.len() <= 4 && next.chars().next().map_or(false, |c| c.is_ascii_digit()) {
                all_parts.push(next);
            }
        }

        // Strip trailing punctuation from each part so "Luftbrücke," and
        // "Luftbrücke" produce the same candidate string and cache key.
        let candidate = all_parts.iter()
            .map(|w| w.trim_end_matches(|c: char| matches!(c, ',' | '.' | ';' | ':' | '!' | '?')))
            .collect::<Vec<_>>()
            .join(" ");
        if candidate.len() >= 5 && seen.insert(candidate.to_lowercase()) {
            out.push(candidate);
        }
    }
    out
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
async fn geocode_location(http: &reqwest::Client, query: &str, city: &str) -> Result<Option<(f64, f64)>, ()> {
    let full = if city.is_empty() { query.to_owned() } else { format!("{query}, {city}") };
    let q = url_encode(&full);
    let url = format!("https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1");
    let resp: reqwest::Response = tokio::time::timeout(
        NOMINATIM_TIMEOUT,
        http.get(&url).header("Accept-Language", "de").send(),
    )
    .await.map_err(|_| ())?.map_err(|_| ())?;
    let body = tokio::time::timeout(NOMINATIM_TIMEOUT, resp.text())
        .await.map_err(|_| ())?.map_err(|_| ())?;
    let arr: serde_json::Value = serde_json::from_str(&body).map_err(|_| ())?;
    let Some(first) = arr.get(0) else { return Ok(None) };
    let lat: f64 = first["lat"].as_str().ok_or(())?.parse().map_err(|_| ())?;
    let lon: f64 = first["lon"].as_str().ok_or(())?.parse().map_err(|_| ())?;
    Ok(Some((lat, lon)))
}

// ── Nominatim rate limiter ────────────────────────────────────────────────────

const NOMINATIM_INTERVAL: Duration = Duration::from_millis(1_100);
const NOMINATIM_TIMEOUT: Duration = Duration::from_secs(8);
const MAX_GEOCODE_CANDIDATES: usize = 60;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// One-permit semaphore: only one geocode request runs at a time.
/// The holder sleeps NOMINATIM_INTERVAL after the request before releasing,
/// so the rate never exceeds 1 req/s regardless of how many tasks are waiting.
type NominatimLimiter = Arc<Semaphore>;

/// Find the closest geocodable street mention in `text` to `(ref_lat, ref_lon)`.
/// Returns `(distance_m, street_name)` or `None`. Results are cached to avoid repeat lookups.
async fn find_nearest_distance(
    http: &reqwest::Client,
    text: &str,
    ref_lat: f64,
    ref_lon: f64,
    city: &str,
    cache: &GeocodeCache,
    limiter: &NominatimLimiter,
) -> Option<(f64, String)> {
    let candidates = extract_street_candidates(text);
    if candidates.is_empty() {
        return None;
    }
    let mut best: Option<(f64, String)> = None;

    for candidate in candidates.into_iter().take(MAX_GEOCODE_CANDIDATES) {
        // Check cache first — Some(Some(coords)) = hit, Some(None) = confirmed miss.
        let cached: Option<Option<(f64, f64)>> = cache.get(&candidate).map(|v| *v);
        let coords = match cached {
            Some(v) => v,
            None => {
                // Acquire the single geocoding permit. Tasks queue here instead of
                // pre-reserving time slots, so wait time stays bounded.
                let _permit = limiter.acquire().await.unwrap();
                // Re-check: another task may have geocoded this while we waited.
                if let Some(v) = cache.get(&candidate).map(|v| *v) {
                    v // cache hit — release permit immediately, no sleep needed
                } else {
                    let result = geocode_location(http, &candidate, city).await;
                    sleep(NOMINATIM_INTERVAL).await; // enforce rate limit before releasing permit
                    match result {
                        Ok(coords) => { cache.insert(candidate.clone(), coords); coords }
                        Err(()) => {
                            warn!("geocode transient failure for {:?} — will retry next poll", candidate);
                            None
                        }
                    }
                }
            }
        };
        if let Some((lat, lon)) = coords {
            let dist = haversine_meters(ref_lat, ref_lon, lat, lon);
            if dist < 20_000.0 && best.as_ref().map_or(true, |(d, _)| dist < *d) {
                best = Some((dist, candidate));
            }
        }
    }
    best
}

// ── Filtering ─────────────────────────────────────────────────────────────────

fn normalize(s: &str) -> String {
    s.to_lowercase()
        .replace('ß', "ss")
        // street abbreviations: "str." and "str " both normalize to "strasse"
        // so "Examplestr.", "Examplestrasse", "Examplestraße" all match
        .replace("strasse", "str")
        .replace("straße", "str")
        .replace("str.", "str")
}

/// Check keywords for blocklist/required/area matches.
/// Returns `None` if the item should be dropped.
/// Returns `Some((implied_meters, matched_terms))` where `implied_meters` is the
/// closest matching area group's distance, or `None` if no area groups are configured.
fn keyword_check(item: &FeedItem, filter: &FilterConfig, required: &[String]) -> Option<(Option<f64>, Vec<String>)> {
    let text = normalize(&format!(
        "{} {} {}",
        item.title,
        item.description.as_deref().unwrap_or(""),
        item.article_text.as_deref().unwrap_or(""),
    ));

    for b in &filter.blocklist {
        if text.contains(&normalize(b)) {
            return None;
        }
    }

    if !required.is_empty() && !required.iter().any(|r| text.contains(&normalize(r))) {
        return None;
    }

    if filter.area.is_empty() {
        return Some((None, vec![]));
    }

    // Find the group with the smallest implied_meters (= closest = highest score)
    let mut best_meters: Option<f64> = None;
    let mut matched: Vec<String> = Vec::new();

    for group in &filter.area {
        if let Some(term) = group.terms.iter().find(|t| text.contains(&normalize(t))) {
            matched.push(format!("\"{}\" ({}m)", term, group.implied_meters as u32));
            if best_meters.map_or(true, |d| group.implied_meters < d) {
                best_meters = Some(group.implied_meters);
            }
        }
    }

    // No area match is not a hard drop — caller can fall back to source.base_implied_meters.
    Some((best_meters, matched))
}


// ── Seen-items store (append-only file) ───────────────────────────────────────

async fn load_seen(path: &Path) -> HashSet<String> {
    fs::read_to_string(path)
        .await
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_owned())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Returns true if the GUID was new (not previously seen).
async fn mark_seen(seen: &Arc<Mutex<HashSet<String>>>, guid: &str, path: &Path) -> bool {
    let inserted = seen.lock().await.insert(guid.to_owned());
    if inserted {
        if let Ok(mut f) = tokio::fs::OpenOptions::new().create(true).append(true).open(path).await {
            f.write_all(format!("{guid}\n").as_bytes()).await.ok();
        }
    }
    inserted
}

// ── Message formatting ────────────────────────────────────────────────────────


fn format_digest(items: &[FeedItem], header: &str) -> (String, String) {
    let mut plain = vec![format!("📡 {header}\n")];
    let mut html  = vec![format!("📡 <strong>{}</strong><br>", html_escape(header))];

    for item in items {
        let src = &item.source_name;

        let mut info_parts: Vec<String> = Vec::new();
        if item.max_score > 0 {
            info_parts.push(format!("{}/{}", item.score, item.max_score));
        }
        if let Some(d) = item.distance_meters {
            info_parts.push(format_distance(d));
        }
        let info_plain = if info_parts.is_empty() { String::new() }
                         else { format!(" [{}]", info_parts.join(" ")) };
        let info_html  = if info_parts.is_empty() { String::new() }
                         else { format!(" <em>[{}]</em>", html_escape(&info_parts.join(" "))) };

        let link_p = item.link.as_deref().map(|l| format!(" — {l}")).unwrap_or_default();
        let link_h = item.link.as_deref()
            .map(|l| format!(" — <a href=\"{l}\">link</a>"))
            .unwrap_or_default();
        let color = score_color(item.score);
        plain.push(format!("{color} [{src}] {}{info_plain}{link_p}", item.title));
        html.push(format!(
            "{color} <em>[{}]</em> {}{info_html}{link_h}<br>",
            html_escape(src),
            html_escape(&item.title)
        ));
    }

    let html_out = html.join("");
    let html_out = html_out.trim_end_matches("<br>").to_owned();
    (plain.join("\n").trim_end().to_owned(), html_out)
}

// ── Posting ───────────────────────────────────────────────────────────────────

async fn post_to_rooms(client: &Client, plain: &str, html: &str) {
    for room in client.joined_rooms() {
        if let Err(e) = room.send(RoomMessageEventContent::text_html(plain, html)).await {
            error!("Failed to post to {}: {e}", room.room_id());
        }
    }
}

// ── Polling loop ──────────────────────────────────────────────────────────────

/// Process a single feed item: fetch article, keyword-check, geocode, score.
/// Returns `Some(item)` if it should go to the digest, `None` if filtered out.
async fn process_item(
    http: reqwest::Client,
    source: SourceConfig,
    mut item: FeedItem,
    filter: Arc<FilterConfig>,
    ref_point: Option<(f64, f64)>,
    geocode_city: Arc<str>,
    geocode_cache: GeocodeCache,
    limiter: NominatimLimiter,
) -> (String, Option<FeedItem>) {
    let source_name = source.name.clone();

    // Fetch article body (used for filtering + geocoding street extraction).
    // extract_article_body() isolates <article>/<main> to avoid navigation false-positives.
    let needs_article = source.filter || source.base_implied_meters.is_some();
    if needs_article && item.article_text.is_none() {
        if let Some(ref url) = item.link {
            tracing::debug!("fetching article [{}] {:?}", source_name, item.title);
            item.article_text = fetch_article_text(&http, url).await;
            if item.article_text.is_none() {
                tracing::debug!("article fetch failed/empty [{}] {:?}", source_name, item.title);
            }
        }
    }

    // ── 1. Keyword check → implied distance ───────────────────────────────────
    let effective_required: &[String] = source.required.as_deref().unwrap_or(&filter.required);
    let kw_implied: Option<f64> = if source.filter {
        match keyword_check(&item, &filter, effective_required) {
            None => {
                tracing::debug!("DROP [{}] {:?}", source_name, item.title);
                return (source_name, None);
            }
            Some((implied, matched)) => {
                let implied = implied.or(source.base_implied_meters);
                if !matched.is_empty() {
                    info!(
                        "PASS [{}] {:?} keyword_implied={:?}m terms=[{}]",
                        source_name, item.title,
                        implied.map(|m| m as u32),
                        matched.join(", ")
                    );
                }
                implied
            }
        }
    } else {
        source.base_implied_meters
    };

    // ── 2. Geocode → actual distance ──────────────────────────────────────────
    // Actual distance and keyword-implied are combined by taking the closer result.
    let geocoded_dist: Option<f64> = if let Some((ref_lat, ref_lon)) = ref_point {
        // If geocoding produces false positives from RSS footer boilerplate
        // (e.g. Polizei Berlin appends their HQ address to every description),
        // switch to article_text-only by uncommenting the two lines below and
        // removing the third:
        // let body = item.article_text.as_deref()
        //     .or(item.description.as_deref())
        //     .unwrap_or("");
        let all_text = format!(
            "{} {} {}",
            item.title,
            item.description.as_deref().unwrap_or(""),
            item.article_text.as_deref().unwrap_or(""),
        );
        let candidates_count = {
            let t = format!("{} {} {}", item.title, item.description.as_deref().unwrap_or(""), item.article_text.as_deref().unwrap_or(""));
            extract_street_candidates(&t).len()
        };
        info!("  geocoding [{}] {:?} ({} candidates)", source_name, item.title, candidates_count);
        match find_nearest_distance(&http, &all_text, ref_lat, ref_lon, &geocode_city, &geocode_cache, &limiter).await {
            Some((dist, ref street)) => {
                info!("  📍 [{}] {:?} → {} at {}", source_name, street, item.title, format_distance(dist));
                item.distance_meters = Some(dist);
                Some(dist)
            }
            None => {
                info!("  no street geocoded [{}] {:?}", source_name, item.title);
                None
            }
        }
    } else {
        None
    };

    // ── 3. Final score ────────────────────────────────────────────────────────
    let final_meters = match (geocoded_dist, kw_implied) {
        (Some(d), Some(k)) => Some(d.min(k)),
        (Some(d), None)    => Some(d),
        (None,    Some(k)) => Some(k),
        (None,    None)    => None,
    };
    let score = final_meters.map(distance_score).unwrap_or(0);

    if source.filter && score < filter.digest_threshold {
        tracing::debug!("DROP [{}] {:?} score={} below threshold", source_name, item.title, score);
        return (source_name, None);
    }

    info!("QUEUE [{}] {:?} score={}", source_name, item.title, score);
    item.score = score;
    item.max_score = 5;
    (source_name, Some(item))
}

async fn poll_once(
    _client: &Client,
    http: &reqwest::Client,
    sources: &[SourceConfig],
    filter: &FilterConfig,
    ref_point: Option<(f64, f64)>,
    seen: &Arc<Mutex<HashSet<String>>>,
    seen_path: &Path,
    digest_queue: &Arc<Mutex<Vec<FeedItem>>>,
    geocode_cache: &GeocodeCache,
    bluesky: Option<&sources::BlueskyContext>,
    test_mode: bool,
) {
    let arc_filter = Arc::new(filter.clone());
    let geocode_city: Arc<str> = Arc::from(filter.geocode_city.as_deref().unwrap_or(""));
    let limiter: NominatimLimiter = Arc::new(Semaphore::new(1));

    // Fetch all feeds and collect new items, then process them in parallel.
    // Seen-check is sequential to avoid races; article fetch + geocoding run concurrently.
    let mut pending: Vec<(SourceConfig, FeedItem)> = Vec::new();
    let mut total_by_source: HashMap<String, usize> = HashMap::new();

    for source in sources {
        let items = sources::build_adapter(source, bluesky).fetch_items(http).await;
        *total_by_source.entry(source.name.clone()).or_default() += items.len();

        for item in items {
            let is_new = if test_mode { true } else { mark_seen(seen, &item.guid, seen_path).await };
            if is_new { pending.push((source.clone(), item)); }
        }
    }

    // Spawn one task per item — article fetches and geocoding run in parallel.
    // Geocoding tasks share the NominatimLimiter so total Nominatim traffic stays ≤ 1 req/s.
    let mut tasks: JoinSet<(String, Option<FeedItem>)> = JoinSet::new();
    let n_pending = pending.len();
    for (source, item) in pending {
        tasks.spawn(process_item(
            http.clone(), source, item,
            arc_filter.clone(), ref_point,
            geocode_city.clone(), geocode_cache.clone(), limiter.clone(),
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
                info!("  ... {}/{} items done, {} geocode cache entries", done, n_pending, cache_n);
            }
        })
    };

    let mut passed_by_source: HashMap<String, usize> = HashMap::new();
    let mut dropped_by_source: HashMap<String, usize> = HashMap::new();

    while let Some(res) = tasks.join_next().await {
        n_done_shared.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match res {
            Ok((src, Some(item))) => {
                *passed_by_source.entry(src).or_default() += 1;
                digest_queue.lock().await.push(item);
            }
            Ok((src, None)) => { *dropped_by_source.entry(src).or_default() += 1; }
            Err(e) => warn!("Item task panicked: {e}"),
        }
    }
    ticker.abort();

    for source in sources {
        let total = total_by_source.get(&source.name).copied().unwrap_or(0);
        let passed = passed_by_source.get(&source.name).copied().unwrap_or(0);
        let dropped = dropped_by_source.get(&source.name).copied().unwrap_or(0);
        if source.filter {
            info!("Source '{}': {total} in feed, {passed} passed, {dropped} dropped", source.name);
        } else {
            info!("Source '{}': {total} in feed, {passed} new", source.name);
        }
    }
}

async fn poll_loop(
    client: Client,
    http: reqwest::Client,
    sources: Vec<SourceConfig>,
    filter: FilterConfig,
    ref_point: Option<(f64, f64)>,
    interval_mins: u64,
    seen: Arc<Mutex<HashSet<String>>>,
    seen_path: PathBuf,
    digest_queue: Arc<Mutex<Vec<FeedItem>>>,
    geocode_cache: GeocodeCache,
    bluesky: Option<sources::BlueskyContext>,
) {
    let interval = Duration::from_secs(interval_mins * 60);
    loop {
        poll_once(&client, &http, &sources, &filter, ref_point, &seen, &seen_path, &digest_queue, &geocode_cache, bluesky.as_ref(), false).await;
        info!("Next poll in {interval_mins}m");
        sleep(interval).await;
    }
}

// ── Digest chunking ───────────────────────────────────────────────────────────

const MAX_DIGEST_BYTES: usize = 8_000;
const MAX_DIGEST_ITEMS: usize = 15;

/// Sort by score descending then split into chunks under size and item-count limits.
fn chunk_digest(items: &[FeedItem]) -> Vec<Vec<FeedItem>> {
    let mut sorted = items.to_vec();
    sorted.sort_by(|a, b| b.score.cmp(&a.score));

    let mut chunks: Vec<Vec<FeedItem>> = Vec::new();
    let mut current: Vec<FeedItem> = Vec::new();
    let mut current_bytes: usize = 0;

    for item in sorted {
        let item_bytes = item.title.len() + item.source_name.len()
            + item.link.as_deref().map_or(0, |l| l.len()) + 50;
        let would_overflow = current_bytes + item_bytes > MAX_DIGEST_BYTES
            || current.len() >= MAX_DIGEST_ITEMS;
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

async fn digest_loop(
    client: Client,
    digest_times: Vec<String>,
    digest_queue: Arc<Mutex<Vec<FeedItem>>>,
) {
    let targets: Vec<NaiveTime> = digest_times.iter()
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
        let next_dt = targets.iter()
            .map(|t| if now.time() < *t { today.and_time(*t) } else { (today + chrono::Duration::days(1)).and_time(*t) })
            .min()
            .unwrap();
        let secs = (next_dt - now.naive_local()).num_seconds().max(0) as u64;
        info!("Next digest in {secs}s (at {})", next_dt.format("%H:%M"));
        sleep(Duration::from_secs(secs)).await;

        let items: Vec<FeedItem> = {
            let mut q = digest_queue.lock().await;
            std::mem::take(&mut *q)
        };

        if items.is_empty() {
            info!("Digest: no pending items — staying silent");
            continue;
        }

        let day_str = Local::now().format("%A, %d %b %Y").to_string();
        let header = format!("Digest — {day_str}");
        info!("Posting digest with {} item(s)", items.len());
        let chunks = chunk_digest(&items);
        let total = chunks.len();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let chunk_header = if total > 1 {
                format!("{header} ({}/{})", i + 1, total)
            } else {
                header.clone()
            };
            let (plain, html) = format_digest(&chunk, &chunk_header);
            post_to_rooms(&client, &plain, &html).await;
            if total > 1 {
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

// ── Verification (same pattern as all other bots) ─────────────────────────────

async fn handle_sas(sas: matrix_sdk::encryption::verification::SasVerification) {
    if let Err(e) = sas.accept().await {
        error!("Failed to accept SAS: {e}");
        return;
    }
    let mut stream = sas.changes();
    while let Some(state) = stream.next().await {
        match state {
            SasState::KeysExchanged { emojis, .. } => {
                if let Some(emojis) = emojis {
                    let names: Vec<_> = emojis.emojis.iter().map(|e| e.description).collect();
                    info!("SAS emojis: {}", names.join(" "));
                }
            }
            SasState::Done { .. } => {
                info!("SAS verification done");
                break;
            }
            SasState::Cancelled(info) => {
                warn!("SAS cancelled: {:?}", info.cancel_code());
                break;
            }
            _ => {}
        }
    }
}

fn is_join_terminal(e: &matrix_sdk::Error) -> bool {
    let s = e.to_string();
    s.contains("No known servers")
        || s.contains("M_FORBIDDEN")
        || s.contains("M_UNKNOWN_TOKEN")
        || s.contains("M_GUEST_ACCESS_FORBIDDEN")
}

async fn bootstrap_cross_signing(client: &Client, user_id: &OwnedUserId) {
    if let Some(status) = client.encryption().cross_signing_status().await {
        if status.has_master && status.has_self_signing && status.has_user_signing {
            info!("Cross-signing already complete (keys present) — skipping bootstrap");
            return;
        }
    }
    match client.encryption().bootstrap_cross_signing(None).await {
        Ok(()) => info!("Cross-signing bootstrapped for {user_id}"),
        Err(e) => warn!("Cross-signing bootstrap failed: {e}"),
    }
}

async fn handle_verification_request(
    client: Client,
    state: BotState,
    request: VerificationRequest,
) {
    let user_id = request.other_user_id();

    let already_verified = client
        .encryption()
        .get_user_devices(user_id)
        .await
        .map(|devices| devices.devices().any(|d| d.is_verified()))
        .unwrap_or(false);

    if already_verified {
        let allowed = state.reset_allowed.lock().await.remove(user_id);
        if !allowed {
            warn!("Rejecting verification from {} — already has a verified device", user_id);
            request.cancel().await.ok();
            return;
        }
        info!("Allowing re-verification for {} (trust was reset by admin)", user_id);
    }

    info!("Accepting verification from {user_id}");
    if let Err(e) = request.accept().await {
        error!("Failed to accept verification: {e}");
        return;
    }

    let mut stream = request.changes();
    while let Some(s) = stream.next().await {
        match s {
            VerificationRequestState::Transitioned { verification } => {
                if let Verification::SasV1(sas) = verification {
                    tokio::spawn(handle_sas(sas));
                    break;
                }
            }
            VerificationRequestState::Done | VerificationRequestState::Cancelled(_) => break,
            _ => {}
        }
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let config_path = args.get(1).map(|s| s.as_str()).unwrap_or("config.toml");
    let test_mode = args.iter().any(|a| a == "--test");

    let config_str = fs::read_to_string(config_path)
        .await
        .with_context(|| format!("Cannot read config: {config_path}"))?;
    let config: Config = toml::from_str(&config_str).context("TOML parse error")?;

    let store_dir = PathBuf::from("store");
    fs::create_dir_all(&store_dir).await?;
    let seen_path = store_dir.join("seen_guids.txt");

    let encryption_strategy: CollectStrategy = config.security.encryption_strategy.into();

    let client = Client::builder()
        .homeserver_url(&config.matrix.homeserver)
        .sqlite_store(store_dir.join("matrix_store"), None)
        .with_room_key_recipient_strategy(encryption_strategy)
        .build()
        .await?;

    let user_id: OwnedUserId = config.matrix.user_id.parse().context("Invalid user_id")?;
    let device_id: OwnedDeviceId = OwnedDeviceId::from(config.matrix.device_id);

    client
        .restore_session(MatrixSession {
            meta: SessionMeta { user_id: user_id.clone(), device_id },
            tokens: SessionTokens {
                access_token: config.matrix.access_token,
                refresh_token: None,
            },
        })
        .await?;
    info!("Session restored as {user_id}");

    if let Some(ref key) = config.matrix.recovery_key {
        match client.encryption().recovery().recover(key).await {
            Ok(()) => info!("Cross-signing keys recovered"),
            Err(e) => warn!("Recovery failed: {e}"),
        }
    }
    bootstrap_cross_signing(&client, &user_id).await;

    let allowed_inviters: HashSet<OwnedUserId> = config
        .security
        .allowed_inviters
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    if allowed_inviters.is_empty() {
        warn!("No allowed_inviters configured — bot accepts invites from anyone");
    } else {
        info!("Allowed inviters: {allowed_inviters:?}");
    }

    let admin_users: HashSet<OwnedUserId> = config
        .security
        .admin_users
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    if admin_users.is_empty() {
        warn!("No admin_users configured — !reset-trust command is disabled");
    } else {
        info!("Admin users: {admin_users:?}");
    }

    let bot_state = BotState {
        bot_user_id: user_id,
        allowed_inviters,
        admin_users,
        reset_allowed: Arc::new(Mutex::new(HashSet::new())),
    };

    // Invite handler
    client.add_event_handler({
        let state = bot_state.clone();
        move |ev: StrippedRoomMemberEvent, room: Room, client: Client| {
            let state = state.clone();
            async move {
                if ev.state_key != state.bot_user_id { return; }
                if !state.allowed_inviters.is_empty()
                    && !state.allowed_inviters.contains(&ev.sender)
                {
                    warn!("Rejecting invite from {} (not in allowed_inviters)", ev.sender);
                    room.leave().await.ok();
                    return;
                }
                info!("Accepted invite from {} to {}", ev.sender, room.room_id());
                let room_id = room.room_id().to_owned();
                let mut via: Vec<OwnedServerName> = vec![ev.sender.server_name().to_owned()];
                if let Some(s) = room_id.server_name() {
                    let s = s.to_owned();
                    if !via.contains(&s) {
                        via.push(s);
                    }
                }
                let room_or_alias = match RoomOrAliasId::parse(room_id.as_str()) {
                    Ok(id) => id,
                    Err(e) => {
                        error!("Invalid room ID {room_id}: {e}");
                        return;
                    }
                };
                tokio::spawn(async move {
                    let mut delay = 2u64;
                    const MAX_ATTEMPTS: u32 = 8;
                    for attempt in 1..=MAX_ATTEMPTS {
                        match client.join_room_by_id_or_alias(&room_or_alias, &via).await {
                            Ok(_) => {
                                info!("Joined {room_id}");
                                return;
                            }
                            Err(ref e) if is_join_terminal(e) => {
                                warn!("Join failed (terminal) for {room_id}: {e}");
                                return;
                            }
                            Err(e) if attempt == MAX_ATTEMPTS => {
                                warn!("Join failed after {MAX_ATTEMPTS} attempts for {room_id}: {e}");
                            }
                            Err(e) => {
                                warn!("Join attempt {attempt}/{MAX_ATTEMPTS} failed for {room_id}: {e}; retry in {delay}s");
                                sleep(Duration::from_secs(delay)).await;
                                delay = (delay * 2).min(300);
                            }
                        }
                    }
                });
            }
        }
    });

    // To-device verification
    client.add_event_handler({
        let state = bot_state.clone();
        move |ev: ToDeviceKeyVerificationRequestEvent, client: Client| {
            let state = state.clone();
            async move {
                let Some(request) = client
                    .encryption()
                    .get_verification_request(&ev.sender, &ev.content.transaction_id)
                    .await
                else {
                    warn!("Verification request object not found");
                    return;
                };
                tokio::spawn(handle_verification_request(client, state, request));
            }
        }
    });

    // In-room messages: verification + !reset-trust
    client.add_event_handler({
        let state = bot_state.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room, client: Client| {
            let state = state.clone();
            async move {
                if ev.sender == state.bot_user_id || room.state() != RoomState::Joined {
                    return;
                }
                match &ev.content.msgtype {
                    MessageType::VerificationRequest(_) => {
                        let Some(request) = client
                            .encryption()
                            .get_verification_request(&ev.sender, &ev.event_id)
                            .await
                        else { return; };
                        tokio::spawn(handle_verification_request(client, state, request));
                    }
                    MessageType::Text(text) => {
                        if let Some(target) = text.body.trim().strip_prefix("!reset-trust ") {
                            if state.admin_users.contains(&ev.sender) {
                                if let Ok(target_user) = target.trim().parse::<OwnedUserId>() {
                                    state.reset_allowed.lock().await.insert(target_user.clone());
                                    info!("Trust reset for {target_user} (by {})", ev.sender);
                                    room.send(RoomMessageEventContent::text_plain(
                                        format!("Trust reset for {target_user}. They may re-verify."),
                                    )).await.ok();
                                }
                            } else {
                                warn!("!reset-trust from non-admin {} — ignored", ev.sender);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    });

    // Initial sync
    info!("Performing initial sync...");
    {
        let filter = FilterDefinition::with_lazy_loading();
        client.sync_once(SyncSettings::default().filter(filter.into())).await?;
    }
    info!("Initial sync complete. {} source(s) configured.", config.sources.len());

    // Drain pending invites from prior sessions.
    let invited = client.invited_rooms();
    if !invited.is_empty() {
        info!("Pending invite(s) found after initial sync — joining {} room(s)", invited.len());
        for room in invited {
            let room_id = room.room_id().to_owned();
            let via: Vec<OwnedServerName> = room_id
                .server_name()
                .map(|s| vec![s.to_owned()])
                .unwrap_or_default();
            match RoomOrAliasId::parse(room_id.as_str()) {
                Ok(room_or_alias) => {
                    match client.join_room_by_id_or_alias(&room_or_alias, &via).await {
                        Ok(_) => info!("Joined pending invite room {room_id}"),
                        Err(e) => warn!("Failed to join pending invite room {room_id}: {e}"),
                    }
                }
                Err(e) => warn!("Invalid room ID in pending invite {room_id}: {e}"),
            }
        }
    }

    let seen = Arc::new(Mutex::new(load_seen(&seen_path).await));
    info!("Loaded {} seen GUIDs", seen.lock().await.len());

    let digest_queue: Arc<Mutex<Vec<FeedItem>>> = Arc::new(Mutex::new(Vec::new()));
    let geocode_cache: GeocodeCache = Arc::new(DashMap::new());
    let http = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (compatible; radar-bot/0.1)")
        .build()?;

    // Resolve reference address to coordinates once at startup.
    let ref_point: Option<(f64, f64)> = if let Some(ref addr) = config.filter.reference_address {
        info!("Geocoding reference address: {addr}");
        match geocode_location(&http, addr, "").await {
            Ok(Some(coords)) => {
                info!("Reference point: {:.5}, {:.5}", coords.0, coords.1);
                Some(coords)
            }
            Ok(None) | Err(()) => {
                warn!("Could not geocode reference address '{addr}' — distance scoring disabled");
                None
            }
        }
    } else {
        warn!("No reference_address configured — distance scoring disabled");
        None
    };

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Interrupted — exiting.");
        std::process::abort();
    });

    let bluesky_ctx: Option<sources::BlueskyContext> =
        match (config.bluesky.identifier, config.bluesky.password) {
            (Some(identifier), Some(password)) => {
                info!("Bluesky credentials configured for {identifier}");
                Some(sources::BlueskyContext {
                    identifier,
                    password,
                    session: sources::bluesky::new_shared_session(),
                })
            }
            _ => {
                let has_bluesky = config.sources.iter().any(|s| matches!(s.source_type, sources::SourceType::Bluesky));
                if has_bluesky {
                    warn!("Bluesky source(s) configured but [bluesky] identifier/password missing — searches will fail");
                }
                None
            }
        };

    if test_mode {
        // Run twice: run 1 builds the geocode cache (cold), run 2 should hit it (warm).
        // The seen-check is bypassed in test mode (is_new = true always), so both runs
        // process the same items — no need to clear any state between them.
        for run in 1..=2u32 {
            let cache_before = geocode_cache.len();
            info!("Test mode: run {run}/2 (geocode cache: {cache_before} entries before)");
            poll_once(
                &client, &http, &config.sources, &config.filter, ref_point,
                &seen, &seen_path, &digest_queue, &geocode_cache, bluesky_ctx.as_ref(), true,
            ).await;
            let cache_after = geocode_cache.len();
            info!("Run {run}/2 done — geocode cache: {cache_before} → {cache_after} entries");

            let items: Vec<FeedItem> = std::mem::take(&mut *digest_queue.lock().await);
            if !items.is_empty() {
                let chunks = chunk_digest(&items);
                let total = chunks.len();
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let header = if total > 1 {
                        format!("Test digest run {run} ({}/{})", i + 1, total)
                    } else {
                        format!("Test digest (run {run}/2)")
                    };
                    let (plain, html) = format_digest(&chunk, &header);
                    post_to_rooms(&client, &plain, &html).await;
                    if total > 1 { sleep(Duration::from_millis(500)).await; }
                }
            } else {
                info!("Run {run}/2: no items passed the filter");
            }
        }
        return Ok(());
    }

    tokio::spawn(digest_loop(
        client.clone(),
        config.schedule.digest_times.clone(),
        digest_queue.clone(),
    ));

    // Spawn poll loop
    tokio::spawn(poll_loop(
        client.clone(),
        http,
        config.sources,
        config.filter,
        ref_point,
        config.schedule.poll_interval_minutes,
        seen,
        seen_path,
        digest_queue,
        geocode_cache,
        bluesky_ctx,
    ));

    // Continuous Matrix sync
    let filter = FilterDefinition::with_lazy_loading();
    client.sync(SyncSettings::default().filter(filter.into())).await?;

    Ok(())
}
