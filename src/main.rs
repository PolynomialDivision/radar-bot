mod alerts;
mod db;
mod sources;
mod weather;

use dashmap::DashMap;
use db::Db;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

use anyhow::{Context, Result};
use chrono::{Local, NaiveTime};
use matrix_sdk::{
    config::SyncSettings,
    ruma::{
        api::client::filter::FilterDefinition,
        events::{
            key::verification::request::ToDeviceKeyVerificationRequestEvent,
            room::{
                member::StrippedRoomMemberEvent,
                message::{MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent},
            },
        },
        OwnedServerName, OwnedUserId, RoomOrAliasId,
    },
    Client, Room, RoomState,
};
use mxbot_common::config::{MatrixConfig, SecurityConfig};

#[derive(Debug, Clone)]
struct GeocodeHit {
    lat: f64,
    lon: f64,
    display_name: Option<String>,
}

type GeocodeCache = Arc<DashMap<String, Option<GeocodeHit>>>;

#[derive(Debug, Clone)]
enum DistanceLookup {
    Found(f64, String),
    NoMatch,
    TransientFailure,
}
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
    #[serde(default)]
    emergency: EmergencyConfig,
    #[serde(default)]
    weather: WeatherConfig,
}

#[derive(Deserialize, Default)]
struct BlueskyConfig {
    identifier: Option<String>,
    password: Option<String>,
}

#[derive(Deserialize)]
struct EmergencyConfig {
    #[serde(default = "default_true")]
    enabled: bool,
    /// Max distance in km for USGS earthquake alerts. None = send all globally.
    #[serde(default)]
    max_distance_km: Option<f64>,
    /// Poll interval in seconds (default 120).
    #[serde(default = "default_alert_poll_secs")]
    poll_interval_secs: u64,
}

impl Default for EmergencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_distance_km: Some(500.0),
            poll_interval_secs: default_alert_poll_secs(),
        }
    }
}

fn default_alert_poll_secs() -> u64 {
    120
}

#[derive(Deserialize)]
struct WeatherConfig {
    #[serde(default = "default_true")]
    enabled: bool,
    /// "brightsky" (DWD, default) or "openmeteo".
    #[serde(default)]
    provider: weather::WeatherProvider,
    /// Time of day to post the forecast (HH:MM, 24h). Defaults to first digest time.
    #[serde(default)]
    post_time: Option<String>,
    /// Poll DWD weather warnings and send them immediately through the alert loop.
    #[serde(default = "default_true")]
    alerts_enabled: bool,
    /// Region keywords used to select DWD weather warnings. Empty = derive from
    /// filter.geocode_city/reference_address, e.g. "Berlin".
    #[serde(default)]
    warning_region_keywords: Vec<String>,
}

impl Default for WeatherConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            provider: weather::WeatherProvider::Brightsky,
            post_time: None,
            alerts_enabled: true,
            warning_region_keywords: Vec::new(),
        }
    }
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
    // ── Search-based sources ──────────────────────────────────────────────────
    /// Search query for type = "bluesky" or type = "google_news".
    #[serde(default)]
    query: Option<String>,
    /// Max posts per poll for Bluesky (default 25, API max 100).
    #[serde(default)]
    limit: Option<usize>,
    /// Only keep feed/search items newer than this many hours where supported.
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

/// Explicit local places with coordinates. These improve relevance for articles
/// that mention stations, squares, venues, or neighbourhood nicknames instead
/// of street addresses.
#[derive(Deserialize, Clone)]
struct PlaceConfig {
    name: String,
    lat: f64,
    lon: f64,
    #[serde(default)]
    aliases: Vec<String>,
}

/// Filtering and scoring configuration.
///   - blocklist match → always drop
///   - required: at least one must match (or list is empty)
///   - area groups: find closest matching group → implied distance fallback
///   - geocoding can override with an actual distance (always takes the closer result)
///   - distance_score(final_meters) >= digest_threshold → queue for digest
///   - candidate_min_score <= distance_score < digest_threshold → keep briefly
///     as a candidate that duplicate clustering can promote later
#[derive(Deserialize, Default, Clone)]
struct FilterConfig {
    #[serde(default)]
    required: Vec<String>,
    #[serde(default)]
    blocklist: Vec<String>,
    #[serde(default = "default_digest_threshold")]
    digest_threshold: i32,
    #[serde(default = "default_candidate_min_score")]
    candidate_min_score: i32,
    #[serde(default = "default_candidate_min_sources")]
    candidate_min_sources: usize,
    #[serde(default = "default_candidate_retention_hours")]
    candidate_retention_hours: u64,
    #[serde(default)]
    area: Vec<AreaGroup>,
    #[serde(default)]
    places: Vec<PlaceConfig>,
    /// Full address used as the reference point for distance scoring.
    /// Geocoded once at startup via Nominatim.
    reference_address: Option<String>,
    /// City string appended to Nominatim queries when geocoding street mentions
    /// from articles (e.g. "Berlin, Germany"). Anchors results to the right city.
    #[serde(default)]
    geocode_city: Option<String>,
}

fn default_digest_threshold() -> i32 {
    1
}
fn default_candidate_min_score() -> i32 {
    1
}
fn default_candidate_min_sources() -> usize {
    2
}
fn default_candidate_retention_hours() -> u64 {
    24
}

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

fn default_poll_interval() -> u64 {
    30
}
fn default_digest_times() -> Vec<String> {
    vec!["08:00".to_owned()]
}
fn default_true() -> bool {
    true
}

fn derive_warning_region_keywords(filter: &FilterConfig, weather: &WeatherConfig) -> Vec<String> {
    if !weather.warning_region_keywords.is_empty() {
        return weather.warning_region_keywords.clone();
    }

    let from_geocode_city = filter
        .geocode_city
        .as_deref()
        .and_then(|city| city.split(',').next())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(city) = from_geocode_city {
        return vec![city.to_owned()];
    }

    filter
        .reference_address
        .as_deref()
        .and_then(|addr| addr.rsplit(',').next())
        .map(str::trim)
        .and_then(|part| {
            part.split_whitespace()
                .find(|token| token.chars().any(|c| c.is_alphabetic()))
        })
        .map(|city| vec![city.to_owned()])
        .unwrap_or_default()
}

// ── Bot state ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct BotState {
    bot_user_id: OwnedUserId,
    allowed_inviters: HashSet<OwnedUserId>,
    admin_users: HashSet<OwnedUserId>,
    reset_allowed: Arc<Mutex<HashSet<OwnedUserId>>>,
    db: Db,
}

// ── Feed item ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FeedItem {
    pub(crate) guid: String,
    pub(crate) title: String,
    pub(crate) link: Option<String>,
    /// Optional user-facing note about the link, e.g. rolling/live ticker pages.
    #[serde(default)]
    pub(crate) link_note: Option<String>,
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
    /// Human-readable place that produced the distance/fallback score.
    #[serde(default)]
    pub(crate) location_label: Option<String>,
    /// Unix timestamp from <pubDate> / <published> / <updated>. None if missing or unparseable.
    #[serde(default)]
    pub(crate) published_at: Option<i64>,
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
            let replaced = if let Some(hex) = entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
            {
                u32::from_str_radix(hex, 16)
                    .ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
            } else if let Some(dec) = entity.strip_prefix('#') {
                dec.parse::<u32>()
                    .ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
            } else {
                match entity {
                    "amp" => Some("&".into()),
                    "lt" => Some("<".into()),
                    "gt" => Some(">".into()),
                    "quot" => Some("\"".into()),
                    "apos" => Some("'".into()),
                    "nbsp" => Some(" ".into()),
                    "shy" => Some("".into()),
                    // dashes & ellipsis
                    "mdash" => Some("—".into()),
                    "ndash" => Some("–".into()),
                    "hellip" => Some("…".into()),
                    // typographic quotes
                    "ldquo" => Some("\u{201C}".into()),
                    "rdquo" => Some("\u{201D}".into()),
                    "lsquo" => Some("\u{2018}".into()),
                    "rsquo" => Some("\u{2019}".into()),
                    "laquo" => Some("«".into()),
                    "raquo" => Some("»".into()),
                    // German-specific
                    "auml" => Some("ä".into()),
                    "ouml" => Some("ö".into()),
                    "uuml" => Some("ü".into()),
                    "Auml" => Some("Ä".into()),
                    "Ouml" => Some("Ö".into()),
                    "Uuml" => Some("Ü".into()),
                    "szlig" => Some("ß".into()),
                    // other common
                    "eacute" => Some("é".into()),
                    "egrave" => Some("è".into()),
                    "ecirc" => Some("ê".into()),
                    "euro" => Some("€".into()),
                    "pound" => Some("£".into()),
                    "copy" => Some("©".into()),
                    "reg" => Some("®".into()),
                    "trade" => Some("™".into()),
                    "bull" => Some("•".into()),
                    "middot" => Some("·".into()),
                    _ => None,
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

// ── RSS / Atom / JSON Feed parser ─────────────────────────────────────────────

pub(crate) fn parse_feed(xml: &str, source_name: &str) -> Vec<FeedItem> {
    match feed_rs::parser::parse(xml.as_bytes()) {
        Ok(feed) => {
            let items: Vec<FeedItem> = feed
                .entries
                .into_iter()
                .filter_map(|entry| normalize_feed_entry(entry, source_name))
                .collect();
            if !items.is_empty() {
                return items;
            }
            warn!("Feed parser returned no entries for '{source_name}'");
        }
        Err(e) => {
            warn!("Feed parser failed for '{source_name}': {e}");
        }
    }

    Vec::new()
}

fn normalize_feed_entry(entry: feed_rs::model::Entry, source_name: &str) -> Option<FeedItem> {
    let title = entry
        .title
        .as_ref()
        .map(|t| text_to_plain(&t.content))
        .filter(|t| !t.is_empty())
        .or_else(|| {
            entry
                .summary
                .as_ref()
                .map(|s| {
                    text_to_plain(&s.content)
                        .chars()
                        .take(120)
                        .collect::<String>()
                })
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            entry
                .content
                .as_ref()
                .and_then(|c| c.body.as_ref())
                .map(|body| text_to_plain(body).chars().take(120).collect::<String>())
                .filter(|s| !s.is_empty())
        })?;

    let link = preferred_entry_link(&entry);
    let description = entry
        .summary
        .as_ref()
        .map(|s| text_to_plain(&s.content))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            entry
                .content
                .as_ref()
                .and_then(|c| c.body.as_ref())
                .map(|body| text_to_plain(body))
                .filter(|s| !s.is_empty())
        });

    let guid = if entry.id.trim().is_empty() {
        link.clone()
            .unwrap_or_else(|| format!("{source_name}::{title}"))
    } else {
        entry.id
    };

    let published_at = entry.published.or(entry.updated).map(|dt| dt.timestamp());

    Some(FeedItem {
        guid,
        title,
        link,
        link_note: None,
        description,
        article_text: None,
        source_name: source_name.to_owned(),
        score: 0,
        max_score: 0,
        distance_meters: None,
        location_label: None,
        published_at,
    })
}

fn preferred_entry_link(entry: &feed_rs::model::Entry) -> Option<String> {
    entry
        .links
        .iter()
        .find(|link| link.rel.as_deref().map_or(true, |rel| rel == "alternate"))
        .or_else(|| entry.links.first())
        .map(|link| link.href.trim().to_owned())
        .filter(|href| !href.is_empty())
}

fn text_to_plain(s: &str) -> String {
    strip_html(s).trim().to_owned()
}

/// Parse RFC 2822 (RSS pubDate) or RFC 3339/ISO 8601 (Atom) date strings to Unix timestamp.
/// Returns None if the string is missing, empty, or unparseable — callers treat None as "keep".
pub(crate) fn parse_feed_date(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // RFC 3339 / ISO 8601 (Atom: "2024-01-15T10:30:00Z", "2024-01-15T10:30:00+01:00")
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    // RFC 2822 (RSS 2.0: "Mon, 15 Jan 2024 10:30:00 +0000")
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(s) {
        return Some(dt.timestamp());
    }
    None
}

#[cfg(test)]
mod feed_tests {
    use super::*;

    #[test]
    fn parses_rss_with_namespaced_content() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Local</title>
    <link>https://example.test/</link>
    <description>Local feed</description>
    <item>
      <title><![CDATA[Incident &amp; update]]></title>
      <link>https://example.test/news/1</link>
      <guid isPermaLink="false">abc-1</guid>
      <pubDate>Mon, 15 Jan 2024 10:30:00 +0000</pubDate>
      <description><![CDATA[Short <b>summary</b>]]></description>
      <content:encoded><![CDATA[Full <strong>article</strong> body]]></content:encoded>
    </item>
  </channel>
</rss>"#;

        let items = parse_feed(xml, "Test RSS");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].guid, "abc-1");
        assert_eq!(items[0].title, "Incident & update");
        assert_eq!(
            items[0].link.as_deref(),
            Some("https://example.test/news/1")
        );
        assert_eq!(items[0].description.as_deref(), Some("Short summary"));
        assert_eq!(items[0].published_at, Some(1_705_314_600));
    }

    #[test]
    fn parses_atom_link_href_and_updated_date() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Feed</title>
  <id>feed-id</id>
  <updated>2024-01-15T10:00:00Z</updated>
  <entry>
    <title>Atom entry</title>
    <id>tag:example.test,2024:entry-1</id>
    <updated>2024-01-15T11:30:00+01:00</updated>
    <link rel="alternate" href="https://example.test/atom/1" />
    <summary type="html">Atom &lt;b&gt;summary&lt;/b&gt;</summary>
  </entry>
</feed>"#;

        let items = parse_feed(xml, "Test Atom");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].guid, "tag:example.test,2024:entry-1");
        assert_eq!(items[0].title, "Atom entry");
        assert_eq!(
            items[0].link.as_deref(),
            Some("https://example.test/atom/1")
        );
        assert_eq!(items[0].description.as_deref(), Some("Atom summary"));
        assert_eq!(items[0].published_at, Some(1_705_314_600));
    }

    #[test]
    fn digest_includes_location_with_distance() {
        let item = DigestItem {
            guids: vec!["1".to_owned()],
            source_name: "Local".to_owned(),
            source_count: 1,
            title: "Street closed".to_owned(),
            link: None,
            link_note: None,
            score: 4,
            max_score: 5,
            distance_meters: Some(350.0),
            location_label: Some("Boxhagener Platz".to_owned()),
        };

        let (plain, html) = format_digest(&[item], "Test");

        assert!(plain.contains("~350m · Boxhagener Platz"));
        assert!(html.contains("~350m · Boxhagener Platz"));
    }

    #[test]
    fn clustering_collapses_multi_source_same_incident_and_boosts_score() {
        let a = db::DbItem {
            guid: "a".to_owned(),
            source_name: "Source A".to_owned(),
            title: "Brand in der Rigaer Straße".to_owned(),
            link: Some("https://a.test/1".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(700.0),
            location_label: Some("Rigaer Straße".to_owned()),
        };
        let b = db::DbItem {
            guid: "b".to_owned(),
            source_name: "Source B".to_owned(),
            title: "Feuerwehreinsatz Rigaer Straße nach Brand".to_owned(),
            link: Some("https://b.test/2".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(730.0),
            location_label: Some("Rigaer Straße".to_owned()),
        };

        let clustered = cluster_digest_items(vec![a, b]);

        assert_eq!(clustered.len(), 1);
        assert_eq!(clustered[0].guids, vec!["a", "b"]);
        assert_eq!(clustered[0].source_count, 2);
        assert_eq!(clustered[0].score, 4);
        assert!(clustered[0].source_name.contains("Source A"));
        assert!(clustered[0].source_name.contains("Source B"));
    }

    #[tokio::test]
    async fn candidate_clustering_promotes_multi_source_weak_items() {
        let path = std::env::temp_dir().join(format!(
            "radar-bot-candidate-test-{}-{}.db",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let db = Db::open(&path).expect("open temp db");

        let a = FeedItem {
            guid: "candidate-a".to_owned(),
            title: "Brand in der Rigaer Straße".to_owned(),
            link: Some("https://a.test/1".to_owned()),
            link_note: None,
            description: None,
            article_text: None,
            source_name: "Source A".to_owned(),
            score: 2,
            max_score: 5,
            distance_meters: Some(1500.0),
            location_label: Some("Rigaer Straße".to_owned()),
            published_at: None,
        };
        let mut b = a.clone();
        b.guid = "candidate-b".to_owned();
        b.source_name = "Source B".to_owned();
        b.title = "Feuerwehreinsatz Rigaer Straße nach Brand".to_owned();
        b.link = Some("https://b.test/2".to_owned());

        db.insert_candidate(&a, "score 2 below digest threshold 3")
            .await
            .expect("insert candidate a");
        db.insert_candidate(&b, "score 2 below digest threshold 3")
            .await
            .expect("insert candidate b");

        let promoted = promote_candidate_clusters(&db, 3, 2, 24)
            .await
            .expect("promote candidates");
        let items = db.take_for_digest(3).await.expect("take digest");

        assert_eq!(promoted, 2);
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|item| item.score >= 3));

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rolling_link_gets_note_when_title_no_longer_matches() {
        let html = r#"
            <html><head><link rel="canonical" href="https://example.test/live/ticker" /></head>
            <body><main>Aktuelle Meldungen zur Verkehrslage ohne den alten Eintrag.</main></body></html>
        "#;
        let (link, note) = resolve_article_link(
            "https://example.test/liveticker",
            html,
            "Aktuelle Meldungen zur Verkehrslage ohne den alten Eintrag.",
            "Brand in der Rigaer Straße",
        );

        assert_eq!(link.as_deref(), Some("https://example.test/live/ticker"));
        assert_eq!(
            note.as_deref(),
            Some("rolling source link may no longer show this item")
        );
    }

    #[test]
    fn google_news_link_resolves_to_external_publisher_when_available() {
        let html = r#"
            <html><head><link rel="canonical" href="https://news.google.com/rss/articles/abc" /></head>
            <body><a href="https://www.google.com/url?url=https%3A%2F%2Fpublisher.test%2Farticle%2F1">Publisher</a></body></html>
        "#;
        let (link, note) = resolve_article_link(
            "https://news.google.com/rss/articles/abc",
            html,
            "Brand in der Rigaer Straße mit Feuerwehreinsatz",
            "Brand in der Rigaer Straße",
        );

        assert_eq!(link.as_deref(), Some("https://publisher.test/article/1"));
        assert_eq!(note, None);
    }

    #[test]
    fn article_extraction_prefers_article_like_content_over_navigation() {
        let html = r#"
            <html><body>
                <div class="nav">Start Politik Sport Wetter Navigation Werbung</div>
                <div class="article-body">
                    <p>Brand in der Rigaer Straße. Die Feuerwehr war am Abend im Einsatz.</p>
                    <p>Mehrere Menschen bemerkten Rauch in einem Wohnhaus nahe der Kreuzung.</p>
                    <p>Die Polizei sperrte den Bereich zeitweise ab und leitete den Verkehr um.</p>
                    <p>Weitere Details sollen im Laufe des Tages bekannt gegeben werden.</p>
                </div>
                <footer>Impressum Datenschutz Kontakt Newsletter</footer>
            </body></html>
        "#;

        let body = extract_article_body(html);

        assert!(body.contains("Brand in der Rigaer Straße"));
        assert!(!body.contains("Impressum Datenschutz"));
    }

    #[test]
    fn configured_place_distance_matches_alias() {
        let filter = FilterConfig {
            places: vec![PlaceConfig {
                name: "RAW-Gelände".to_owned(),
                lat: 52.5070,
                lon: 13.4540,
                aliases: vec!["RAW Gelände".to_owned(), "RAW".to_owned()],
            }],
            ..Default::default()
        };
        let item = FeedItem {
            guid: "place".to_owned(),
            title: "Feuerwehreinsatz am RAW".to_owned(),
            link: None,
            link_note: None,
            description: None,
            article_text: None,
            source_name: "Test".to_owned(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            location_label: None,
            published_at: None,
        };

        let hit = find_configured_place_distance(&item, &filter, Some((52.5070, 13.4540)));

        assert_eq!(hit.map(|(_, label)| label), Some("RAW-Gelände".to_owned()));
    }

    #[test]
    fn street_evidence_ignores_boilerplate_after_impressum() {
        let evidence = extract_street_evidence(
            "Brand in der Rigaer Straße",
            None,
            Some("Weitere Details folgen. Impressum Kontakt Alexanderplatz 1 Datenschutz."),
        );
        let candidates: Vec<String> = evidence.into_iter().map(|e| e.candidate).collect();

        assert!(candidates.iter().any(|c| c.contains("Rigaer Straße")));
        assert!(!candidates.iter().any(|c| c.contains("Alexanderplatz")));
    }
}

// ── Article fetcher ───────────────────────────────────────────────────────────

fn tag_blocks<'a>(html: &'a str, tag_name: &str) -> Vec<(&'a str, &'a str)> {
    let lower = html.to_lowercase();
    let open_str = format!("<{tag_name}");
    let close_str = format!("</{tag_name}>");
    let mut out = Vec::new();
    let mut offset = 0;

    while let Some(pos) = lower[offset..].find(&open_str) {
        let start = offset + pos;
        let Some(gt_rel) = html[start..].find('>') else {
            break;
        };
        let content_start = start + gt_rel + 1;
        let Some(close_rel) = lower[content_start..].find(&close_str) else {
            break;
        };
        let content_end = content_start + close_rel;
        let tag = &html[start..content_start];
        let inner = &html[content_start..content_end];
        out.push((tag, inner));
        offset = content_end + close_str.len();
    }

    out
}

fn article_container_score(tag: &str, text: &str) -> i32 {
    let tag = tag.to_lowercase();
    let mut score = 0;
    for needle in [
        "article",
        "articlebody",
        "story",
        "content",
        "main",
        "post",
        "entry",
        "body",
        "meldung",
        "text",
    ] {
        if tag.contains(needle) {
            score += 4;
        }
    }
    for needle in [
        "nav", "menu", "footer", "header", "aside", "comment", "related", "teaser", "ad", "advert",
        "social",
    ] {
        if tag.contains(needle) {
            score -= 5;
        }
    }
    let words = text.split_whitespace().count() as i32;
    score + (words / 80).min(8)
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
                    lower = format!("{}{}", &lower[..s], &lower[end..]);
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
    let cleaned = remove_blocks(
        html,
        &[
            "script", "style", "nav", "header", "footer", "aside", "noscript",
        ],
    );

    let mut best: Option<(i32, String)> = None;
    for tag_name in &["article", "main", "section", "div"] {
        for (tag, inner) in tag_blocks(&cleaned, tag_name) {
            let text = strip_html(inner);
            let text = text.trim();
            if text.len() < 200 {
                continue;
            }
            let score = article_container_score(tag, text);
            if score > 0
                && best.as_ref().map_or(true, |(best_score, best_text)| {
                    score > *best_score || (score == *best_score && text.len() > best_text.len())
                })
            {
                best = Some((score, text.to_owned()));
            }
        }
    }

    best.map(|(_, text)| text)
        .unwrap_or_else(|| strip_html(&cleaned))
}

#[derive(Debug, Clone)]
struct ArticleFetch {
    text: String,
    resolved_link: Option<String>,
    link_note: Option<String>,
}

fn attr_value(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_lowercase();
    let needle = format!("{attr}=");
    let pos = lower.find(&needle)?;
    let rest = &tag[pos + needle.len()..];
    let mut chars = rest.chars();
    let quote = chars.next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value: String = chars.take_while(|c| *c != quote).collect();
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn link_rel_value(html: &str, rel_name: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find("<link") {
        let start = offset + pos;
        let Some(end_rel) = lower[start..].find('>') else {
            break;
        };
        let end = start + end_rel + 1;
        let tag = &html[start..end];
        let rel_matches = attr_value(tag, "rel")
            .map(|rel| {
                rel.split_whitespace()
                    .any(|part| part.eq_ignore_ascii_case(rel_name))
            })
            .unwrap_or(false);
        if rel_matches {
            if let Some(href) = attr_value(tag, "href") {
                return Some(href);
            }
        }
        offset = end;
    }
    None
}

fn meta_property_value(html: &str, prop_name: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find("<meta") {
        let start = offset + pos;
        let Some(end_rel) = lower[start..].find('>') else {
            break;
        };
        let end = start + end_rel + 1;
        let tag = &html[start..end];
        let prop_matches = attr_value(tag, "property")
            .or_else(|| attr_value(tag, "name"))
            .map(|prop| prop.eq_ignore_ascii_case(prop_name))
            .unwrap_or(false);
        if prop_matches {
            if let Some(content) = attr_value(tag, "content") {
                return Some(content);
            }
        }
        offset = end;
    }
    None
}

fn looks_like_google_url(url: &str) -> bool {
    let u = url.to_lowercase();
    u.contains("://news.google.") || u.contains("://www.google.") || u.contains("://google.")
}

fn decode_url_parameter(url: &str, keys: &[&str]) -> Option<String> {
    let query_start = url.find('?')?;
    for part in url[query_start + 1..].split('&') {
        let (key, value) = part.split_once('=')?;
        if keys.iter().any(|wanted| key.eq_ignore_ascii_case(wanted)) {
            let decoded = percent_decode(value.replace('+', " ").as_str());
            if decoded.starts_with("http://") || decoded.starts_with("https://") {
                return Some(decoded);
            }
        }
    }
    None
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn external_href_from_google_html(html: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find("<a") {
        let start = offset + pos;
        let Some(end_rel) = lower[start..].find('>') else {
            break;
        };
        let end = start + end_rel + 1;
        let tag = &html[start..end];
        if let Some(href) = attr_value(tag, "href") {
            let href = decode_entities(&href);
            let candidate = decode_url_parameter(&href, &["url", "q"]).unwrap_or(href);
            if (candidate.starts_with("http://") || candidate.starts_with("https://"))
                && !looks_like_google_url(&candidate)
            {
                if !looks_like_google_url(&candidate) {
                    return Some(candidate);
                }
            }
        }
        offset = end;
    }
    None
}

fn looks_like_rolling_url(url: &str) -> bool {
    let u = url.to_lowercase();
    [
        "ticker",
        "liveticker",
        "liveblog",
        "newsblog",
        "live-blog",
        "live-ticker",
    ]
    .iter()
    .any(|needle| u.contains(needle))
}

fn significant_words(s: &str) -> Vec<String> {
    normalize(s)
        .split_whitespace()
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()).to_owned())
        .filter(|w| w.chars().count() >= 5)
        .filter(|w| {
            !matches!(
                w.as_str(),
                "berlin" | "polizei" | "meldung" | "update" | "heute"
            )
        })
        .take(8)
        .collect()
}

fn text_matches_title(text: &str, title: &str) -> bool {
    let words = significant_words(title);
    if words.len() < 2 {
        return true;
    }
    let haystack = normalize(text);
    let hits = words
        .iter()
        .filter(|w| haystack.contains(w.as_str()))
        .count();
    hits >= 2 || hits * 2 >= words.len()
}

fn resolve_article_link(
    feed_url: &str,
    html: &str,
    article_text: &str,
    title: &str,
) -> (Option<String>, Option<String>) {
    let canonical = link_rel_value(html, "canonical")
        .or_else(|| meta_property_value(html, "og:url"))
        .filter(|url| url.starts_with("http://") || url.starts_with("https://"));

    let mut link = canonical.unwrap_or_else(|| feed_url.to_owned());
    if looks_like_google_url(&link) || looks_like_google_url(feed_url) {
        if let Some(external) = external_href_from_google_html(html) {
            link = external;
        }
    }
    let rolling = looks_like_rolling_url(feed_url) || looks_like_rolling_url(&link);
    let same_item = text_matches_title(article_text, title);
    let mut note = None;

    if rolling && !same_item {
        note = Some("rolling source link may no longer show this item".to_owned());
    } else if rolling {
        note = Some("rolling source link".to_owned());
    }

    if link.trim().is_empty() {
        link = feed_url.to_owned();
    }
    (Some(link), note)
}

/// Fetch a URL and return the article body plus resolved link metadata.
async fn fetch_article(http: &reqwest::Client, url: &str, title: &str) -> Option<ArticleFetch> {
    let resp = tokio::time::timeout(Duration::from_secs(10), http.get(url).send())
        .await
        .ok()?
        .ok()?;

    let html = tokio::time::timeout(Duration::from_secs(10), resp.text())
        .await
        .ok()?
        .ok()?;

    // extract_article_body calls remove_blocks which is CPU-intensive on large HTML pages.
    // Run it on the blocking thread pool so it doesn't stall the async runtime.
    let title = title.to_owned();
    let feed_url = url.to_owned();
    let text = tokio::task::spawn_blocking(move || {
        let t = extract_article_body(&html);
        let t = t.trim().to_owned();
        if t.is_empty() {
            None
        } else {
            let (resolved_link, link_note) = resolve_article_link(&feed_url, &html, &t, &title);
            Some(ArticleFetch {
                text: t,
                resolved_link,
                link_note,
            })
        }
    })
    .await
    .ok()??;

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
const DIST_SCORE_5: f64 = 200.0; // 🔴 very close
const DIST_SCORE_4: f64 = 500.0; // 🟠
const DIST_SCORE_3: f64 = 1_000.0; // 🟡
const DIST_SCORE_2: f64 = 2_000.0; // 🟢
const DIST_SCORE_1: f64 = 10_000.0; // 🔵 city-wide

fn format_distance(m: f64) -> String {
    if m < 1_000.0 {
        format!("~{}m", m.round() as u32)
    } else {
        format!("~{:.1}km", m / 1_000.0)
    }
}

fn distance_score(m: f64) -> i32 {
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
            if next.len() <= 4 && next.chars().next().map_or(false, |c| c.is_ascii_digit()) {
                all_parts.push(next);
            }
        }

        // Strip trailing punctuation from each part so "Luftbrücke," and
        // "Luftbrücke" produce the same candidate string and cache key.
        let candidate = all_parts
            .iter()
            .map(|w| w.trim_end_matches(|c: char| matches!(c, ',' | '.' | ';' | ':' | '!' | '?')))
            .collect::<Vec<_>>()
            .join(" ");
        if candidate.len() >= 5 && seen.insert(candidate.to_lowercase()) {
            out.push(candidate);
        }
    }
    out
}

#[derive(Debug, Clone)]
struct StreetEvidence {
    candidate: String,
    confidence: i32,
}

fn contains_any_normalized(text: &str, terms: &[&str]) -> bool {
    let n = normalize(text);
    terms.iter().any(|term| n.contains(&normalize(term)))
}

fn first_relevant_article_text(article: Option<&str>) -> &str {
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

fn street_confidence(section: &str, text: &str) -> i32 {
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

fn extract_street_evidence(
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

fn short_location_label(display_name: &str) -> Option<String> {
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
async fn geocode_location(
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

const NOMINATIM_INTERVAL: Duration = Duration::from_millis(1_100);
const NOMINATIM_TIMEOUT: Duration = Duration::from_secs(8);
const MAX_GEOCODE_CANDIDATES: usize = 60;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// One-permit semaphore: only one geocode request runs at a time.
/// The holder sleeps NOMINATIM_INTERVAL after the request before releasing,
/// so the rate never exceeds 1 req/s regardless of how many tasks are waiting.
type NominatimLimiter = Arc<Semaphore>;

/// Find the closest geocodable street mention in `text` to `(ref_lat, ref_lon)`.
/// Transient lookup failures stay separate from "no geocodable street" so the
/// caller can retry instead of permanently dropping a potentially relevant item.
async fn find_nearest_distance(
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
                let replace = best.as_ref().map_or(true, |(best_conf, best_dist, _)| {
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

fn find_configured_place_distance(
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

        if terms.iter().any(|term| text.contains(&normalize(term))) {
            let dist = haversine_meters(ref_lat, ref_lon, place.lat, place.lon);
            if best
                .as_ref()
                .map_or(true, |(best_dist, _)| dist < *best_dist)
            {
                best = Some((dist, place.name.clone()));
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
fn keyword_check(
    item: &FeedItem,
    filter: &FilterConfig,
    required: &[String],
) -> Option<(Option<(f64, String)>, Vec<String>)> {
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
    let mut best: Option<(f64, String)> = None;
    let mut matched: Vec<String> = Vec::new();

    for group in &filter.area {
        if let Some(term) = group.terms.iter().find(|t| text.contains(&normalize(t))) {
            matched.push(format!("\"{}\" ({}m)", term, group.implied_meters as u32));
            if best
                .as_ref()
                .map_or(true, |(d, _)| group.implied_meters < *d)
            {
                best = Some((group.implied_meters, term.clone()));
            }
        }
    }

    // No area match is not a hard drop — caller can fall back to source.base_implied_meters.
    Some((best, matched))
}

// ── Seen-items store (append-only file) ───────────────────────────────────────

// ── Message formatting ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct DigestItem {
    guids: Vec<String>,
    source_name: String,
    source_count: usize,
    title: String,
    link: Option<String>,
    link_note: Option<String>,
    score: i32,
    max_score: i32,
    distance_meters: Option<f64>,
    location_label: Option<String>,
}

impl DigestItem {
    fn from_db(item: db::DbItem) -> Self {
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

fn format_digest(items: &[DigestItem], header: &str) -> (String, String) {
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
        if let Some(d) = item.distance_meters {
            match item.location_label.as_deref() {
                Some(label) if !label.trim().is_empty() => {
                    info_parts.push(format!("{} · {}", format_distance(d), label.trim()));
                }
                _ => info_parts.push(format_distance(d)),
            }
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

fn unique_source_names(names: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for name in names {
        if seen.insert(name.to_lowercase()) {
            out.push(name.clone());
        }
    }
    out
}

fn normalized_location_label(item: &db::DbItem) -> Option<String> {
    item.location_label
        .as_deref()
        .map(normalize)
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn locations_compatible(a: &db::DbItem, b: &db::DbItem) -> bool {
    match (normalized_location_label(a), normalized_location_label(b)) {
        (Some(la), Some(lb)) if la == lb => return true,
        _ => {}
    }

    match (a.distance_meters, b.distance_meters) {
        (Some(da), Some(db)) => (da - db).abs() <= 300.0,
        _ => false,
    }
}

fn title_overlap(a: &str, b: &str) -> usize {
    let a_words: HashSet<String> = significant_words(a).into_iter().collect();
    let b_words: HashSet<String> = significant_words(b).into_iter().collect();
    a_words.intersection(&b_words).count()
}

fn likely_same_incident(item: &db::DbItem, cluster: &[db::DbItem]) -> bool {
    cluster.iter().any(|other| {
        let same_link = item.link.is_some() && item.link == other.link;
        if same_link {
            return true;
        }
        locations_compatible(item, other) && title_overlap(&item.title, &other.title) >= 2
    })
}

fn cluster_digest_items(items: Vec<db::DbItem>) -> Vec<DigestItem> {
    let mut sorted = items;
    sorted.sort_by(|a, b| b.score.cmp(&a.score));

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

fn cluster_db_items(items: Vec<db::DbItem>) -> Vec<Vec<db::DbItem>> {
    let mut sorted = items;
    sorted.sort_by(|a, b| b.score.cmp(&a.score));

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

async fn promote_candidate_clusters(
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

// ── Posting ───────────────────────────────────────────────────────────────────

/// Returns true if all rooms received the message. On false the caller should
/// requeue the items so they appear in the next digest.
pub(crate) async fn post_to_rooms(client: &Client, plain: &str, html: &str) -> bool {
    let mut all_ok = true;
    for room in client.joined_rooms() {
        if let Err(e) = room
            .send(RoomMessageEventContent::text_html(plain, html))
            .await
        {
            error!("Failed to post to {}: {e}", room.room_id());
            all_ok = false;
        }
    }
    all_ok
}

// ── Polling loop ──────────────────────────────────────────────────────────────

enum ProcessOutcome {
    Queued(FeedItem),
    Candidate { item: FeedItem, reason: String },
    Dropped { item: FeedItem, reason: String },
    Retry { item: FeedItem, reason: String },
}

/// Process a single feed item: fetch article, keyword-check, geocode, score.
async fn process_item(
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

async fn poll_once(
    _client: &Client,
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

async fn poll_loop(
    client: Client,
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

// ── Digest chunking ───────────────────────────────────────────────────────────

const MAX_DIGEST_BYTES: usize = 8_000;
const MAX_DIGEST_ITEMS: usize = 15;

/// Sort by score descending then split into chunks under size and item-count limits.
fn chunk_digest(items: &[DigestItem]) -> Vec<Vec<DigestItem>> {
    let mut sorted = items.to_vec();
    sorted.sort_by(|a, b| b.score.cmp(&a.score));

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

async fn digest_loop(
    client: Client,
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

// ── Verification (same pattern as all other bots) ─────────────────────────────

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
    // radar-bot keeps its sqlite matrix store at store/matrix_store/ to leave
    // room alongside store/items.db. Pass the full path to build_and_restore.
    let (client, user_id) = mxbot_common::session::build_and_restore(
        &config.matrix,
        &store_dir.join("matrix_store"),
        config.security.encryption_strategy.into(),
    )
    .await?;

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

    let db = Db::open(&store_dir.join("items.db"))?;

    let bot_state = BotState {
        bot_user_id: user_id,
        allowed_inviters,
        admin_users,
        reset_allowed: Arc::new(Mutex::new(HashSet::new())),
        db: db.clone(),
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
                            Err(ref e) if mxbot_common::verify::is_join_terminal(e) => {
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
                tokio::spawn(mxbot_common::verify::handle_verification_request(
                    client,
                    Arc::clone(&state.reset_allowed),
                    request,
                ));
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
                        else {
                            return;
                        };
                        tokio::spawn(mxbot_common::verify::handle_verification_request(
                            client,
                            Arc::clone(&state.reset_allowed),
                            request,
                        ));
                    }
                    MessageType::Text(text) => {
                        let body = text.body.trim();
                        if let Some(target) = body.strip_prefix("!reset-trust ") {
                            if state.admin_users.contains(&ev.sender) {
                                if let Ok(target_user) = target.trim().parse::<OwnedUserId>() {
                                    state.reset_allowed.lock().await.insert(target_user.clone());
                                    info!("Trust reset for {target_user} (by {})", ev.sender);
                                    room.send(RoomMessageEventContent::text_plain(format!(
                                        "Trust reset for {target_user}. They may re-verify."
                                    )))
                                    .await
                                    .ok();
                                }
                            } else {
                                warn!("!reset-trust from non-admin {} — ignored", ev.sender);
                            }
                        } else if body.starts_with("!source-stats") {
                            if !state.admin_users.contains(&ev.sender) {
                                warn!("!source-stats from non-admin {} — ignored", ev.sender);
                                return;
                            }
                            let hours = body
                                .split_whitespace()
                                .nth(1)
                                .and_then(|s| s.parse::<u32>().ok())
                                .unwrap_or(24)
                                .clamp(1, 24 * 30);
                            match state.db.source_stats_summary(hours).await {
                                Ok(stats) if stats.is_empty() => {
                                    room.send(RoomMessageEventContent::text_plain(format!(
                                        "No source stats recorded in the last {hours}h."
                                    )))
                                    .await
                                    .ok();
                                }
                                Ok(stats) => {
                                    let mut lines =
                                        vec![format!("Source stats, last {hours}h:")];
                                    for stat in stats {
                                        lines.push(format!(
                                            "{}: {} total, {} queued, {} candidate, {} dropped, {} retry",
                                            stat.source_name,
                                            stat.total,
                                            stat.queued,
                                            stat.candidate,
                                            stat.dropped,
                                            stat.retry_later
                                        ));
                                    }
                                    room.send(RoomMessageEventContent::text_plain(
                                        lines.join("\n"),
                                    ))
                                    .await
                                    .ok();
                                }
                                Err(e) => {
                                    warn!("!source-stats failed: {e}");
                                    room.send(RoomMessageEventContent::text_plain(
                                        "Source stats query failed.",
                                    ))
                                    .await
                                    .ok();
                                }
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
        client
            .sync_once(SyncSettings::default().filter(filter.into()))
            .await?;
    }
    info!(
        "Initial sync complete. {} source(s) configured.",
        config.sources.len()
    );

    // Drain pending invites from prior sessions.
    let invited = client.invited_rooms();
    if !invited.is_empty() {
        info!(
            "Pending invite(s) found after initial sync — joining {} room(s)",
            invited.len()
        );
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

    // Crash recovery: anything left in 'processing' from last run goes back to 'queued'.
    let recovered = db.recover_processing().await?;
    if recovered > 0 {
        warn!("{recovered} item(s) recovered from interrupted digest — will retry at next digest time");
    }

    let geocode_cache: GeocodeCache = Arc::new(DashMap::new());
    let http = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (compatible; radar-bot/0.1)")
        .build()?;

    // Resolve reference address to coordinates once at startup.
    let ref_point: Option<(f64, f64)> = if let Some(ref addr) = config.filter.reference_address {
        info!("Geocoding reference address: {addr}");
        match geocode_location(&http, addr, "").await {
            Ok(Some(hit)) => {
                info!("Reference point: {:.5}, {:.5}", hit.lat, hit.lon);
                Some((hit.lat, hit.lon))
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

    let bluesky_ctx: Option<sources::BlueskyContext> = match (
        config.bluesky.identifier,
        config.bluesky.password,
    ) {
        (Some(identifier), Some(password)) => {
            info!("Bluesky credentials configured for {identifier}");
            Some(sources::BlueskyContext {
                identifier,
                password,
                session: sources::bluesky::new_shared_session(),
            })
        }
        _ => {
            let has_bluesky = config
                .sources
                .iter()
                .any(|s| matches!(s.source_type, sources::SourceType::Bluesky));
            if has_bluesky {
                warn!("Bluesky source(s) configured but [bluesky] identifier/password missing — searches will fail");
            }
            None
        }
    };
    let dwd_region_keywords = if config.weather.alerts_enabled {
        derive_warning_region_keywords(&config.filter, &config.weather)
    } else {
        Vec::new()
    };
    if config.weather.alerts_enabled && dwd_region_keywords.is_empty() {
        warn!("DWD weather warnings disabled — no weather.warning_region_keywords, geocode_city, or reference_address city found");
    } else if !dwd_region_keywords.is_empty() {
        info!(
            "DWD weather warning region keyword(s): {:?}",
            dwd_region_keywords
        );
    }

    if test_mode {
        // Run twice: run 1 builds the geocode cache (cold), run 2 should hit it (warm).
        // test_mode bypasses the DB seen-check (is_new = true always) and does not write
        // to the DB, so both runs process the same items without affecting state.
        for run in 1..=2u32 {
            let cache_before = geocode_cache.len();
            info!("Test mode: run {run}/2 (geocode cache: {cache_before} entries before)");
            poll_once(
                &client,
                &http,
                &config.sources,
                &config.filter,
                ref_point,
                &db,
                &geocode_cache,
                bluesky_ctx.as_ref(),
                true,
            )
            .await;
            let cache_after = geocode_cache.len();
            info!("Run {run}/2 done — geocode cache: {cache_before} → {cache_after} entries");

            // In test mode poll_once doesn't write to DB; query what would be queued by
            // re-running take_for_digest on an empty DB (no-op) — instead just note it.
            let items = db
                .take_for_digest(config.filter.digest_threshold)
                .await
                .unwrap_or_default();
            if !items.is_empty() {
                let clustered_items = cluster_digest_items(items);
                let chunks = chunk_digest(&clustered_items);
                let total = chunks.len();
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let header = if total > 1 {
                        format!("Test digest run {run} ({}/{})", i + 1, total)
                    } else {
                        format!("Test digest (run {run}/2)")
                    };
                    let (plain, html) = format_digest(&chunk, &header);
                    post_to_rooms(&client, &plain, &html).await;
                    if total > 1 {
                        sleep(Duration::from_millis(500)).await;
                    }
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
        db.clone(),
        config.filter.digest_threshold,
        config.filter.candidate_min_sources,
        config.filter.candidate_retention_hours,
    ));

    if config.emergency.enabled {
        let nina_ags = match ref_point {
            Some(pt) => alerts::lookup_nina_ags(&http, pt).await,
            None => {
                warn!("No reference_address — NINA alerts disabled");
                None
            }
        };
        tokio::spawn(alerts::alert_loop(
            client.clone(),
            http.clone(),
            db.clone(),
            alerts::AlertConfig {
                nina_ags,
                ref_point,
                dwd_region_keywords,
                max_distance_km: config.emergency.max_distance_km,
                poll_interval_secs: config.emergency.poll_interval_secs,
            },
        ));
    }

    if config.weather.enabled {
        if let Some(pt) = ref_point {
            let post_time_str = config
                .weather
                .post_time
                .or_else(|| config.schedule.digest_times.first().cloned())
                .unwrap_or_else(|| "08:00".to_owned());
            let post_time = {
                let mut p = post_time_str.splitn(2, ':');
                let h: u32 = p.next().and_then(|s| s.parse().ok()).unwrap_or(8);
                let m: u32 = p.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                NaiveTime::from_hms_opt(h, m, 0)
                    .unwrap_or_else(|| NaiveTime::from_hms_opt(8, 0, 0).unwrap())
            };
            tokio::spawn(weather::weather_loop(
                client.clone(),
                http.clone(),
                db.clone(),
                pt,
                post_time,
                config.weather.provider,
            ));
        } else {
            warn!("Weather forecast disabled — no reference_address configured");
        }
    }

    // Spawn poll loop
    tokio::spawn(poll_loop(
        client.clone(),
        http,
        config.sources,
        config.filter,
        ref_point,
        config.schedule.poll_interval_minutes,
        db,
        geocode_cache,
        bluesky_ctx,
    ));

    // Continuous Matrix sync
    let filter = FilterDefinition::with_lazy_loading();
    loop {
        match client
            .sync(SyncSettings::default().filter(filter.clone().into()))
            .await
        {
            Ok(()) => warn!("Sync loop exited cleanly — reconnecting"),
            Err(e) => warn!("Sync loop error: {e} — reconnecting in 5s"),
        }
        sleep(Duration::from_secs(5)).await;
    }
}
