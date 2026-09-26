//! Configuration: `[filter]`, `[schedule]`, `[weather]`, `[emergency]`, sources.

use super::*;

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(crate) struct Config {
    pub(crate) matrix: MatrixConfig,
    #[serde(default)]
    pub(crate) security: SecurityConfig,
    #[serde(default)]
    pub(crate) schedule: ScheduleConfig,
    #[serde(default)]
    pub(crate) filter: FilterConfig,
    #[serde(default)]
    pub(crate) sources: Vec<SourceConfig>,
    #[serde(default)]
    pub(crate) bluesky: BlueskyConfig,
    #[serde(default)]
    pub(crate) emergency: EmergencyConfig,
    #[serde(default)]
    pub(crate) weather: WeatherConfig,
}

#[derive(Deserialize, Default)]
pub(crate) struct BlueskyConfig {
    pub(crate) identifier: Option<String>,
    pub(crate) password: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct EmergencyConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    /// Max distance in km for USGS earthquake alerts. None = send all globally.
    #[serde(default)]
    pub(crate) max_distance_km: Option<f64>,
    /// Poll interval in seconds (default 120).
    #[serde(default = "default_alert_poll_secs")]
    pub(crate) poll_interval_secs: u64,
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

pub(crate) fn default_alert_poll_secs() -> u64 {
    120
}

#[derive(Deserialize)]
pub(crate) struct WeatherConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    /// "brightsky" (DWD, default) or "openmeteo".
    #[serde(default)]
    pub(crate) provider: weather::WeatherProvider,
    /// Time of day to post the forecast (HH:MM, 24h). Defaults to first digest time.
    #[serde(default)]
    pub(crate) post_time: Option<String>,
    /// Poll DWD weather warnings and send them immediately through the alert loop.
    #[serde(default = "default_true")]
    pub(crate) alerts_enabled: bool,
    /// Region keywords used to select DWD weather warnings. Empty = derive from
    /// filter.geocode_city/reference_address, e.g. "Berlin".
    #[serde(default)]
    pub(crate) warning_region_keywords: Vec<String>,
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
pub(crate) struct SourceConfig {
    /// Display name shown in posted messages.
    pub(crate) name: String,
    /// Source type. Defaults to "rss" so existing configs need no changes.
    #[serde(rename = "type", default)]
    pub(crate) source_type: sources::SourceType,
    // ── RSS / Atom ────────────────────────────────────────────────────────────
    /// Feed URL (required for type = "rss").
    #[serde(default)]
    pub(crate) url: Option<String>,
    // ── Search-based sources ──────────────────────────────────────────────────
    /// Search query for type = "bluesky" or type = "google_news".
    #[serde(default)]
    pub(crate) query: Option<String>,
    /// Max posts per poll for Bluesky (default 25, API max 100).
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    /// Only keep feed/search items newer than this many hours where supported.
    #[serde(default)]
    pub(crate) max_age_hours: Option<u64>,
    // ── Shared ────────────────────────────────────────────────────────────────
    /// Apply location filter (default true). Set false for hyper-local sources.
    #[serde(default = "default_true")]
    pub(crate) filter: bool,
    /// Implied distance (metres) used as fallback when neither area keywords
    /// nor geocoding produce a result.
    #[serde(default)]
    pub(crate) base_implied_meters: Option<f64>,
    /// Per-source required keywords. When set, overrides the global filter.required.
    /// Set to [] to skip the required check for city-specific sources.
    #[serde(default)]
    pub(crate) required: Option<Vec<String>>,
}

/// Keyword group with an implied distance. When no street can be geocoded,
/// the closest matching group's implied_meters is used as the fallback distance.
#[derive(Deserialize, Clone)]
pub(crate) struct AreaGroup {
    /// Assumed distance from the reference point in metres (e.g. 300 for your street,
    /// 1500 for your district). Lower = more relevant.
    pub(crate) implied_meters: f64,
    #[serde(default)]
    pub(crate) terms: Vec<String>,
}

/// Explicit local places with coordinates. These improve relevance for articles
/// that mention stations, squares, venues, or neighbourhood nicknames instead
/// of street addresses.
#[derive(Deserialize, Clone)]
pub(crate) struct PlaceConfig {
    pub(crate) name: String,
    pub(crate) lat: f64,
    pub(crate) lon: f64,
    #[serde(default)]
    pub(crate) aliases: Vec<String>,
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
pub(crate) struct FilterConfig {
    #[serde(default)]
    pub(crate) required: Vec<String>,
    #[serde(default)]
    pub(crate) blocklist: Vec<String>,
    #[serde(default = "default_digest_threshold")]
    pub(crate) digest_threshold: i32,
    #[serde(default = "default_candidate_min_score")]
    pub(crate) candidate_min_score: i32,
    #[serde(default = "default_candidate_min_sources")]
    pub(crate) candidate_min_sources: usize,
    #[serde(default = "default_candidate_retention_hours")]
    pub(crate) candidate_retention_hours: u64,
    /// Drop items published longer ago than this, for sources without their
    /// own `max_age_hours`. Checked after the article page is fetched, so a
    /// page's own publish date can unmask an old article with a fresh feed date.
    #[serde(default)]
    pub(crate) max_age_hours: Option<u64>,
    #[serde(default)]
    pub(crate) area: Vec<AreaGroup>,
    #[serde(default)]
    pub(crate) places: Vec<PlaceConfig>,
    /// Full address used as the reference point for distance scoring.
    /// Geocoded once at startup via Nominatim.
    pub(crate) reference_address: Option<String>,
    /// City string appended to Nominatim queries when geocoding street mentions
    /// from articles (e.g. "Berlin, Germany"). Anchors results to the right city.
    #[serde(default)]
    pub(crate) geocode_city: Option<String>,
}

pub(crate) fn default_digest_threshold() -> i32 {
    1
}
pub(crate) fn default_candidate_min_score() -> i32 {
    1
}
pub(crate) fn default_candidate_min_sources() -> usize {
    2
}
pub(crate) fn default_candidate_retention_hours() -> u64 {
    24
}

#[derive(Deserialize, Clone)]
pub(crate) struct ScheduleConfig {
    /// How often to poll all sources (minutes).
    #[serde(default = "default_poll_interval")]
    pub(crate) poll_interval_minutes: u64,
    /// Times of day to post a digest (HH:MM, 24h). Each fires independently;
    /// the queue is drained on every posting so digests never overlap.
    #[serde(default = "default_digest_times")]
    pub(crate) digest_times: Vec<String>,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            poll_interval_minutes: default_poll_interval(),
            digest_times: default_digest_times(),
        }
    }
}

pub(crate) fn default_poll_interval() -> u64 {
    30
}
pub(crate) fn default_digest_times() -> Vec<String> {
    vec!["08:00".to_owned()]
}
pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn derive_warning_region_keywords(
    filter: &FilterConfig,
    weather: &WeatherConfig,
) -> Vec<String> {
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
