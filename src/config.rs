//! Configuration: `[filter]`, `[schedule]`, `[weather]`, `[emergency]`, sources.

// ── Config ────────────────────────────────────────────────────────────────────

use mxbot_common::config::{MatrixConfig, SecurityConfig, TimeOfDay};
use serde::Deserialize;

use crate::{sources, weather};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub(crate) http: HttpConfig,
}

/// `[http]`: the User-Agents of outbound requests.
#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpConfig {
    /// Sent to APIs (geocoder, warnings, forecast). Default `radar-bot/<version>`.
    #[serde(default = "crate::fetch::default_api_agent")]
    pub(crate) user_agent: String,
    /// Sent to feeds and article pages.
    #[serde(default = "crate::fetch::default_page_agent")]
    pub(crate) page_user_agent: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            user_agent: crate::fetch::default_api_agent(),
            page_user_agent: crate::fetch::default_page_agent(),
        }
    }
}

impl HttpConfig {
    pub(crate) fn agents(&self) -> crate::fetch::Agents {
        crate::fetch::Agents {
            api: self.user_agent.clone(),
            page: self.page_user_agent.clone(),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlueskyConfig {
    pub(crate) identifier: Option<String>,
    pub(crate) password: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct EmergencyConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    /// Max distance in km for USGS/GDACS alerts (default 500, the same whether
    /// or not an [emergency] section exists). A huge value such as 20000
    /// receives everything worldwide.
    #[serde(default = "default_alert_max_distance_km")]
    pub(crate) max_distance_km: Option<f64>,
    /// Poll interval in seconds (default 120).
    #[serde(default = "default_alert_poll_secs")]
    pub(crate) poll_interval_secs: u64,
    /// Earthquakes (USGS) and global disasters (GDACS), every 15th round. Off by default:
    /// within the usual radius they have nothing to report.
    #[serde(default)]
    pub(crate) quakes: bool,
}

impl Default for EmergencyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_distance_km: default_alert_max_distance_km(),
            poll_interval_secs: default_alert_poll_secs(),
            quakes: false,
        }
    }
}

pub(crate) fn default_alert_max_distance_km() -> Option<f64> {
    Some(500.0)
}

pub(crate) fn default_alert_poll_secs() -> u64 {
    120
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WeatherConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    /// "brightsky" (DWD, default) or "openmeteo".
    #[serde(default)]
    pub(crate) provider: weather::WeatherProvider,
    /// Time of day to post the forecast (HH:MM, 24h). Defaults to first digest time.
    #[serde(default)]
    pub(crate) post_time: Option<TimeOfDay>,
    /// Poll DWD weather warnings and send them immediately through the alert loop.
    #[serde(default = "default_true")]
    pub(crate) alerts_enabled: bool,
    /// Region keywords used to select DWD weather warnings. Empty = derive from
    /// filter.geocode_city/reference_address, e.g. "Berlin".
    #[serde(default)]
    pub(crate) warning_region_keywords: Vec<String>,
    /// Post DWD warnings this many hours before they take effect (default 3),
    /// so a storm warning issued in the morning for the afternoon arrives in
    /// the morning.
    #[serde(default = "default_warning_lead_hours")]
    pub(crate) warning_lead_hours: u64,
}

pub(crate) fn default_warning_lead_hours() -> u64 {
    3
}

/// The forecast's time when `[weather] post_time` is not set and there is no digest time.
const DEFAULT_FORECAST_TIME: TimeOfDay = TimeOfDay { hour: 8, minute: 0 };

impl WeatherConfig {
    /// When the forecast is posted: `post_time`, else the first digest time, else 08:00.
    pub(crate) fn forecast_time(&self, schedule: &ScheduleConfig) -> anyhow::Result<TimeOfDay> {
        match (self.post_time, schedule.digest_times.first()) {
            (Some(time), _) => Ok(time),
            (None, Some(digest)) => digest
                .parse::<TimeOfDay>()
                .map_err(|e| anyhow::anyhow!("Time {digest:?}: {e:#}")),
            (None, None) => Ok(DEFAULT_FORECAST_TIME),
        }
    }
}

impl Default for WeatherConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            provider: weather::WeatherProvider::Brightsky,
            post_time: None,
            alerts_enabled: true,
            warning_region_keywords: Vec::new(),
            warning_lead_hours: default_warning_lead_hours(),
        }
    }
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceConfig {
    /// Display name shown in posted messages. It may change freely; `id` may not.
    pub(crate) name: String,
    /// The source's identity in the store, forever (default: the name as a slug). Set it
    /// before renaming a source, to the slug of its old name.
    #[serde(default)]
    pub(crate) id: Option<String>,
    /// What the source covers: "local" (the neighbourhood), "city" or "wide". Default from
    /// the old keys: `filter = false` is local, `required = []` with
    /// `base_implied_meters >= 5000` is city, anything else wide.
    #[serde(default)]
    pub(crate) scope: Option<crate::model::Scope>,
    /// A police, fire brigade or district office feed (counts like a second publisher).
    #[serde(default)]
    pub(crate) official: bool,
    /// The category its items have when no keyword says otherwise.
    #[serde(default)]
    pub(crate) category: Option<crate::model::Category>,
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
    /// A feed whose newest entry is older than this is stale (default 45; not for searches).
    #[serde(default)]
    pub(crate) stale_after_days: Option<u64>,
}

impl SourceConfig {
    /// The source's identity: `id`, else its name lower-cased, each run of other characters
    /// than letters and digits one `-` ("Polizei Berlin" → "polizei-berlin").
    pub(crate) fn id(&self) -> String {
        self.id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map_or_else(|| slug(&self.name), str::to_owned)
    }

    /// The scope: configured, or from the old keys.
    pub(crate) fn scope(&self) -> crate::model::Scope {
        use crate::model::Scope;
        if let Some(scope) = self.scope {
            return scope;
        }
        let city_wide = self.required.as_ref().is_some_and(Vec::is_empty)
            && self.base_implied_meters.is_some_and(|m| m >= 5_000.0);
        if !self.filter {
            Scope::Local
        } else if city_wide {
            Scope::City
        } else {
            Scope::Wide
        }
    }

    /// A social network search rather than a publisher.
    pub(crate) fn social(&self) -> bool {
        matches!(self.source_type, sources::SourceType::Bluesky)
    }
}

/// `name` lower-cased, each run of characters that are not letters or digits one `-`.
pub(crate) fn slug(name: &str) -> String {
    let mut slug = String::new();
    for c in name.trim().to_lowercase().chars() {
        if c.is_alphanumeric() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.trim_end_matches('-').to_owned()
}

/// Keyword group with an implied distance. When no street can be geocoded,
/// the closest matching group's implied_meters is used as the fallback distance.
#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
    /// Places that are *not* near you ("Kreuzberg", "Neukölln"). An item whose
    /// title names one of these, but none of your own area terms or places,
    /// loses one tier on location evidence taken from its text.
    #[serde(default)]
    pub(crate) elsewhere: Vec<String>,
    /// Bezirk of the reference address, resolved at startup. Items a source
    /// files under another Bezirk (Polizei Berlin "Ereignisort") are treated
    /// like items naming an `elsewhere` place.
    #[serde(skip)]
    pub(crate) home_borough: Option<String>,
    /// Full address used as the reference point for distance scoring.
    /// Geocoded once at startup via Nominatim.
    pub(crate) reference_address: Option<String>,
    /// The reference point itself (both or neither): with it, a start needs no geocoder; the
    /// city, district and postal code are still looked up in the background.
    #[serde(default)]
    pub(crate) lat: Option<f64>,
    #[serde(default)]
    pub(crate) lon: Option<f64>,
    /// City string appended to Nominatim queries when geocoding street mentions
    /// from articles (e.g. "Berlin, Germany"). Anchors results to the right city.
    #[serde(default)]
    pub(crate) geocode_city: Option<String>,
}

pub(crate) fn default_digest_threshold() -> i32 {
    2
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
#[serde(deny_unknown_fields)]
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

impl Config {
    /// What serde cannot check: every source can be polled and has a name of its own, terms
    /// and places are usable, scores and times are valid. An error names what to fix.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        use anyhow::{bail, ensure};

        validate_sources(&self.sources)?;
        let mut names = std::collections::HashSet::new();
        for source in &self.sources {
            ensure!(!source.name.trim().is_empty(), "A source has no name");
            ensure!(
                names.insert(source.name.trim().to_lowercase()),
                "Two sources are named {:?}: statistics and health are kept by name",
                source.name
            );
        }
        let mut ids = std::collections::HashSet::new();
        for source in &self.sources {
            ensure!(
                ids.insert(source.id()),
                "Two sources have the id {:?}: give one an `id` of its own",
                source.id()
            );
        }
        let bluesky = self
            .sources
            .iter()
            .any(|s| matches!(s.source_type, sources::SourceType::Bluesky));
        if bluesky && (self.bluesky.identifier.is_none() || self.bluesky.password.is_none()) {
            bail!("Bluesky sources need [bluesky] identifier and password");
        }

        let filter = &self.filter;
        for (key, score) in [
            ("digest_threshold", filter.digest_threshold),
            ("candidate_min_score", filter.candidate_min_score),
        ] {
            ensure!(
                (0..=5).contains(&score),
                "[filter] {key} = {score}: scores are 0 to 5"
            );
        }
        for (key, terms) in [
            ("required", &filter.required),
            ("blocklist", &filter.blocklist),
            ("elsewhere", &filter.elsewhere),
        ] {
            ensure!(
                terms.iter().all(|t| !t.trim().is_empty()),
                "[filter] {key} has an empty term"
            );
        }
        for (i, group) in filter.area.iter().enumerate() {
            ensure!(
                group.implied_meters.is_finite() && group.implied_meters >= 0.0,
                "[[filter.area]] #{}: implied_meters must be a distance",
                i + 1
            );
            ensure!(
                group.terms.iter().all(|t| !t.trim().is_empty()),
                "[[filter.area]] #{}: an empty term matches everything",
                i + 1
            );
        }
        match (filter.lat, filter.lon) {
            (Some(lat), Some(lon)) => ensure!(
                (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon),
                "[filter] lat {lat} / lon {lon} is no coordinate"
            ),
            (None, None) => {}
            _ => bail!("[filter] lat and lon go together"),
        }
        for place in &filter.places {
            ensure!(
                !place.name.trim().is_empty(),
                "A [[filter.places]] entry has no name"
            );
            ensure!(
                (-90.0..=90.0).contains(&place.lat) && (-180.0..=180.0).contains(&place.lon),
                "[[filter.places]] {:?}: lat {} / lon {} is no coordinate",
                place.name,
                place.lat,
                place.lon
            );
            ensure!(
                place.aliases.iter().all(|a| !a.trim().is_empty()),
                "[[filter.places]] {:?}: an empty alias",
                place.name
            );
        }

        for time in &self.schedule.digest_times {
            time.parse::<TimeOfDay>()
                .map_err(|e| anyhow::anyhow!("Time {time:?}: {e:#}"))?;
        }
        if self.weather.enabled {
            let forecast = self.weather.forecast_time(&self.schedule)?;
            ensure!(
                forecast.hour < 23,
                "The forecast time {forecast} leaves no period of the day to show: \
                 set [weather] post_time before 23:00"
            );
        }
        for (key, agent) in [
            ("user_agent", &self.http.user_agent),
            ("page_user_agent", &self.http.page_user_agent),
        ] {
            ensure!(
                !agent.trim().is_empty() && reqwest::header::HeaderValue::from_str(agent).is_ok(),
                "[http] {key} {agent:?} is no usable User-Agent"
            );
        }
        ensure!(
            self.schedule.poll_interval_minutes > 0,
            "[schedule] poll_interval_minutes must be at least 1"
        );
        ensure!(
            self.emergency.poll_interval_secs > 0,
            "[emergency] poll_interval_secs must be at least 1"
        );
        Ok(())
    }
}

/// Every source can be polled: an RSS feed has a URL, a search has a query.
pub(crate) fn validate_sources(sources: &[SourceConfig]) -> anyhow::Result<()> {
    for source in sources {
        let missing = |field: &Option<String>| field.as_deref().is_none_or(|v| v.trim().is_empty());
        match source.source_type {
            sources::SourceType::Rss if missing(&source.url) => {
                anyhow::bail!("Source {:?}: an RSS source needs a url", source.name)
            }
            sources::SourceType::GoogleNews | sources::SourceType::Bluesky
                if missing(&source.query) =>
            {
                anyhow::bail!("Source {:?}: a search source needs a query", source.name)
            }
            _ => {}
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> Config {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.toml.example");
        mxbot_common::config::load_toml(&path).expect("config.toml.example loads")
    }

    #[test]
    fn the_example_configuration_loads_and_validates() {
        let config = example();
        config.validate().unwrap();
        assert_eq!(config.filter.digest_threshold, 2);
        assert!(config.sources.len() >= 5);
    }

    #[test]
    fn a_misspelt_key_fails_at_start_with_its_name() {
        let example = std::fs::read_to_string(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.toml.example"),
        )
        .unwrap();
        let typo = example.replace("digest_threshold = 2", "digest_treshold = 2");
        assert_ne!(typo, example);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, typo).unwrap();

        let error = mxbot_common::config::load_toml::<Config>(&path)
            .err()
            .unwrap();

        assert!(
            format!("{error:#}").contains("digest_treshold"),
            "{error:#}"
        );
    }

    #[test]
    fn validation_names_what_to_fix() {
        let invalid = |change: &dyn Fn(&mut Config)| {
            let mut config = example();
            change(&mut config);
            format!("{:#}", config.validate().unwrap_err())
        };

        let duplicate = invalid(&|c| {
            let first = c.sources[0].clone();
            c.sources.push(first);
        });
        assert!(duplicate.contains("Two sources are named"), "{duplicate}");
        assert!(invalid(&|c| c.filter.places[0].lat = 152.0).contains("no coordinate"));
        assert!(invalid(&|c| c.schedule.digest_times.push("25:00".to_owned())).contains("25:00"));
        assert!(invalid(&|c| c.filter.digest_threshold = 7).contains("digest_threshold"));
        assert!(invalid(&|c| c.filter.area[0].terms.push(" ".to_owned())).contains("empty term"));
        assert!(invalid(&|c| c.bluesky.password = None).contains("[bluesky]"));
        assert!(invalid(&|c| c.sources[0].url = None).contains("needs a url"));
    }

    #[test]
    fn the_forecast_time_is_parsed_once_and_must_leave_a_period() {
        let mut config = example();
        config.weather.post_time = Some(TimeOfDay::new(9, 30));
        assert_eq!(
            config.weather.forecast_time(&config.schedule).unwrap(),
            TimeOfDay::new(9, 30)
        );
        config.weather.post_time = None;
        config.schedule.digest_times = vec!["07:15".to_owned()];
        assert_eq!(
            config.weather.forecast_time(&config.schedule).unwrap(),
            TimeOfDay::new(7, 15)
        );

        config.weather.post_time = Some(TimeOfDay::new(23, 0));
        let error = format!("{:#}", config.validate().unwrap_err());
        assert!(error.contains("23:00"), "{error}");
        config.weather.enabled = false;
        config.validate().unwrap();
    }

    #[test]
    fn user_agents_default_to_the_bot_and_the_page_agent() {
        let config = example();
        assert_eq!(
            config.http.user_agent,
            format!("radar-bot/{}", env!("CARGO_PKG_VERSION"))
        );
        assert!(config.http.page_user_agent.starts_with("Mozilla/5.0"));

        let mut broken = example();
        broken.http.user_agent = "radar\nbot".to_owned();
        assert!(format!("{:#}", broken.validate().unwrap_err()).contains("[http] user_agent"));
    }

    #[test]
    fn a_source_id_is_its_name_as_a_slug() {
        assert_eq!(slug("Polizei Berlin"), "polizei-berlin");
        assert_eq!(slug("  Bluesky #YourCity "), "bluesky-yourcity");
        assert_eq!(slug("Straßen & Plätze!"), "straßen-plätze");
    }

    #[test]
    fn a_reference_point_needs_both_coordinates() {
        let mut config = example();
        config.filter.lat = Some(52.47);
        assert!(format!("{:#}", config.validate().unwrap_err()).contains("go together"));
        config.filter.lon = Some(13.42);
        config.validate().unwrap();
        config.filter.lon = Some(213.42);
        assert!(format!("{:#}", config.validate().unwrap_err()).contains("no coordinate"));
    }

    #[test]
    fn sources_get_a_scope_and_an_id() {
        use crate::model::Scope;
        let config = example();
        let scope = |name: &str| {
            config
                .sources
                .iter()
                .find(|s| s.name == name)
                .unwrap()
                .scope()
        };
        assert_eq!(scope("Local Police"), Scope::City);
        assert_eq!(scope("Neighbourhood Blog"), Scope::Local);
        assert_eq!(scope("National News"), Scope::Wide);

        let mut renamed = config.sources[0].clone();
        renamed.id = Some("local-police".to_owned());
        renamed.name = "Police".to_owned();
        assert_eq!(renamed.id(), config.sources[0].id());

        let mut twice = example();
        let mut copy = twice.sources[1].clone();
        copy.name = "Another name".to_owned();
        copy.id = Some(twice.sources[0].id());
        twice.sources.push(copy);
        assert!(format!("{:#}", twice.validate().unwrap_err()).contains("id"));
    }
}
