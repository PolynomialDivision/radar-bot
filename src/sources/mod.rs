pub mod bluesky;
pub mod rss;

use crate::{
    fetch::{FetchError, Http},
    FeedItem, SourceConfig,
};
use serde::Deserialize;
use tracing::warn;

// ── Why a source gave nothing ─────────────────────────────────────────────────

/// A poll of a source that did not work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceError {
    /// Down or overloaded for now: network, timeout, 429, 5xx.
    Unavailable(String),
    /// The source refuses the request (401, 403, 404, 410, …): a configuration to fix.
    Refused(u16),
    /// The answer is not what the source should send.
    Malformed(String),
}

impl SourceError {
    pub(crate) fn from_status(status: u16) -> Self {
        match status {
            429 | 500..=599 => SourceError::Unavailable(format!("HTTP {status}")),
            _ => SourceError::Refused(status),
        }
    }

    pub(crate) fn from_fetch(error: FetchError) -> Self {
        match error {
            FetchError::TooLarge(cap) => {
                SourceError::Malformed(format!("answer larger than {cap} bytes"))
            }
            other => SourceError::Unavailable(other.to_string()),
        }
    }
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceError::Unavailable(why) => write!(f, "unavailable: {why}"),
            SourceError::Refused(status) => write!(f, "refused: HTTP {status}"),
            SourceError::Malformed(why) => write!(f, "malformed: {why}"),
        }
    }
}

// ── Source type discriminator ─────────────────────────────────────────────────
//
// Adding a new platform:
//   1. Add a variant here.
//   2. Create src/sources/<platform>.rs with a struct + fetch_items().
//   3. Add the variant to SourceAdapter and build_adapter() below.
//   4. Add platform-specific fields to SourceConfig in main.rs (optional).
//   That's it: poll_once() and everything downstream is untouched.

#[derive(Deserialize, Clone, Default, Debug)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    #[default]
    Rss,
    GoogleNews,
    Bluesky,
    // Mastodon,
    // Reddit,
}

// ── Bluesky auth context ──────────────────────────────────────────────────────

/// Credentials and shared token state for all Bluesky sources.
/// Created once in main() and passed to every poll.
#[derive(Clone)]
pub struct BlueskyContext {
    pub identifier: String,
    pub password: String,
    pub session: bluesky::SharedSession,
}

// ── Adapter enum ──────────────────────────────────────────────────────────────
//
// Each variant wraps a source-specific struct. All structs expose one method:
//   async fn fetch_items(&self, http: &Http) -> Result<Vec<FeedItem>, SourceError>
//
// The enum's own fetch_items dispatches to the right impl. poll_once() only
// ever calls this method and never needs to know which source type it's talking to.

pub enum SourceAdapter {
    Rss(rss::RssAdapter),
    Bluesky(bluesky::BlueskyAdapter),
    // Mastodon(mastodon::MastodonAdapter),
}

/// What a source answered: its items (after the source's own age filter), and the publish date
/// of the newest entry it lists (before that filter), which tells a feed that went stale.
#[derive(Debug, Default)]
pub(crate) struct Fetched {
    pub(crate) items: Vec<FeedItem>,
    pub(crate) newest_entry_at: Option<i64>,
}

impl From<Vec<FeedItem>> for Fetched {
    fn from(items: Vec<FeedItem>) -> Self {
        Fetched {
            items,
            newest_entry_at: None,
        }
    }
}

impl SourceAdapter {
    pub(crate) async fn fetch_items(&self, http: &Http) -> Result<Fetched, SourceError> {
        match self {
            Self::Rss(a) => a.fetch_items(http).await,
            Self::Bluesky(a) => a.fetch_items(http).await.map(Fetched::from),
        }
    }
}

// ── Factory ───────────────────────────────────────────────────────────────────

pub fn build_adapter(source: &SourceConfig, bluesky: Option<&BlueskyContext>) -> SourceAdapter {
    match source.source_type {
        SourceType::Rss => SourceAdapter::Rss(rss::RssAdapter {
            name: source.name.clone(),
            url: source.url.clone().unwrap_or_default(),
            max_age_hours: source.max_age_hours,
            search: false,
        }),
        SourceType::GoogleNews => SourceAdapter::Rss(rss::RssAdapter {
            name: source.name.clone(),
            url: google_news_rss_url(&source.name, source.query.as_deref().unwrap_or_default()),
            max_age_hours: source.max_age_hours,
            search: true,
        }),
        SourceType::Bluesky => {
            let (identifier, password, session) = bluesky
                .map(|ctx| {
                    (
                        ctx.identifier.clone(),
                        ctx.password.clone(),
                        ctx.session.clone(),
                    )
                })
                .unwrap_or_else(|| (String::new(), String::new(), bluesky::new_shared_session()));
            SourceAdapter::Bluesky(bluesky::BlueskyAdapter {
                name: source.name.clone(),
                query: source.query.clone().unwrap_or_default(),
                limit: source.limit.unwrap_or(25),
                max_age_hours: source.max_age_hours.unwrap_or(24),
                identifier,
                password,
                session,
            })
        }
    }
}

fn google_news_rss_url(source_name: &str, query: &str) -> String {
    if query.trim().is_empty() {
        warn!("Google News source '{source_name}' has no query configured, skipping");
        return String::new();
    }
    let q = crate::url_encode(query);
    format!("https://news.google.com/rss/search?q={q}&hl=de&gl=DE&ceid=DE:de")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_news_source_builds_german_rss_search_url() {
        let url = google_news_rss_url("Google News Test", r#""Britzer Allee" Berlin Unfall"#);

        assert_eq!(
            url,
            "https://news.google.com/rss/search?q=%22Britzer+Allee%22+Berlin+Unfall&hl=de&gl=DE&ceid=DE:de"
        );
    }
}
