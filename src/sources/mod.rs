pub mod bluesky;
pub mod rss;

use crate::{FeedItem, SourceConfig};
use serde::Deserialize;

// ── Source type discriminator ─────────────────────────────────────────────────
//
// Adding a new platform:
//   1. Add a variant here.
//   2. Create src/sources/<platform>.rs with a struct + fetch_items().
//   3. Add the variant to SourceAdapter and build_adapter() below.
//   4. Add platform-specific fields to SourceConfig in main.rs (optional).
//   That's it — poll_once() and everything downstream is untouched.

#[derive(Deserialize, Clone, Default, Debug)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    #[default]
    Rss,
    Bluesky,
    // Mastodon,
    // Reddit,
}

// ── Bluesky auth context ──────────────────────────────────────────────────────

/// Credentials and shared token state for all Bluesky sources.
/// Created once in main() and passed to every poll.
pub struct BlueskyContext {
    pub identifier: String,
    pub password: String,
    pub session: bluesky::SharedSession,
}

// ── Adapter enum ──────────────────────────────────────────────────────────────
//
// Each variant wraps a source-specific struct. All structs expose one method:
//   async fn fetch_items(&self, http: &reqwest::Client) -> Vec<FeedItem>
//
// The enum's own fetch_items dispatches to the right impl. poll_once() only
// ever calls this method and never needs to know which source type it's talking to.

pub enum SourceAdapter {
    Rss(rss::RssAdapter),
    Bluesky(bluesky::BlueskyAdapter),
    // Mastodon(mastodon::MastodonAdapter),
}

impl SourceAdapter {
    pub async fn fetch_items(&self, http: &reqwest::Client) -> Vec<FeedItem> {
        match self {
            Self::Rss(a) => a.fetch_items(http).await,
            Self::Bluesky(a) => a.fetch_items(http).await,
        }
    }
}

// ── Factory ───────────────────────────────────────────────────────────────────

pub fn build_adapter(source: &SourceConfig, bluesky: Option<&BlueskyContext>) -> SourceAdapter {
    match source.source_type {
        SourceType::Rss => SourceAdapter::Rss(rss::RssAdapter {
            name: source.name.clone(),
            url: source.url.clone().unwrap_or_default(),
        }),
        SourceType::Bluesky => {
            let (identifier, password, session) = bluesky
                .map(|ctx| (ctx.identifier.clone(), ctx.password.clone(), ctx.session.clone()))
                .unwrap_or_else(|| {
                    (String::new(), String::new(), bluesky::new_shared_session())
                });
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
