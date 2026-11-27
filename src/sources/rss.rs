use tokio::time::Duration;
use tracing::{debug, warn};

use super::SourceError;
use crate::feed::FeedItem;
use crate::fetch::Http;

pub struct RssAdapter {
    pub name: String,
    pub url: String,
    /// Drop items older than this many hours. None = no limit.
    pub max_age_hours: Option<u64>,
}

impl RssAdapter {
    pub(crate) async fn fetch_items(&self, http: &Http) -> Result<Vec<FeedItem>, SourceError> {
        if self.url.trim().is_empty() {
            return Err(SourceError::Malformed("no URL configured".to_owned()));
        }

        let (response, unchanged) = http
            .get_feed(&self.url, Duration::from_secs(30))
            .await
            .map_err(|e| {
                warn!("Failed to fetch RSS '{}': {e}", self.name);
                SourceError::from_fetch(e)
            })?;
        if unchanged && response.status == 304 {
            // Not modified, and nothing kept from before (a restart): no new items.
            return Ok(Vec::new());
        }
        if !response.is_success() {
            warn!(
                "RSS '{}' answered HTTP {} ({}), no feed",
                self.name,
                response.status,
                response.content_type()
            );
            return Err(SourceError::from_status(response.status));
        }

        let mut items = match crate::parse_feed_entries(
            &response.bytes,
            response.content_type.as_deref(),
            &self.name,
        ) {
            Ok(items) if !items.is_empty() => items,
            Ok(_) => {
                warn!(
                    "RSS '{}' has no entries (HTTP {}, {}, {} bytes)",
                    self.name,
                    response.status,
                    response.content_type(),
                    response.body.len()
                );
                return Ok(Vec::new());
            }
            Err(e) => {
                warn!(
                    "RSS '{}' is not a feed (HTTP {}, {}): {e}",
                    self.name,
                    response.status,
                    response.content_type()
                );
                return Err(SourceError::Malformed(e));
            }
        };

        if let Some(max_age) = self.max_age_hours {
            let cutoff = chrono::Utc::now().timestamp() - (max_age as i64 * 3600);
            let before = items.len();
            // Items with no date are kept (fail open, better to over-include than silently drop).
            items.retain(|item| item.published_at.is_none_or(|ts| ts >= cutoff));
            let dropped = before - items.len();
            if dropped > 0 {
                debug!(
                    "RSS age filter [{}]: dropped {dropped} item(s) older than {max_age}h",
                    self.name
                );
            }
        }

        Ok(items)
    }
}
