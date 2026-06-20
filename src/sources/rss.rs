use crate::FeedItem;
use tokio::time::Duration;
use tracing::warn;

pub struct RssAdapter {
    pub name: String,
    pub url: String,
    /// Drop items older than this many hours. None = no limit.
    pub max_age_hours: Option<u64>,
}

impl RssAdapter {
    pub async fn fetch_items(&self, http: &reqwest::Client) -> Vec<FeedItem> {
        let resp = tokio::time::timeout(Duration::from_secs(30), http.get(&self.url).send()).await;

        let xml = match resp {
            Ok(Ok(r)) => match r.text().await {
                Ok(t) => t,
                Err(e) => {
                    warn!("Failed to read RSS response from '{}': {e}", self.name);
                    return vec![];
                }
            },
            Ok(Err(e)) => {
                warn!("Failed to fetch RSS '{}': {e}", self.name);
                return vec![];
            }
            Err(_) => {
                warn!("RSS fetch timed out for '{}'", self.name);
                return vec![];
            }
        };

        let mut items = crate::parse_feed(&xml, &self.name);

        if let Some(max_age) = self.max_age_hours {
            let cutoff = chrono::Utc::now().timestamp() - (max_age as i64 * 3600);
            let before = items.len();
            // Items with no date are kept (fail open — better to over-include than silently drop).
            items.retain(|item| item.published_at.map_or(true, |ts| ts >= cutoff));
            let dropped = before - items.len();
            if dropped > 0 {
                warn!(
                    "RSS age filter [{}]: dropped {dropped} item(s) older than {max_age}h",
                    self.name
                );
            }
        }

        items
    }
}
