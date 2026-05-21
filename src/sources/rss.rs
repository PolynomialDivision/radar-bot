use crate::FeedItem;
use tokio::time::Duration;
use tracing::warn;

pub struct RssAdapter {
    pub name: String,
    pub url: String,
}

impl RssAdapter {
    pub async fn fetch_items(&self, http: &reqwest::Client) -> Vec<FeedItem> {
        let resp = tokio::time::timeout(
            Duration::from_secs(30),
            http.get(&self.url).send(),
        )
        .await;

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

        crate::parse_feed(&xml, &self.name)
    }
}
