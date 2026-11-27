use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::Duration as TokioDuration;
use tracing::warn;

use super::SourceError;
use crate::feed::FeedItem;
use crate::fetch::{FetchError, Http, Response};

fn since_iso8601(hours: u64) -> String {
    let t = chrono::Utc::now() - chrono::Duration::hours(hours as i64);
    t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

const PDS: &str = "https://bsky.social/xrpc";

// ── Session ───────────────────────────────────────────────────────────────────

pub(crate) struct BlueskySession {
    access_jwt: String,
    refresh_jwt: String,
    expires_at: Instant,
}

pub type SharedSession = Arc<Mutex<Option<BlueskySession>>>;

pub fn new_shared_session() -> SharedSession {
    Arc::new(Mutex::new(None))
}

// ── Adapter ───────────────────────────────────────────────────────────────────

pub struct BlueskyAdapter {
    pub name: String,
    pub query: String,
    pub limit: usize,
    pub max_age_hours: u64,
    pub identifier: String,
    pub password: String,
    pub session: SharedSession,
}

impl BlueskyAdapter {
    /// Returns a valid access JWT, refreshing or re-authenticating as needed.
    async fn ensure_token(&self, http: &Http) -> Option<String> {
        let mut lock = self.session.lock().await;

        if let Some(ref sess) = *lock {
            if sess.expires_at > Instant::now() + Duration::from_secs(300) {
                return Some(sess.access_jwt.clone());
            }
            if let Some(new_sess) = refresh_session(http, &sess.refresh_jwt).await {
                let token = new_sess.access_jwt.clone();
                *lock = Some(new_sess);
                return Some(token);
            }
            warn!("Bluesky token refresh failed, re-authenticating");
        }

        match create_session(http, &self.identifier, &self.password).await {
            Some(sess) => {
                let token = sess.access_jwt.clone();
                *lock = Some(sess);
                Some(token)
            }
            None => {
                warn!("Bluesky authentication failed for {}", self.identifier);
                None
            }
        }
    }

    pub(crate) async fn fetch_items(&self, http: &Http) -> Result<Vec<FeedItem>, SourceError> {
        let Some(token) = self.ensure_token(http).await else {
            return Err(SourceError::Unavailable("Bluesky login failed".to_owned()));
        };

        let url = format!(
            "{PDS}/app.bsky.feed.searchPosts?q={}&limit={}&sort=latest&since={}",
            crate::url_encode(&self.query),
            self.limit.min(100),
            crate::url_encode(&since_iso8601(self.max_age_hours)),
        );

        let bearer = format!("Bearer {token}");
        let headers = [
            ("Accept", "application/json"),
            ("Authorization", bearer.as_str()),
        ];
        let response = match http.get(&url, &headers, TokioDuration::from_secs(15)).await {
            Ok(r) => r,
            Err(e) => {
                if e == FetchError::Timeout {
                    warn!("Bluesky fetch timed out [{}]", self.name);
                } else {
                    warn!("Bluesky fetch failed [{}]: {e}", self.name);
                }
                return Err(SourceError::from_fetch(e));
            }
        };
        // Not JSON with posts: a refusal when the status says so, else a malformed answer.
        let failed = |why: String| {
            if response.is_success() {
                SourceError::Malformed(why)
            } else {
                SourceError::from_status(response.status)
            }
        };

        let json: serde_json::Value = match serde_json::from_str(&response.body) {
            Ok(j) => j,
            Err(e) => {
                warn!("Bluesky JSON parse failed [{}]: {e}", self.name);
                return Err(failed(e.to_string()));
            }
        };

        let Some(posts) = json["posts"].as_array() else {
            warn!(
                "Bluesky response missing 'posts' array [{}]: {:.200}",
                self.name, response.body
            );
            return Err(failed("no posts in the answer".to_owned()));
        };

        Ok(posts.iter().filter_map(|p| self.normalize(p)).collect())
    }

    fn normalize(&self, post: &serde_json::Value) -> Option<FeedItem> {
        let uri = post["uri"].as_str()?;
        let handle = post["author"]["handle"].as_str().unwrap_or("unknown");
        // URI format: at://did:plc:{id}/app.bsky.feed.post/{rkey}
        let rkey = uri.split('/').next_back()?;
        let text = post["record"]["text"].as_str().unwrap_or("").trim();
        if text.is_empty() {
            return None;
        }

        let title: String = text
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or(text)
            .trim()
            .chars()
            .take(120)
            .collect();

        let link = format!("https://bsky.app/profile/{handle}/post/{rkey}");

        Some(FeedItem {
            guid: uri.to_owned(),
            title,
            link: Some(link),
            link_note: None,
            description: Some(text.to_owned()),
            // Pre-fill article_text so process_item() skips the HTTP article fetch.
            article_text: Some(text.to_owned()),
            source_name: self.name.clone(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            location_label: None,
            published_at: post["record"]["createdAt"]
                .as_str()
                .and_then(crate::parse_feed_date),
            ..Default::default()
        })
    }
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

async fn create_session(http: &Http, identifier: &str, password: &str) -> Option<BlueskySession> {
    let body = format!(
        r#"{{"identifier":{},"password":{}}}"#,
        serde_json::to_string(identifier).ok()?,
        serde_json::to_string(password).ok()?,
    );

    let resp = match http
        .post(
            &format!("{PDS}/com.atproto.server.createSession"),
            &[("Content-Type", "application/json")],
            body,
            TokioDuration::from_secs(15),
        )
        .await
    {
        Ok(r) => r,
        Err(FetchError::Timeout) => {
            warn!("Bluesky createSession timed out");
            return None;
        }
        Err(e) => {
            warn!("Bluesky createSession request failed: {e}");
            return None;
        }
    };

    parse_session_response(resp, "createSession")
}

async fn refresh_session(http: &Http, refresh_jwt: &str) -> Option<BlueskySession> {
    let bearer = format!("Bearer {refresh_jwt}");
    let resp = match http
        .post(
            &format!("{PDS}/com.atproto.server.refreshSession"),
            &[("Authorization", bearer.as_str())],
            String::new(),
            TokioDuration::from_secs(15),
        )
        .await
    {
        Ok(r) => r,
        Err(FetchError::Timeout) => {
            warn!("Bluesky refreshSession timed out");
            return None;
        }
        Err(e) => {
            warn!("Bluesky refreshSession request failed: {e}");
            return None;
        }
    };

    parse_session_response(resp, "refreshSession")
}

fn parse_session_response(resp: Response, op: &str) -> Option<BlueskySession> {
    let body = resp.body;

    let json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(j) => j,
        Err(e) => {
            warn!("Bluesky {op} JSON parse failed: {e}");
            return None;
        }
    };

    if let Some(err) = json["error"].as_str() {
        warn!(
            "Bluesky {op} error: {}, {}",
            err,
            json["message"].as_str().unwrap_or("")
        );
        return None;
    }

    let access_jwt = json["accessJwt"].as_str()?.to_owned();
    let refresh_jwt = json["refreshJwt"].as_str()?.to_owned();

    Some(BlueskySession {
        access_jwt,
        refresh_jwt,
        // Bluesky access tokens last ~2h; treat as 90 min to refresh early.
        expires_at: Instant::now() + Duration::from_secs(90 * 60),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_posts_become_items() {
        let adapter = BlueskyAdapter {
            name: "Bluesky #YourCity".to_owned(),
            query: "#YourCity".to_owned(),
            limit: 25,
            max_age_hours: 24,
            identifier: String::new(),
            password: String::new(),
            session: new_shared_session(),
        };
        let json: serde_json::Value =
            serde_json::from_str(&crate::testing::fixture("bluesky/search.json")).unwrap();

        let items: Vec<FeedItem> = json["posts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| adapter.normalize(p))
            .collect();

        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0].link.as_deref(),
            Some("https://bsky.app/profile/neighbour.example.social/post/3kfix00000001")
        );
        assert!(items[0].title.starts_with("Großes Kiezfest am Samstag"));
        assert_eq!(items[0].article_text, items[0].description);
        assert_eq!(items[0].published_at, Some(1_795_169_700));
    }
}
