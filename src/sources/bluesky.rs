use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::Duration as TokioDuration;
use tracing::warn;

use crate::FeedItem;

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
    async fn ensure_token(&self, http: &reqwest::Client) -> Option<String> {
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
            warn!("Bluesky token refresh failed — re-authenticating");
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

    pub async fn fetch_items(&self, http: &reqwest::Client) -> Vec<FeedItem> {
        let token = match self.ensure_token(http).await {
            Some(t) => t,
            None => return vec![],
        };

        let url = format!(
            "{PDS}/app.bsky.feed.searchPosts?q={}&limit={}&sort=latest&since={}",
            crate::url_encode(&self.query),
            self.limit.min(100),
            crate::url_encode(&since_iso8601(self.max_age_hours)),
        );

        let resp = match tokio::time::timeout(
            TokioDuration::from_secs(15),
            http.get(&url)
                .header("Accept", "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .send(),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                warn!("Bluesky fetch failed [{}]: {e}", self.name);
                return vec![];
            }
            Err(_) => {
                warn!("Bluesky fetch timed out [{}]", self.name);
                return vec![];
            }
        };

        let body = match resp.text().await {
            Ok(b) => b,
            Err(e) => {
                warn!("Bluesky response read failed [{}]: {e}", self.name);
                return vec![];
            }
        };

        let json: serde_json::Value = match serde_json::from_str(&body) {
            Ok(j) => j,
            Err(e) => {
                warn!("Bluesky JSON parse failed [{}]: {e}", self.name);
                return vec![];
            }
        };

        let Some(posts) = json["posts"].as_array() else {
            warn!("Bluesky response missing 'posts' array [{}]: {body:.200}", self.name);
            return vec![];
        };

        posts.iter().filter_map(|p| self.normalize(p)).collect()
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
            description: Some(text.to_owned()),
            // Pre-fill article_text so process_item() skips the HTTP article fetch.
            article_text: Some(text.to_owned()),
            source_name: self.name.clone(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            published_at: post["record"]["createdAt"].as_str().and_then(crate::parse_feed_date),
        })
    }
}

// ── Auth helpers ──────────────────────────────────────────────────────────────

async fn create_session(http: &reqwest::Client, identifier: &str, password: &str) -> Option<BlueskySession> {
    let body = format!(r#"{{"identifier":{},"password":{}}}"#,
        serde_json::to_string(identifier).ok()?,
        serde_json::to_string(password).ok()?,
    );

    let resp = match tokio::time::timeout(
        TokioDuration::from_secs(15),
        http.post(format!("{PDS}/com.atproto.server.createSession"))
            .header("Content-Type", "application/json")
            .body(body)
            .send(),
    )
    .await
    {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => { warn!("Bluesky createSession request failed: {e}"); return None; }
        Err(_)    => { warn!("Bluesky createSession timed out"); return None; }
    };

    parse_session_response(resp, "createSession").await
}

async fn refresh_session(http: &reqwest::Client, refresh_jwt: &str) -> Option<BlueskySession> {
    let resp = match tokio::time::timeout(
        TokioDuration::from_secs(15),
        http.post(format!("{PDS}/com.atproto.server.refreshSession"))
            .header("Authorization", format!("Bearer {refresh_jwt}"))
            .send(),
    )
    .await
    {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => { warn!("Bluesky refreshSession request failed: {e}"); return None; }
        Err(_)    => { warn!("Bluesky refreshSession timed out"); return None; }
    };

    parse_session_response(resp, "refreshSession").await
}

async fn parse_session_response(resp: reqwest::Response, op: &str) -> Option<BlueskySession> {
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => { warn!("Bluesky {op} response read failed: {e}"); return None; }
    };

    let json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(j) => j,
        Err(e) => { warn!("Bluesky {op} JSON parse failed: {e}"); return None; }
    };

    if let Some(err) = json["error"].as_str() {
        warn!("Bluesky {op} error: {} — {}", err, json["message"].as_str().unwrap_or(""));
        return None;
    }

    let access_jwt  = json["accessJwt"].as_str()?.to_owned();
    let refresh_jwt = json["refreshJwt"].as_str()?.to_owned();

    Some(BlueskySession {
        access_jwt,
        refresh_jwt,
        // Bluesky access tokens last ~2h; treat as 90 min to refresh early.
        expires_at: Instant::now() + Duration::from_secs(90 * 60),
    })
}
