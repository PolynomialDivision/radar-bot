//! Outbound HTTP for sources, article pages and the geocoder: the live client, or recorded
//! answers (tests, and `--dry-run --fixtures DIR`), so that nothing outside needs the network
//! to be exercised.
//!
//! Every request, live or recorded, goes through the same limits: at most
//! [`MAX_IN_FLIGHT`] at a time and [`MAX_PER_HOST`] per host, so that a poll round with many
//! new items does not hit one publisher with a burst. Bodies are read up to a cap
//! ([`MAX_BODY_BYTES`] unless the caller asks for less) and dropped beyond it.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::sync::Semaphore;

/// Requests in flight at once, over all hosts.
pub(crate) const MAX_IN_FLIGHT: usize = 8;
/// Requests in flight at once to one host.
pub(crate) const MAX_PER_HOST: usize = 2;
/// Body cap unless a caller sets its own: feeds and API answers.
pub(crate) const MAX_BODY_BYTES: usize = 5 * 1024 * 1024;
/// Body cap for article pages.
pub(crate) const MAX_PAGE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum Method {
    Get,
    Post,
}

/// What came back: the status, the content type and the body as text.
#[derive(Debug, Clone)]
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) content_type: Option<String>,
    pub(crate) body: String,
    /// The validators of the answer, for a conditional request next time.
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<String>,
}

impl Response {
    pub(crate) fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The content type for a log line.
    pub(crate) fn content_type(&self) -> &str {
        self.content_type.as_deref().unwrap_or("no content type")
    }
}

/// The request did not produce a usable response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FetchError {
    Timeout,
    /// The body was longer than the cap (bytes).
    TooLarge(usize),
    Failed(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Timeout => write!(f, "timed out"),
            FetchError::TooLarge(cap) => write!(f, "body larger than {cap} bytes"),
            FetchError::Failed(why) => write!(f, "{why}"),
        }
    }
}

/// Where requests go, with the shared limits. Cheap to clone.
#[derive(Clone)]
pub(crate) struct Http {
    transport: Transport,
    limits: Arc<Limits>,
    /// The last answer of each feed that came with validators ([`Http::get_feed`]).
    feeds: Arc<Mutex<HashMap<String, Response>>>,
}

#[derive(Clone)]
enum Transport {
    Live(reqwest::Client),
    Recorded(Arc<Recorded>),
}

struct Limits {
    total: Semaphore,
    hosts: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl Limits {
    fn new() -> Self {
        Limits {
            total: Semaphore::new(MAX_IN_FLIGHT),
            hosts: Mutex::new(HashMap::new()),
        }
    }

    fn host(&self, url: &str) -> Arc<Semaphore> {
        let host = host_of(url);
        let mut hosts = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        hosts
            .entry(host)
            .or_insert_with(|| Arc::new(Semaphore::new(MAX_PER_HOST)))
            .clone()
    }
}

fn host_of(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned))
        .unwrap_or_default()
}

impl Http {
    /// The live client.
    pub(crate) fn live(client: reqwest::Client) -> Self {
        Http {
            transport: Transport::Live(client),
            limits: Arc::new(Limits::new()),
            feeds: Arc::default(),
        }
    }

    /// Recorded answers only.
    pub(crate) fn recorded(recorded: Arc<Recorded>) -> Self {
        Http {
            transport: Transport::Recorded(recorded),
            limits: Arc::new(Limits::new()),
            feeds: Arc::default(),
        }
    }

    /// No answers at all: every request fails like an unreachable host.
    #[cfg(test)]
    pub(crate) fn offline() -> Self {
        Self::recorded(Arc::new(Recorded::new()))
    }

    /// Whether requests reach the network (and rate limits such as Nominatim's apply).
    pub(crate) fn is_live(&self) -> bool {
        matches!(self.transport, Transport::Live(_))
    }

    /// `GET url`. `timeout` bounds sending and, separately, reading the body.
    pub(crate) async fn get(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        timeout: Duration,
    ) -> Result<Response, FetchError> {
        self.send(Method::Get, url, headers, None, timeout, MAX_BODY_BYTES)
            .await
    }

    /// [`Http::get`] with its own body cap.
    pub(crate) async fn get_capped(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<Response, FetchError> {
        self.send(Method::Get, url, headers, None, timeout, max_bytes)
            .await
    }

    /// [`Http::get`] for a feed that is polled again and again: the request carries the
    /// validators (`If-None-Match`, `If-Modified-Since`) of the feed's last answer, and a
    /// `304 Not Modified` returns that answer again, so the caller sees the same items without
    /// the download. Only answers with validators are kept, in memory: after a restart the
    /// first request is unconditional. The flag says whether the answer was not modified.
    pub(crate) async fn get_feed(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<(Response, bool), FetchError> {
        let cached = self
            .feeds
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(url)
            .cloned();
        let mut headers: Vec<(&str, &str)> = Vec::new();
        if let Some(cached) = &cached {
            if let Some(etag) = &cached.etag {
                headers.push(("If-None-Match", etag));
            }
            if let Some(modified) = &cached.last_modified {
                headers.push(("If-Modified-Since", modified));
            }
        }
        let response = self.get(url, &headers, timeout).await?;
        if response.status == 304 {
            return Ok((cached.unwrap_or(response), true));
        }
        let mut feeds = self.feeds.lock().unwrap_or_else(|e| e.into_inner());
        if response.is_success() && (response.etag.is_some() || response.last_modified.is_some()) {
            feeds.insert(url.to_owned(), response.clone());
        } else {
            feeds.remove(url);
        }
        Ok((response, false))
    }

    /// `POST url` with `body`.
    pub(crate) async fn post(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: String,
        timeout: Duration,
    ) -> Result<Response, FetchError> {
        self.send(
            Method::Post,
            url,
            headers,
            Some(body),
            timeout,
            MAX_BODY_BYTES,
        )
        .await
    }

    async fn send(
        &self,
        method: Method,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<String>,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<Response, FetchError> {
        let host = self.limits.host(url);
        let _host = host.acquire().await.expect("never closed");
        let _total = self.limits.total.acquire().await.expect("never closed");
        let request = Request {
            method,
            url,
            headers,
            body,
            timeout,
            max_bytes,
        };
        match &self.transport {
            Transport::Live(client) => live(client, request).await,
            Transport::Recorded(recorded) => recorded.answer(method, url, headers, max_bytes).await,
        }
    }
}

struct Request<'a> {
    method: Method,
    url: &'a str,
    headers: &'a [(&'a str, &'a str)],
    body: Option<String>,
    timeout: Duration,
    max_bytes: usize,
}

async fn live(client: &reqwest::Client, req: Request<'_>) -> Result<Response, FetchError> {
    let mut request = match req.method {
        Method::Get => client.get(req.url),
        Method::Post => client.post(req.url),
    };
    for (name, value) in req.headers {
        request = request.header(*name, *value);
    }
    if let Some(body) = req.body {
        request = request.body(body);
    }
    let mut response = tokio::time::timeout(req.timeout, request.send())
        .await
        .map_err(|_| FetchError::Timeout)?
        .map_err(|e| FetchError::Failed(e.to_string()))?;
    let status = response.status().as_u16();
    let header = |name: reqwest::header::HeaderName| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let content_type = header(reqwest::header::CONTENT_TYPE);
    let etag = header(reqwest::header::ETAG);
    let last_modified = header(reqwest::header::LAST_MODIFIED);
    let max_bytes = req.max_bytes;
    if response
        .content_length()
        .is_some_and(|n| n > max_bytes as u64)
    {
        return Err(FetchError::TooLarge(max_bytes));
    }
    let bytes = tokio::time::timeout(req.timeout, async {
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| FetchError::Failed(e.to_string()))?
        {
            if bytes.len() + chunk.len() > max_bytes {
                return Err(FetchError::TooLarge(max_bytes));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    })
    .await
    .map_err(|_| FetchError::Timeout)??;
    Ok(Response {
        body: decode(&bytes, content_type.as_deref()),
        status,
        content_type,
        etag,
        last_modified,
    })
}

/// The body as text: the `charset` of the content type, else UTF-8, with a BOM taking
/// precedence and malformed sequences replaced (as reqwest's `text_with_charset("utf-8")`).
fn decode(bytes: &[u8], content_type: Option<&str>) -> String {
    let charset = content_type
        .and_then(|ct| {
            ct.split(';')
                .skip(1)
                .filter_map(|p| p.split_once('='))
                .find(|(k, _)| k.trim().eq_ignore_ascii_case("charset"))
                .map(|(_, v)| v.trim().trim_matches('"').to_owned())
        })
        .unwrap_or_else(|| "utf-8".to_owned());
    let encoding =
        encoding_rs::Encoding::for_label(charset.as_bytes()).unwrap_or(encoding_rs::UTF_8);
    encoding.decode(bytes).0.into_owned()
}

/// Recorded answers by URL prefix, the first matching route wins. A URL without a route fails
/// like an unreachable host. Every request is recorded.
#[derive(Default)]
pub(crate) struct Recorded {
    routes: Vec<Route>,
    calls: Mutex<Vec<String>>,
    delay: Duration,
    in_flight: Mutex<HashMap<String, usize>>,
    most_in_flight: AtomicUsize,
}

#[derive(Debug)]
struct Route {
    method: Option<Method>,
    prefix: String,
    status: u16,
    content_type: Option<String>,
    body: String,
    /// The first this many requests fail like an unreachable host.
    fail_first: Mutex<usize>,
    /// Sent as the answer's `ETag`; a request with `If-None-Match` of it gets a 304.
    etag: Option<String>,
}

impl Recorded {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Answer requests whose URL starts with `prefix` (any method) with `status` and `body`.
    #[cfg(test)]
    pub(crate) fn route(self, prefix: &str, status: u16, body: &str) -> Self {
        self.route_failing_first(prefix, 0, status, body)
    }

    /// As [`Recorded::route`], but the first `failures` requests fail like an unreachable host.
    #[cfg(test)]
    pub(crate) fn route_failing_first(
        mut self,
        prefix: &str,
        failures: usize,
        status: u16,
        body: &str,
    ) -> Self {
        self.routes.push(Route {
            method: None,
            prefix: prefix.to_owned(),
            status,
            content_type: None,
            body: body.to_owned(),
            fail_first: Mutex::new(failures),
            etag: None,
        });
        self
    }

    /// The last route answers with `etag`, and with 304 to a request that names it.
    #[cfg(test)]
    pub(crate) fn with_etag(mut self, etag: &str) -> Self {
        if let Some(route) = self.routes.last_mut() {
            route.etag = Some(etag.to_owned());
        }
        self
    }

    /// Every answer takes `delay`, so that tests see requests overlap.
    #[cfg(test)]
    pub(crate) fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// The routes of `DIR/routes.toml`, bodies read from files next to it:
    ///
    /// ```toml
    /// [[route]]
    /// url = "https://example.org/feed.xml"   # prefix
    /// file = "feeds/rss_full_text.xml"
    /// # status = 200, content_type = "…", method = "POST", body = "…" instead of file
    /// ```
    pub(crate) fn from_dir(dir: &Path) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct File {
            route: Vec<RouteFile>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RouteFile {
            url: String,
            file: Option<PathBuf>,
            body: Option<String>,
            #[serde(default = "ok")]
            status: u16,
            content_type: Option<String>,
            method: Option<Method>,
        }
        fn ok() -> u16 {
            200
        }

        let index = dir.join("routes.toml");
        let raw = std::fs::read_to_string(&index)
            .with_context(|| format!("Reading {}", index.display()))?;
        let file: File =
            toml::from_str(&raw).with_context(|| format!("Parsing {}", index.display()))?;
        let mut recorded = Self::new();
        for route in file.route {
            let body = match (&route.file, route.body) {
                (Some(path), None) => std::fs::read_to_string(dir.join(path))
                    .with_context(|| format!("Reading fixture {}", path.display()))?,
                (None, Some(body)) => body,
                (None, None) => String::new(),
                (Some(_), Some(_)) => anyhow::bail!("route {}: file and body both set", route.url),
            };
            recorded.routes.push(Route {
                method: route.method,
                prefix: route.url,
                status: route.status,
                content_type: route.content_type,
                body,
                fail_first: Mutex::new(0),
                etag: None,
            });
        }
        Ok(recorded)
    }

    /// The requests made so far, as `GET url`.
    #[cfg(test)]
    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The most requests that were in flight to one host at the same time.
    #[cfg(test)]
    pub(crate) fn most_in_flight(&self) -> usize {
        self.most_in_flight.load(Ordering::SeqCst)
    }

    async fn answer(
        &self,
        method: Method,
        url: &str,
        headers: &[(&str, &str)],
        max_bytes: usize,
    ) -> Result<Response, FetchError> {
        let verb = match method {
            Method::Get => "GET",
            Method::Post => "POST",
        };
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(format!("{verb} {url}"));
        let host = host_of(url);
        {
            let mut in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            let now = in_flight.entry(host.clone()).or_default();
            *now += 1;
            self.most_in_flight.fetch_max(*now, Ordering::SeqCst);
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        *self
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(host)
            .or_default() -= 1;

        let route = self
            .routes
            .iter()
            .find(|r| r.method.is_none_or(|m| m == method) && url.starts_with(&r.prefix))
            .ok_or_else(|| FetchError::Failed(format!("no recorded answer for {url}")))?;
        let failing = {
            let mut left = route.fail_first.lock().unwrap_or_else(|e| e.into_inner());
            let failing = *left > 0;
            *left = left.saturating_sub(1);
            failing
        };
        if failing {
            return Err(FetchError::Failed(format!("{url}: recorded failure")));
        }
        let not_modified = route.etag.as_deref().is_some_and(|etag| {
            headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("if-none-match") && *value == etag)
        });
        if not_modified {
            return Ok(Response {
                status: 304,
                content_type: None,
                body: String::new(),
                etag: route.etag.clone(),
                last_modified: None,
            });
        }
        if route.body.len() > max_bytes {
            return Err(FetchError::TooLarge(max_bytes));
        }
        Ok(Response {
            status: route.status,
            content_type: route.content_type.clone(),
            body: route.body.clone(),
            etag: route.etag.clone(),
            last_modified: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recorded_answers_by_prefix_and_records_calls() {
        let recorded = Arc::new(
            Recorded::new()
                .route("https://a.test/feed", 200, "<rss/>")
                .route("https://a.test/", 404, ""),
        );
        let http = Http::recorded(recorded.clone());

        let feed = http
            .get("https://a.test/feed.xml", &[], Duration::from_secs(1))
            .await
            .unwrap();
        let other = http
            .get("https://a.test/x", &[], Duration::from_secs(1))
            .await
            .unwrap();
        let none = http
            .get("https://b.test/", &[], Duration::from_secs(1))
            .await;

        assert_eq!((feed.status, feed.body.as_str()), (200, "<rss/>"));
        assert_eq!(other.status, 404);
        assert!(none.is_err());
        assert_eq!(
            recorded.calls(),
            vec![
                "GET https://a.test/feed.xml",
                "GET https://a.test/x",
                "GET https://b.test/"
            ]
        );
    }

    #[tokio::test]
    async fn one_host_never_gets_more_than_two_requests_at_once() {
        let recorded = Arc::new(
            Recorded::new()
                .route("https://busy.test/", 200, "<html></html>")
                .with_delay(Duration::from_millis(20)),
        );
        let http = Http::recorded(recorded.clone());

        let requests = (0..10).map(|i| {
            let http = http.clone();
            tokio::spawn(async move {
                http.get(
                    &format!("https://busy.test/{i}"),
                    &[],
                    Duration::from_secs(5),
                )
                .await
            })
        });
        for request in futures_util::future::join_all(requests).await {
            assert!(request.unwrap().is_ok());
        }

        assert_eq!(recorded.calls().len(), 10);
        assert_eq!(recorded.most_in_flight(), MAX_PER_HOST);
    }

    #[tokio::test]
    async fn a_body_over_the_cap_is_a_failed_fetch() {
        let page = "x".repeat(3 * 1024 * 1024);
        let http = Http::recorded(Arc::new(Recorded::new().route(
            "https://big.test/",
            200,
            &page,
        )));

        let result = http
            .get_capped(
                "https://big.test/a",
                &[],
                Duration::from_secs(1),
                MAX_PAGE_BYTES,
            )
            .await;

        assert_eq!(result.unwrap_err(), FetchError::TooLarge(MAX_PAGE_BYTES));
    }

    #[test]
    fn bodies_are_decoded_by_their_charset() {
        assert_eq!(decode("Straße".as_bytes(), None), "Straße");
        assert_eq!(
            decode(b"Stra\xdfe", Some("text/xml; charset=ISO-8859-1")),
            "Straße"
        );
        assert_eq!(decode("\u{feff}ok".as_bytes(), Some("text/html")), "ok");
    }

    #[tokio::test]
    async fn an_unchanged_feed_is_answered_from_its_last_body() {
        let recorded = Arc::new(
            Recorded::new()
                .route("https://feed.test/rss", 200, "<rss>items</rss>")
                .with_etag("\"v1\""),
        );
        let http = Http::recorded(recorded.clone());
        let timeout = Duration::from_secs(1);

        let (first, first_cached) = http
            .get_feed("https://feed.test/rss", timeout)
            .await
            .unwrap();
        let (second, second_cached) = http
            .get_feed("https://feed.test/rss", timeout)
            .await
            .unwrap();

        assert!(!first_cached);
        assert!(
            second_cached,
            "the second request was conditional and got a 304"
        );
        assert_eq!(second.body, first.body);
        assert_eq!(recorded.calls().len(), 2);
    }
}
