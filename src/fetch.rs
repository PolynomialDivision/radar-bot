//! Outbound HTTP for sources, article pages and the geocoder: the live client, or recorded
//! answers (tests, and `--dry-run --fixtures DIR`), so that nothing outside needs the network
//! to be exercised.
//!
//! Every request, live or recorded, goes through the same limits: at most
//! [`MAX_IN_FLIGHT`] at a time and [`MAX_PER_HOST`] per host, so that a poll round with many
//! new items does not hit one publisher with a burst. Redirects are followed here, one hop at a
//! time, so that the limits of the host a redirect leads to apply as well. Bodies are read up
//! to a cap ([`MAX_BODY_BYTES`] unless the caller asks for less) and dropped beyond it.
//!
//! Two User-Agents: services with an API (geocoder, warnings, forecast, structured sources) get
//! the bot's own name ([`Agents::api`]), feeds and article pages the page agent.

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
/// Redirects followed for one request.
const MAX_REDIRECTS: usize = 10;

/// The User-Agents of the two kinds of request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Agents {
    /// For APIs: identifies the bot (`radar-bot/<version>`), as Nominatim and Overpass ask.
    pub(crate) api: String,
    /// For feeds and article pages, which some publishers refuse to unknown bots.
    pub(crate) page: String,
}

impl Default for Agents {
    fn default() -> Self {
        Agents {
            api: default_api_agent(),
            page: default_page_agent(),
        }
    }
}

pub(crate) fn default_api_agent() -> String {
    format!("radar-bot/{}", env!("CARGO_PKG_VERSION"))
}

pub(crate) fn default_page_agent() -> String {
    "Mozilla/5.0 (compatible; radar-bot/0.1)".to_owned()
}

/// Which agent a request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Agent {
    Api,
    Page,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum Method {
    Get,
    Post,
}

/// What came back: the status, the content type and the body, as text and as it came.
#[derive(Debug, Clone)]
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) content_type: Option<String>,
    pub(crate) body: String,
    /// The body undecoded: feeds name their encoding inside.
    pub(crate) bytes: Vec<u8>,
    /// The validators of the answer, for a conditional request next time.
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<String>,
    /// Where a redirect points.
    location: Option<String>,
    /// The URL that gave this answer, after redirects (set by [`Http`]).
    pub(crate) final_url: Option<String>,
}

impl Response {
    pub(crate) fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// An answer as the live transport would build it.
    #[cfg(test)]
    pub(crate) fn for_test(status: u16, content_type: Option<&str>, bytes: Vec<u8>) -> Self {
        Response {
            status,
            body: decode(&bytes, content_type),
            content_type: content_type.map(str::to_owned),
            bytes,
            etag: None,
            last_modified: None,
            location: None,
            final_url: None,
        }
    }

    /// The content type for a log line.
    pub(crate) fn content_type(&self) -> &str {
        self.content_type.as_deref().unwrap_or("no content type")
    }

    /// The target of a redirect, resolved against `url`.
    fn redirect_target(&self, url: &str) -> Option<String> {
        if !matches!(self.status, 301 | 302 | 303 | 307 | 308) {
            return None;
        }
        let location = self.location.as_deref()?;
        reqwest::Url::parse(url)
            .ok()?
            .join(location)
            .ok()
            .map(String::from)
    }
}

/// The request did not produce a usable response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FetchError {
    Timeout,
    /// The body was longer than the cap (bytes).
    TooLarge(usize),
    /// The URL is not one the bot fetches: no http(s), or a host inside the house's networks.
    Refused(String),
    Failed(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Timeout => write!(f, "timed out"),
            FetchError::TooLarge(cap) => write!(f, "body larger than {cap} bytes"),
            FetchError::Refused(why) => write!(f, "refused: {why}"),
            FetchError::Failed(why) => write!(f, "{why}"),
        }
    }
}

/// Where requests go, with the shared limits. Cheap to clone.
#[derive(Clone)]
pub(crate) struct Http {
    transport: Transport,
    /// Which targets the live transport refuses; none for recorded answers.
    guard: Option<Arc<Guard>>,
    agents: Arc<Agents>,
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
    /// The live client (built without redirects: [`Http`] follows them itself).
    pub(crate) fn live(client: reqwest::Client, agents: Agents) -> Self {
        Http {
            transport: Transport::Live(client),
            guard: Some(Arc::default()),
            agents: Arc::new(agents),
            limits: Arc::new(Limits::new()),
            feeds: Arc::default(),
        }
    }

    /// Recorded answers only.
    pub(crate) fn recorded(recorded: Arc<Recorded>) -> Self {
        Http {
            transport: Transport::Recorded(recorded),
            guard: None,
            agents: Arc::default(),
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
        self.send(
            Agent::Page,
            Method::Get,
            url,
            headers,
            None,
            Bounds::new(timeout, MAX_BODY_BYTES),
        )
        .await
    }

    /// [`Http::get`] for an API: with the bot's own User-Agent.
    pub(crate) async fn get_api(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        timeout: Duration,
    ) -> Result<Response, FetchError> {
        self.send(
            Agent::Api,
            Method::Get,
            url,
            headers,
            None,
            Bounds::new(timeout, MAX_BODY_BYTES),
        )
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
        self.send(
            Agent::Page,
            Method::Get,
            url,
            headers,
            None,
            Bounds::new(timeout, max_bytes),
        )
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
        self.conditional(Agent::Page, url, timeout).await
    }

    /// [`Http::get_feed`] for an API file (the DWD warnings): with the bot's own User-Agent.
    pub(crate) async fn get_api_feed(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<(Response, bool), FetchError> {
        self.conditional(Agent::Api, url, timeout).await
    }

    async fn conditional(
        &self,
        agent: Agent,
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
        let response = self
            .send(
                agent,
                Method::Get,
                url,
                &headers,
                None,
                Bounds::new(timeout, MAX_BODY_BYTES),
            )
            .await?;
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
            Agent::Page,
            Method::Post,
            url,
            headers,
            Some(body),
            Bounds::new(timeout, MAX_BODY_BYTES),
        )
        .await
    }

    async fn send(
        &self,
        agent: Agent,
        method: Method,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<String>,
        Bounds { timeout, max_bytes }: Bounds,
    ) -> Result<Response, FetchError> {
        let agent = match agent {
            Agent::Api => self.agents.api.as_str(),
            Agent::Page => self.agents.page.as_str(),
        };
        let mut url = url.to_owned();
        let mut method = method;
        let mut body = body;
        for _ in 0..=MAX_REDIRECTS {
            if let Some(guard) = &self.guard {
                guard.check(&url).await?;
            }
            let response = {
                let host = self.limits.host(&url);
                let _host = host.acquire().await.expect("never closed");
                let _total = self.limits.total.acquire().await.expect("never closed");
                let request = Request {
                    method,
                    url: &url,
                    agent,
                    headers,
                    body: body.clone(),
                    timeout,
                    max_bytes,
                };
                match &self.transport {
                    Transport::Live(client) => live(client, request).await?,
                    Transport::Recorded(recorded) => {
                        recorded.answer(method, &url, headers, max_bytes).await?
                    }
                }
            };
            let Some(target) = response.redirect_target(&url) else {
                return Ok(Response {
                    final_url: Some(url),
                    ..response
                });
            };
            if response.status == 303 {
                method = Method::Get;
                body = None;
            }
            url = target;
        }
        Err(FetchError::Failed(format!(
            "more than {MAX_REDIRECTS} redirects"
        )))
    }
}

/// The targets the live transport refuses: URLs that are not http(s), and hosts that resolve
/// to a loopback, private, link-local or unique-local address (the bot runs inside the
/// house's container network and follows whatever links feeds carry). Checked on every
/// redirect hop.
#[derive(Default)]
struct Guard {
    /// Addresses let through anyway (tests against a local server).
    allowed: Vec<std::net::IpAddr>,
}

impl Guard {
    async fn check(&self, url: &str) -> Result<(), FetchError> {
        let parsed =
            reqwest::Url::parse(url).map_err(|e| FetchError::Refused(format!("{url}: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(FetchError::Refused(format!(
                "{} is no web address",
                parsed.scheme()
            )));
        }
        let port = parsed.port_or_known_default().unwrap_or(80);
        let host = parsed
            .host_str()
            .ok_or_else(|| FetchError::Refused(format!("{url} has no host")))?;
        // An IPv6 literal comes in brackets.
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let addresses: Vec<std::net::IpAddr> = match bare.parse() {
            Ok(ip) => vec![ip],
            Err(_) => tokio::net::lookup_host((bare, port))
                .await
                .map_err(|e| FetchError::Failed(format!("{bare}: {e}")))?
                .map(|address| address.ip())
                .collect(),
        };
        match addresses
            .iter()
            .find(|ip| is_internal(**ip) && !self.allowed.contains(ip))
        {
            Some(ip) => Err(FetchError::Refused(format!(
                "{} is an internal address ({ip})",
                parsed.host_str().unwrap_or_default()
            ))),
            None => Ok(()),
        }
    }
}

/// Loopback, private, link-local, unique-local, unspecified or shared address space.
fn is_internal(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                // 100.64.0.0/10, carrier-grade NAT and some container networks
                || (a == 100 && (64..128).contains(&b))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_internal(IpAddr::V4(v4));
            }
            let first = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || (first & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (first & 0xffc0) == 0xfe80 // link local fe80::/10
        }
    }
}

/// How long a request may take (sending and, separately, reading) and how large its body.
#[derive(Clone, Copy)]
struct Bounds {
    timeout: Duration,
    max_bytes: usize,
}

impl Bounds {
    fn new(timeout: Duration, max_bytes: usize) -> Self {
        Bounds { timeout, max_bytes }
    }
}

struct Request<'a> {
    method: Method,
    url: &'a str,
    agent: &'a str,
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
    request = request.header(reqwest::header::USER_AGENT, req.agent);
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
    let location = header(reqwest::header::LOCATION);
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
        bytes,
        status,
        content_type,
        etag,
        last_modified,
        location,
        final_url: None,
    })
}

/// The body as text: the `charset` of the content type, else UTF-8, with a BOM taking
/// precedence and malformed sequences replaced (as reqwest's `text_with_charset("utf-8")`).
pub(crate) fn decode(bytes: &[u8], content_type: Option<&str>) -> String {
    let encoding = content_type
        .and_then(charset_of)
        .and_then(|charset| encoding_rs::Encoding::for_label(charset.as_bytes()))
        .unwrap_or(encoding_rs::UTF_8);
    encoding.decode(bytes).0.into_owned()
}

/// The `charset` parameter of a content type.
pub(crate) fn charset_of(content_type: &str) -> Option<String> {
    content_type
        .split(';')
        .skip(1)
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("charset"))
        .map(|(_, v)| v.trim().trim_matches('"').to_owned())
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
    /// Sent as the answer's `Location`.
    location: Option<String>,
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
            location: None,
        });
        self
    }

    /// The last route redirects to `location` (give it a 3xx status).
    #[cfg(test)]
    pub(crate) fn with_location(mut self, location: &str) -> Self {
        if let Some(route) = self.routes.last_mut() {
            route.location = Some(location.to_owned());
        }
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
                location: None,
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
                bytes: Vec::new(),
                etag: route.etag.clone(),
                last_modified: None,
                location: None,
                final_url: None,
            });
        }
        if route.body.len() > max_bytes {
            return Err(FetchError::TooLarge(max_bytes));
        }
        Ok(Response {
            status: route.status,
            content_type: route.content_type.clone(),
            body: route.body.clone(),
            bytes: route.body.clone().into_bytes(),
            etag: route.etag.clone(),
            last_modified: None,
            location: route.location.clone(),
            final_url: None,
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

/// `Http::live` against local servers: the one place where tests open a socket.
#[cfg(test)]
mod live_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The live transport, with the local test servers let through the guard.
    fn live() -> Http {
        let mut http = guarded();
        http.guard = Some(Arc::new(Guard {
            allowed: vec!["127.0.0.1".parse().unwrap(), "127.0.0.2".parse().unwrap()],
        }));
        http
    }

    /// The live transport as the bot uses it.
    fn guarded() -> Http {
        Http::live(
            crate::http_client().unwrap(),
            Agents {
                api: "radar-bot/test".to_owned(),
                page: "Page-Agent/1".to_owned(),
            },
        )
    }

    #[tokio::test]
    async fn internal_addresses_and_other_schemes_are_refused() {
        let http = guarded();
        for url in [
            "http://127.0.0.1/",
            "http://[::1]/",
            "http://10.0.0.1/feed",
            "http://192.168.1.10/",
            "http://169.254.169.254/latest/meta-data/",
            "http://[fd00::1]/",
            "file:///etc/passwd",
            "ftp://example.com/feed",
        ] {
            let result = http.get(url, &[], SECOND).await;
            assert!(
                matches!(result, Err(FetchError::Refused(_))),
                "{url}: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_redirect_into_the_house_network_is_refused() {
        let server = MockServer::start().await;
        Mock::given(path("/moved"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("Location", "http://10.0.0.1/admin"),
            )
            .mount(&server)
            .await;

        let result = live()
            .get(&format!("{}/moved", server.uri()), &[], SECOND * 5)
            .await;

        assert!(
            matches!(result, Err(FetchError::Refused(ref why)) if why.contains("10.0.0.1")),
            "{result:?}"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    const SECOND: Duration = Duration::from_secs(1);

    /// A server that answers one request with `chunks` of `chunk` bytes each, chunked and
    /// without `Content-Length`.
    async fn chunked_server(chunks: usize, chunk: usize) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let _ = socket.read(&mut request).await;
            let head =
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nTransfer-Encoding: chunked\r\n\r\n";
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            let body = vec![b'x'; chunk];
            for _ in 0..chunks {
                let size = format!("{chunk:x}\r\n");
                if socket.write_all(size.as_bytes()).await.is_err()
                    || socket.write_all(&body).await.is_err()
                    || socket.write_all(b"\r\n").await.is_err()
                {
                    return;
                }
            }
            let _ = socket.write_all(b"0\r\n\r\n").await;
        });
        format!("http://{address}/page")
    }

    #[tokio::test]
    async fn a_chunked_body_over_the_cap_is_refused_while_reading() {
        let url = chunked_server(40, 64 * 1024).await;

        let result = live().get_capped(&url, &[], SECOND * 5, 1024 * 1024).await;

        assert_eq!(result.unwrap_err(), FetchError::TooLarge(1024 * 1024));
    }

    #[tokio::test]
    async fn a_declared_length_over_the_cap_is_refused_before_reading() {
        let server = MockServer::start().await;
        Mock::given(path("/big"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 2 * 1024 * 1024]))
            .mount(&server)
            .await;

        let result = live()
            .get_capped(
                &format!("{}/big", server.uri()),
                &[],
                SECOND * 5,
                1024 * 1024,
            )
            .await;

        assert_eq!(result.unwrap_err(), FetchError::TooLarge(1024 * 1024));
    }

    #[tokio::test]
    async fn a_latin_1_body_is_decoded_by_its_charset_and_kept_as_it_came() {
        let server = MockServer::start().await;
        Mock::given(path("/feed"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/xml; charset=ISO-8859-1")
                    .set_body_bytes(b"<t>Stra\xdfe</t>".to_vec()),
            )
            .mount(&server)
            .await;

        let response = live()
            .get(&format!("{}/feed", server.uri()), &[], SECOND * 5)
            .await
            .unwrap();

        assert_eq!(response.body, "<t>Straße</t>");
        assert_eq!(response.bytes, b"<t>Stra\xdfe</t>");
    }

    #[tokio::test]
    async fn an_unchanged_feed_is_asked_for_with_its_etag_and_answered_304() {
        let server = MockServer::start().await;
        Mock::given(path("/rss"))
            .and(header("If-None-Match", "\"v1\""))
            .respond_with(ResponseTemplate::new(304))
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(path("/rss"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("ETag", "\"v1\"")
                    .set_body_string("<rss>items</rss>"),
            )
            .with_priority(2)
            .mount(&server)
            .await;
        let http = live();
        let url = format!("{}/rss", server.uri());

        let (first, first_unchanged) = http.get_feed(&url, SECOND * 5).await.unwrap();
        let (second, second_unchanged) = http.get_feed(&url, SECOND * 5).await.unwrap();

        assert!(!first_unchanged);
        assert!(second_unchanged, "the second answer was a 304");
        assert_eq!(second.body, "<rss>items</rss>");
        assert_eq!(first.body, second.body);
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn apis_and_pages_get_their_own_user_agent() {
        let server = MockServer::start().await;
        Mock::given(path("/api"))
            .and(header("User-Agent", "radar-bot/test"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(path("/page"))
            .and(header("User-Agent", "Page-Agent/1"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let http = live();

        let api = http
            .get_api(&format!("{}/api", server.uri()), &[], SECOND * 5)
            .await
            .unwrap();
        let page = http
            .get(&format!("{}/page", server.uri()), &[], SECOND * 5)
            .await
            .unwrap();

        assert_eq!((api.status, page.status), (200, 200));
    }

    #[tokio::test]
    async fn a_redirect_is_followed_under_the_limit_of_the_host_it_leads_to() {
        // Two loopback addresses are two hosts.
        let target = MockServer::builder()
            .listener(std::net::TcpListener::bind("127.0.0.2:0").unwrap())
            .start()
            .await;
        Mock::given(method("GET"))
            .and(path("/slow"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("arrived")
                    .set_delay(Duration::from_millis(400)),
            )
            .mount(&target)
            .await;
        let origin = MockServer::start().await;
        Mock::given(path("/moved"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", format!("{}/slow", target.uri())),
            )
            .mount(&origin)
            .await;
        let http = live();
        let slow = format!("{}/slow", target.uri());

        // Both permits of the target host are taken by direct requests.
        let direct: Vec<_> = (0..MAX_PER_HOST)
            .map(|_| {
                let (http, slow) = (http.clone(), slow.clone());
                tokio::spawn(async move { http.get(&slow, &[], SECOND * 5).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = std::time::Instant::now();
        let redirected = http
            .get(&format!("{}/moved", origin.uri()), &[], SECOND * 5)
            .await
            .unwrap();
        let waited = started.elapsed();

        assert_eq!(
            (redirected.status, redirected.body.as_str()),
            (200, "arrived")
        );
        assert!(
            waited >= Duration::from_millis(550),
            "the redirected request waited for a permit of the target host: {waited:?}"
        );
        for request in direct {
            assert_eq!(request.await.unwrap().unwrap().status, 200);
        }
    }
}
