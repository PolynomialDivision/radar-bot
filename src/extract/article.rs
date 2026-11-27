//! Fetching article pages, reading their text and metadata, resolving aggregator links.
//!
//! A page is parsed once (`scraper`, an HTML5 parser) and every reading works on that document.
//!
//! A page's nesting depth and size are chosen by its author, so nothing here recurses over the
//! document and every pass has a budget: the parser is fed in chunks against a deadline (its
//! own cost grows with the square of the nesting depth of some elements), the text walk stops
//! at [`MAX_NODES`] nodes or [`MAX_DEPTH`] levels. A page beyond a budget is not an article.

use std::time::{Duration, Instant};

use html5ever::tendril::{StrTendril, TendrilSink};
use scraper::{node::Node, ElementRef, Html, HtmlTreeSink, Selector};

/// A node of a parsed document (ego_tree's, which scraper does not re-export).
type NodeRef<'a> = <ElementRef<'a> as std::ops::Deref>::Target;

use crate::feed::parse_feed_date;
use crate::fetch::{FetchError, Http, MAX_PAGE_BYTES};
use crate::filter::{is_invisible, normalize};

// ── Article text ──────────────────────────────────────────────────────────────

/// Elements whose text is page furniture, never the article.
const NOT_TEXT: &[&str] = &[
    "script", "style", "nav", "header", "footer", "aside", "noscript", "template",
];

/// [`NOT_TEXT`] inside the chosen container: an article's own `header` holds its headline and
/// lead, its `footer` the byline; both are part of it.
const NOT_TEXT_INSIDE: &[&str] = &["script", "style", "nav", "aside", "noscript", "template"];

/// Candidate containers of the article, in the order they are considered.
const CONTAINERS: &[&str] = &["article", "main", "section", "div"];

/// Elements after which the text continues as a new word: `<h1>Brand</h1><p>Die` is
/// "Brand Die", not "BrandDie".
const BLOCKS: &[&str] = &[
    "p",
    "div",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "br",
    "tr",
    "td",
    "th",
    "ul",
    "ol",
    "dt",
    "dd",
    "table",
    "section",
    "article",
    "main",
    "header",
    "footer",
    "nav",
    "aside",
    "blockquote",
    "figure",
    "figcaption",
    "hr",
    "pre",
];

/// Class or id tokens of page parts around an article: teaser lists, related stories,
/// comments, breadcrumbs, newsletter boxes. Their text is never part of the body.
const NOT_BODY_TOKENS: &[&str] = &[
    "teaser",
    "related",
    "comment",
    "comments",
    "breadcrumb",
    "newsletter",
    "recommendation",
    "recommendations",
    "more",
];

/// A container loses to a descendant container that holds at least this share of its text
/// (percent): the innermost one that still has the article wins over the page wrapper.
const INNER_SHARE_PERCENT: usize = 60;

/// Nodes the text walk visits at most.
pub(crate) const MAX_NODES: usize = 200_000;
/// Nesting depth the text walk follows at most.
pub(crate) const MAX_DEPTH: usize = 512;
/// Time the parser may take for one page. A 2 MB page of ordinary shape takes about 40 ms.
const PARSE_BUDGET: Duration = Duration::from_secs(1);
/// Bytes handed to the parser between two looks at the clock.
const PARSE_CHUNK: usize = 1024;
/// Time reading one page may take in all, on its blocking thread.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The page is beyond a work budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TooComplex;

fn selector(css: &str) -> Selector {
    Selector::parse(css).expect("a valid selector")
}

/// `html` as a document, or [`TooComplex`] when the parser runs past [`PARSE_BUDGET`].
fn parse_page(html: &str) -> Result<Html, TooComplex> {
    let started = Instant::now();
    let mut parser = html5ever::driver::parse_document(
        HtmlTreeSink::new(Html::new_document()),
        Default::default(),
    );
    let mut rest = html;
    while !rest.is_empty() {
        let mut cut = PARSE_CHUNK.min(rest.len());
        while !rest.is_char_boundary(cut) {
            cut += 1;
        }
        let (chunk, tail) = rest.split_at(cut);
        parser.process(StrTendril::from_slice(chunk));
        if started.elapsed() > PARSE_BUDGET {
            return Err(TooComplex);
        }
        rest = tail;
    }
    Ok(parser.finish())
}

/// Whether the class or id of `element` names a page part that is not the article body.
fn is_not_body(element: &scraper::node::Element) -> bool {
    if matches!(element.name(), "html" | "body") {
        return false;
    }
    ["class", "id"]
        .iter()
        .filter_map(|attr| element.attr(attr))
        .flat_map(|value| value.split(|c: char| !c.is_alphanumeric()))
        .any(|token| {
            NOT_BODY_TOKENS
                .iter()
                .any(|not_body| token.eq_ignore_ascii_case(not_body))
        })
}

/// A candidate container: where its text lies in [`PageText::text`].
struct Container<'a> {
    element: ElementRef<'a>,
    /// Byte range of its text.
    start: usize,
    end: usize,
    /// Words of the page before it and up to its end.
    words_before: usize,
    words_end: usize,
    /// Candidates after this one up to here are its descendants.
    descendants_until: usize,
}

/// The visible text of a whole document in one pass: whitespace collapsed, page furniture
/// ([`NOT_TEXT`], [`NOT_BODY_TOKENS`]) and invisible characters left out, and for every
/// candidate container the range of the text that is its own.
struct PageText<'a> {
    text: String,
    words: usize,
    at_space: bool,
    containers: Vec<Container<'a>>,
}

impl<'a> PageText<'a> {
    fn push_text(&mut self, text: &str) {
        for c in text.chars().filter(|c| !is_invisible(*c)) {
            if c.is_whitespace() {
                self.push_space();
            } else {
                if self.at_space {
                    self.words += 1;
                }
                self.text.push(c);
                self.at_space = false;
            }
        }
    }

    fn push_space(&mut self) {
        if !self.at_space {
            self.text.push(' ');
            self.at_space = true;
        }
    }

    fn of(doc: &'a Html) -> Result<Self, TooComplex> {
        Self::below(doc.tree.root(), NOT_TEXT)
    }

    /// The text below `root`, leaving out the elements of `skip`.
    fn below(root: NodeRef<'a>, skip: &[&str]) -> Result<Self, TooComplex> {
        let mut page = PageText {
            text: String::new(),
            words: 0,
            at_space: true,
            containers: Vec::new(),
        };
        // Containers that are open at the current node, innermost last.
        let mut open: Vec<usize> = Vec::new();
        let mut node = root;
        let mut depth = 0;
        let mut visited = 0;
        loop {
            visited += 1;
            if visited > MAX_NODES || depth > MAX_DEPTH {
                return Err(TooComplex);
            }
            let mut descend = true;
            match node.value() {
                Node::Text(text) => page.push_text(text),
                Node::Element(element) => {
                    let name = element.name();
                    if skip.contains(&name) || is_not_body(element) {
                        descend = false;
                    } else if CONTAINERS.contains(&name) {
                        open.push(page.containers.len());
                        page.containers.push(Container {
                            element: ElementRef::wrap(node).expect("an element"),
                            start: page.text.len(),
                            end: page.text.len(),
                            words_before: page.words,
                            words_end: page.words,
                            descendants_until: 0,
                        });
                    }
                }
                _ => {}
            }
            if descend {
                if let Some(child) = node.first_child() {
                    node = child;
                    depth += 1;
                    continue;
                }
            }
            // Leave `node`, and each ancestor whose last child it was.
            loop {
                if let Node::Element(element) = node.value() {
                    if let Some(&index) = open.last() {
                        if page.containers[index].element.id() == node.id() {
                            open.pop();
                            let until = page.containers.len();
                            let container = &mut page.containers[index];
                            container.end = page.text.len();
                            container.words_end = page.words;
                            container.descendants_until = until;
                        }
                    }
                    if BLOCKS.contains(&element.name()) {
                        page.push_space();
                    }
                }
                if node == root {
                    return Ok(page);
                }
                if let Some(next) = node.next_sibling() {
                    node = next;
                    break;
                }
                node = node.parent().expect("below the root");
                depth -= 1;
            }
        }
    }

    fn text_of(&self, container: &Container<'_>) -> &str {
        self.text[container.start..container.end].trim()
    }

    fn words_of(&self, container: &Container<'_>) -> usize {
        // A container whose text continues a word of the text before it ("a<b>bc</b>") has
        // that word counted before it.
        let continues = container.start > 0
            && !self.text[..container.start].ends_with(' ')
            && self.text[container.start..container.end]
                .chars()
                .next()
                .is_some_and(|c| c != ' ');
        container.words_end - container.words_before + usize::from(continues)
    }
}

/// Score of a container from its tag name, its class and id tokens and its number of words.
/// A token counts for a needle when it is the needle or, for the article words, begins with it
/// ("articlebody"); so "lead" or "download" do not count as "ad", nor "context" as "text".
fn container_score(element: ElementRef<'_>, words: usize) -> i32 {
    let tokens = element_tokens(element);
    let has = |needle: &str, prefix: bool| {
        tokens
            .iter()
            .any(|t| t == needle || (prefix && t.starts_with(needle)))
    };
    let mut score = 0;
    for needle in [
        "article",
        "articlebody",
        "story",
        "content",
        "main",
        "post",
        "entry",
        "body",
        "meldung",
        "text",
    ] {
        if has(needle, true) {
            score += 4;
        }
    }
    for needle in [
        "nav", "menu", "footer", "header", "aside", "comment", "related", "teaser", "ad", "advert",
        "social",
    ] {
        // Longer page-part words also as the start of a token ("comments", "teaserlist").
        if has(needle, needle.len() >= 5) {
            score -= 5;
        }
    }
    score + (words / 80).min(8) as i32
}

/// The tag name and the class and id tokens of `element`, lower-cased.
fn element_tokens(element: ElementRef<'_>) -> Vec<String> {
    let value = element.value();
    std::iter::once(value.name().to_owned())
        .chain(
            ["class", "id"]
                .iter()
                .filter_map(|attr| value.attr(attr))
                .flat_map(|v| v.split(|c: char| !c.is_alphanumeric()))
                .filter(|t| !t.is_empty())
                .map(str::to_lowercase),
        )
        .collect()
}

/// Extract only the article body from a full HTML page.
#[cfg(test)]
pub(crate) fn extract_article_body(html: &str) -> String {
    article_body_in(&parse_page(html).expect("within budget")).expect("within budget")
}

/// The article's text: the best-scoring container (semantic containers first, page furniture
/// left out), then the innermost container inside it that still holds most of its text; the
/// whole page's text when no container qualifies.
fn article_body_in(doc: &Html) -> Result<String, TooComplex> {
    let page = PageText::of(doc)?;
    // Qualifying containers in document order, with their score and text length.
    let qualified: Vec<(usize, i32, usize)> = page
        .containers
        .iter()
        .enumerate()
        .filter_map(|(index, container)| {
            let len = page.text_of(container).len();
            if len < 200 {
                return None;
            }
            let score = container_score(container.element, page.words_of(container));
            (score > 0).then_some((index, score, len))
        })
        .collect();
    // The order of the scan before: by tag name in `CONTAINERS` order, then document order; a
    // later one wins only with a higher score, or the same score and longer text.
    let kind = |index: usize| {
        let name = page.containers[index].element.value().name();
        CONTAINERS.iter().position(|c| *c == name).unwrap_or(0)
    };
    let best_of = |candidates: &[(usize, i32, usize)]| {
        candidates.iter().copied().max_by(|a, b| {
            (a.1, a.2)
                .cmp(&(b.1, b.2))
                .then_with(|| (kind(b.0), b.0).cmp(&(kind(a.0), a.0)))
        })
    };
    let Some(mut chosen) = best_of(&qualified) else {
        return Ok(page.text.trim().to_owned());
    };
    loop {
        let (index, _, len) = chosen;
        let until = page.containers[index].descendants_until;
        let from = qualified.partition_point(|q| q.0 <= index);
        let to = qualified.partition_point(|q| q.0 < until);
        let inner: Vec<_> = qualified[from..to]
            .iter()
            .copied()
            .filter(|q| q.2 * 100 >= len * INNER_SHARE_PERCENT)
            .collect();
        match best_of(&inner) {
            Some(next) => chosen = next,
            None => break,
        }
    }
    // The chosen container's text again, now with its own header and footer.
    let element = page.containers[chosen.0].element;
    let inside = PageText::below(*element, NOT_TEXT_INSIDE)?;
    Ok(inside.text.trim().to_owned())
}

#[derive(Debug, Clone)]
pub(crate) struct ArticleFetch {
    pub(crate) text: String,
    pub(crate) resolved_link: Option<String>,
    pub(crate) link_note: Option<String>,
    /// Publish date the page itself declares, if any.
    pub(crate) published_at: Option<i64>,
    /// Venue of a schema.org Event on the page.
    pub(crate) event: EventLocation,
    /// District the page files the item under (Polizei Berlin "Ereignisort").
    pub(crate) district: Option<String>,
    /// The page says it is for subscribers: its text is not the article.
    pub(crate) paywalled: bool,
}

impl ArticleFetch {
    fn paywalled() -> Self {
        ArticleFetch {
            text: String::new(),
            resolved_link: None,
            link_note: None,
            published_at: None,
            event: EventLocation::default(),
            district: None,
            paywalled: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct EventLocation {
    pub(crate) coords: Option<(f64, f64)>,
    pub(crate) address: Option<String>,
}

/// Where a schema.org `Event` in the page's JSON-LD takes place. Only Event
/// objects are read: publisher `Organization` blocks carry the newsroom's
/// own address, which says nothing about the story.
#[cfg(test)]
pub(crate) fn page_event_location(html: &str) -> EventLocation {
    event_location_in(&Html::parse_document(html))
}

fn event_location_in(doc: &Html) -> EventLocation {
    json_ld_blocks(doc)
        .find_map(|json| find_event_location(&json))
        .unwrap_or_default()
}

fn find_event_location(value: &serde_json::Value) -> Option<EventLocation> {
    match value {
        serde_json::Value::Array(items) => items.iter().find_map(find_event_location),
        serde_json::Value::Object(obj) => {
            let is_event = match &obj.get("@type") {
                Some(serde_json::Value::String(t)) => t.ends_with("Event"),
                Some(serde_json::Value::Array(ts)) => ts
                    .iter()
                    .any(|t| t.as_str().is_some_and(|t| t.ends_with("Event"))),
                _ => false,
            };
            if is_event {
                let location = obj.get("location")?;
                let location = location
                    .as_array()
                    .and_then(|l| l.first())
                    .unwrap_or(location);
                let found = EventLocation {
                    coords: json_ld_geo(&location["geo"]),
                    address: json_ld_address(&location["address"])
                        .or_else(|| location.as_str().map(str::to_owned)),
                };
                return (found != EventLocation::default()).then_some(found);
            }
            obj.get("@graph").and_then(find_event_location)
        }
        _ => None,
    }
}

fn json_ld_number(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse().ok())
}

fn json_ld_geo(geo: &serde_json::Value) -> Option<(f64, f64)> {
    let lat = json_ld_number(&geo["latitude"])?;
    let lon = json_ld_number(&geo["longitude"])?;
    ((-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon)).then_some((lat, lon))
}

fn json_ld_address(address: &serde_json::Value) -> Option<String> {
    if let Some(text) = address.as_str() {
        return Some(text.trim().to_owned()).filter(|t| !t.is_empty());
    }
    let parts: Vec<&str> = ["streetAddress", "postalCode", "addressLocality"]
        .iter()
        .filter_map(|key| address[*key].as_str())
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect();
    (!parts.is_empty()).then(|| parts.join(" "))
}

/// The district a page files its story under: Polizei Berlin reports carry
/// `<p class="polizeimeldung" title="Ereignisort">Charlottenburg-Wilmersdorf</p>`.
#[cfg(test)]
pub(crate) fn page_district(html: &str) -> Option<String> {
    district_in(&Html::parse_document(html))
}

fn district_in(doc: &Html) -> Option<String> {
    doc.select(&selector("[title]"))
        .find(|e| {
            e.value()
                .attr("title")
                .is_some_and(|t| t.eq_ignore_ascii_case("ereignisort"))
        })
        .map(|e| e.text().collect::<String>().trim().to_owned())
        .filter(|district| !district.is_empty())
}

/// Publish date from the page's metadata: Open Graph / article meta tags or
/// schema.org JSON-LD `datePublished`. Modification dates are ignored. An
/// updated old article is still an old event.
#[cfg(test)]
pub(crate) fn page_published_at(html: &str) -> Option<i64> {
    published_at_in(&Html::parse_document(html))
}

fn published_at_in(doc: &Html) -> Option<i64> {
    const META_NAMES: &[&str] = &[
        "article:published_time",
        "og:article:published_time",
        "datePublished",
        "date",
        "dc.date",
        "dc.date.issued",
        "pubdate",
    ];
    META_NAMES
        .iter()
        .find_map(|name| meta_content_in(doc, name))
        .or_else(|| {
            json_ld_article(doc)?["datePublished"]
                .as_str()
                .map(str::to_owned)
        })
        .and_then(|value| parse_page_date(&value))
}

/// schema.org types of a news article.
const ARTICLE_TYPES: &[&str] = &[
    "NewsArticle",
    "Article",
    "ReportageNewsArticle",
    "BlogPosting",
];

/// The page's own article in its JSON-LD (an object of an [`ARTICLE_TYPES`] type, also inside
/// `@graph` or a list), not data about other articles that the page embeds.
fn json_ld_article(doc: &Html) -> Option<serde_json::Value> {
    fn find(value: &serde_json::Value) -> Option<&serde_json::Value> {
        match value {
            serde_json::Value::Array(items) => items.iter().find_map(find),
            serde_json::Value::Object(obj) => {
                let is_article = match obj.get("@type") {
                    Some(serde_json::Value::String(t)) => ARTICLE_TYPES.contains(&t.as_str()),
                    Some(serde_json::Value::Array(ts)) => ts
                        .iter()
                        .any(|t| t.as_str().is_some_and(|t| ARTICLE_TYPES.contains(&t))),
                    _ => false,
                };
                if is_article {
                    Some(value)
                } else {
                    obj.get("@graph").and_then(find)
                }
            }
            _ => None,
        }
    }
    json_ld_blocks(doc).find_map(|json| find(&json).cloned())
}

/// The parsed JSON-LD blocks of the page.
fn json_ld_blocks(doc: &Html) -> impl Iterator<Item = serde_json::Value> + '_ {
    static SCRIPTS: std::sync::LazyLock<Selector> =
        std::sync::LazyLock::new(|| selector("script[type]"));
    doc.select(&SCRIPTS)
        .filter(|script| {
            script
                .value()
                .attr("type")
                .is_some_and(|t| t.trim().eq_ignore_ascii_case("application/ld+json"))
        })
        .filter_map(|script| {
            let body: String = script.text().collect();
            serde_json::from_str(body.trim()).ok()
        })
}

/// Whether the page's article says it is not free to read (`"isAccessibleForFree": false`).
fn paywalled_in(doc: &Html) -> bool {
    json_ld_article(doc).is_some_and(|article| match &article["isAccessibleForFree"] {
        serde_json::Value::Bool(free) => !free,
        serde_json::Value::String(free) => free.trim().eq_ignore_ascii_case("false"),
        _ => false,
    })
}

fn parse_page_date(value: &str) -> Option<i64> {
    let value = value.trim();
    parse_feed_date(value)
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
                .ok()
                .and_then(|dt| dt.and_local_timezone(chrono::Local).earliest())
                .map(|dt| dt.timestamp())
        })
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(value.get(..10)?, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(12, 0, 0))
                .map(|dt| dt.and_utc().timestamp())
        })
}

/// The `content` of the first `<meta>` whose `property` (or, without one, `name`) is `name`.
fn meta_content_in(doc: &Html, name: &str) -> Option<String> {
    doc.select(&selector("meta"))
        .find(|meta| {
            let element = meta.value();
            element
                .attr("property")
                .or_else(|| element.attr("name"))
                .is_some_and(|p| p.trim().eq_ignore_ascii_case(name))
        })
        .and_then(|meta| meta.value().attr("content"))
        .map(str::trim)
        .filter(|content| !content.is_empty())
        .map(str::to_owned)
}

/// The page's `<link rel="canonical">`.
#[cfg(test)]
pub(crate) fn page_canonical(html: &str) -> Option<String> {
    canonical_in(&Html::parse_document(html))
}

fn canonical_in(doc: &Html) -> Option<String> {
    doc.select(&selector("link[rel][href]"))
        .find(|link| {
            link.value().attr("rel").is_some_and(|rel| {
                rel.split_whitespace()
                    .any(|r| r.eq_ignore_ascii_case("canonical"))
            })
        })
        .and_then(|link| link.value().attr("href"))
        .map(str::trim)
        .filter(|href| !href.is_empty())
        .map(str::to_owned)
}

pub(crate) fn looks_like_google_url(url: &str) -> bool {
    let u = url.to_lowercase();
    u.contains("://news.google.") || u.contains("://www.google.") || u.contains("://google.")
}

pub(crate) fn decode_url_parameter(url: &str, keys: &[&str]) -> Option<String> {
    let query_start = url.find('?')?;
    for part in url[query_start + 1..].split('&') {
        let (key, value) = part.split_once('=')?;
        if keys.iter().any(|wanted| key.eq_ignore_ascii_case(wanted)) {
            let decoded = percent_decode(value.replace('+', " ").as_str());
            if decoded.starts_with("http://") || decoded.starts_with("https://") {
                return Some(decoded);
            }
        }
    }
    None
}

pub(crate) fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn external_href_in(doc: &Html) -> Option<String> {
    doc.select(&selector("a[href]")).find_map(|a| {
        let href = a.value().attr("href")?.trim().to_owned();
        let candidate = decode_url_parameter(&href, &["url", "q"]).unwrap_or(href);
        ((candidate.starts_with("http://") || candidate.starts_with("https://"))
            && !looks_like_google_url(&candidate))
        .then_some(candidate)
    })
}

pub(crate) fn looks_like_rolling_url(url: &str) -> bool {
    let u = url.to_lowercase();
    [
        "ticker",
        "liveticker",
        "liveblog",
        "newsblog",
        "live-blog",
        "live-ticker",
    ]
    .iter()
    .any(|needle| u.contains(needle))
}

pub(crate) fn significant_words(s: &str) -> Vec<String> {
    normalize(s)
        .split_whitespace()
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric()).to_owned())
        .filter(|w| w.chars().count() >= 5)
        .filter(|w| {
            !matches!(
                w.as_str(),
                "berlin"
                    | "polizei"
                    | "meldung"
                    | "update"
                    | "heute"
                    | "werden"
                    | "wurde"
                    | "wurden"
                    | "worden"
                    | "einem"
                    | "einer"
                    | "eines"
                    | "diese"
                    | "dieser"
                    | "nicht"
                    | "gegen"
                    | "unter"
                    | "sollen"
                    | "haben"
                    | "zwischen"
                    | "liveticker"
            )
        })
        .take(8)
        .collect()
}

pub(crate) fn text_matches_title(text: &str, title: &str) -> bool {
    let words = significant_words(title);
    if words.len() < 2 {
        return true;
    }
    let haystack = normalize(text);
    let hits = words
        .iter()
        .filter(|w| haystack.contains(w.as_str()))
        .count();
    hits >= 2 || hits * 2 >= words.len()
}

#[cfg(test)]
pub(crate) fn resolve_article_link(
    feed_url: &str,
    html: &str,
    article_text: &str,
    title: &str,
) -> (Option<String>, Option<String>) {
    resolve_link_in(feed_url, &Html::parse_document(html), article_text, title)
}

fn resolve_link_in(
    feed_url: &str,
    doc: &Html,
    article_text: &str,
    title: &str,
) -> (Option<String>, Option<String>) {
    let canonical = canonical_in(doc)
        .or_else(|| meta_content_in(doc, "og:url"))
        .filter(|url| url.starts_with("http://") || url.starts_with("https://"));

    let mut link = canonical.unwrap_or_else(|| feed_url.to_owned());
    if looks_like_google_url(&link) || looks_like_google_url(feed_url) {
        if let Some(external) = external_href_in(doc) {
            link = external;
        }
    }
    let rolling = looks_like_rolling_url(feed_url) || looks_like_rolling_url(&link);
    let same_item = text_matches_title(article_text, title);
    let mut note = None;

    if rolling && !same_item {
        note = Some("rolling source link may no longer show this item".to_owned());
    } else if rolling {
        note = Some("rolling source link".to_owned());
    }

    if link.trim().is_empty() {
        link = feed_url.to_owned();
    }
    (Some(link), note)
}

/// What fetching an item's link gave.
pub(crate) enum Page {
    Article(ArticleFetch),
    /// The link is no article page (a PDF, a video, an error page, a page too large or too
    /// complex to read): the item is decided on its feed text.
    NotAnArticle(String),
    /// Unreachable, rate-limited, a server error, or no text: tried again next poll.
    Failed,
}

/// Whether a content type is a web page.
fn is_html(content_type: &str) -> bool {
    content_type.to_ascii_lowercase().contains("html")
}

/// The page as text: by the charset of its `Content-Type`, else by a `<meta charset>` (or
/// `http-equiv` content type) in its first 1,024 bytes, else as UTF-8.
fn page_text(response: &crate::fetch::Response) -> String {
    let header = response
        .content_type
        .as_deref()
        .and_then(crate::fetch::charset_of);
    if header.is_some() {
        return response.body.clone();
    }
    let head = String::from_utf8_lossy(&response.bytes[..response.bytes.len().min(1024)])
        .to_ascii_lowercase();
    let declared = head.find("charset=").and_then(|at| {
        let value = head[at + "charset=".len()..].trim_start_matches(['"', '\'', ' ']);
        let end = value
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'))
            .unwrap_or(value.len());
        encoding_rs::Encoding::for_label(&value.as_bytes()[..end])
    });
    match declared {
        Some(encoding) => encoding.decode(&response.bytes).0.into_owned(),
        None => response.body.clone(),
    }
}

/// Fetch a URL and return the article body plus resolved link metadata.
pub(crate) async fn fetch_article(http: &Http, url: &str, title: &str) -> Page {
    let response = match http
        .get_capped(url, &[], Duration::from_secs(10), MAX_PAGE_BYTES)
        .await
    {
        Ok(response) => response,
        Err(e @ (FetchError::TooLarge(_) | FetchError::Refused(_))) => {
            return Page::NotAnArticle(e.to_string())
        }
        Err(e) => {
            tracing::debug!("article fetch failed {url}: {e}");
            return Page::Failed;
        }
    };
    match response.status {
        200..=299 => {}
        429 | 500..=599 => {
            tracing::debug!("article fetch failed {url}: HTTP {}", response.status);
            return Page::Failed;
        }
        status => return Page::NotAnArticle(format!("HTTP {status}")),
    }
    if let Some(content_type) = response.content_type.as_deref() {
        if !is_html(content_type) {
            return Page::NotAnArticle(content_type.to_owned());
        }
    }
    let html = page_text(&response);

    // Parsing a large page takes a while: off the async runtime, with a deadline.
    let title = title.to_owned();
    let feed_url = url.to_owned();
    let reading = tokio::task::spawn_blocking(move || read_page(&feed_url, &html, &title));
    match tokio::time::timeout(READ_TIMEOUT, reading).await {
        Ok(Ok(Ok(Some(article)))) if article.paywalled => {
            Page::NotAnArticle("paywalled".to_owned())
        }
        Ok(Ok(Ok(Some(article)))) => Page::Article(article),
        Ok(Ok(Err(TooComplex))) | Err(_) => Page::NotAnArticle("too complex".to_owned()),
        Ok(Ok(Ok(None))) | Ok(Err(_)) => Page::Failed,
    }
}

/// Everything the bot reads from a page, parsed once. `None` when it has no text.
pub(crate) fn read_page(
    feed_url: &str,
    html: &str,
    title: &str,
) -> Result<Option<ArticleFetch>, TooComplex> {
    let doc = parse_page(html)?;
    if paywalled_in(&doc) {
        return Ok(Some(ArticleFetch::paywalled()));
    }
    let text = article_body_in(&doc)?;
    if text.is_empty() {
        return Ok(None);
    }
    let (resolved_link, link_note) = resolve_link_in(feed_url, &doc, &text, title);
    Ok(Some(ArticleFetch {
        resolved_link,
        link_note,
        published_at: published_at_in(&doc),
        event: event_location_in(&doc),
        district: district_in(&doc),
        text,
        paywalled: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_publish_date_comes_from_meta_or_json_ld() {
        let meta =
            r#"<meta property="article:published_time" content="2024-03-01T08:00:00+01:00">"#;
        let json_ld = r#"<script type="application/ld+json">{"@type":"NewsArticle","datePublished": "2024-03-01T07:00:00Z","dateModified":"2026-09-26T10:00:00Z"}</script>"#;

        assert_eq!(page_published_at(meta), Some(1_709_276_400));
        assert_eq!(page_published_at(json_ld), Some(1_709_276_400));
        assert_eq!(page_published_at("<p>no date</p>"), None);
    }

    #[test]
    fn event_venue_comes_from_event_json_ld_not_the_publisher() {
        let html = r#"<script type="application/ld+json">{"@context":"https://schema.org","@graph":[
            {"@type":"Organization","name":"rbb","address":{"streetAddress":"Masurenallee 8-14"}},
            {"@type":"MusicEvent","name":"Konzert","location":{"@type":"Place","name":"Quasimodo",
             "address":{"@type":"PostalAddress","streetAddress":"Pestalozzistr. 99","postalCode":"10587","addressLocality":"Berlin"},
             "geo":{"@type":"GeoCoordinates","latitude":"52.4675","longitude":13.4039}}}]}</script>"#;

        let event = page_event_location(html);

        assert_eq!(event.coords, Some((52.4675, 13.4039)));
        assert_eq!(
            event.address.as_deref(),
            Some("Pestalozzistr. 99 10587 Berlin")
        );

        let publisher_only = r#"<script type="application/ld+json">{"@type":"NewsArticle",
            "publisher":{"@type":"Organization","address":{"streetAddress":"Masurenallee 8-14"}}}</script>"#;
        assert_eq!(
            page_event_location(publisher_only),
            EventLocation::default()
        );
    }

    #[test]
    fn police_report_district_is_read_from_the_ereignisort_tag() {
        let html = r#"<p class="polizeimeldung">Polizeimeldung vom 26.09.2026</p><p class="polizeimeldung" title="Ereignisort">Charlottenburg-Wilmersdorf</p>"#;

        assert_eq!(
            page_district(html).as_deref(),
            Some("Charlottenburg-Wilmersdorf")
        );
        assert_eq!(page_district("<p>kein Tag</p>"), None);
    }

    #[test]
    fn article_pages_are_reduced_to_their_text() {
        for (page, golden) in [
            ("pages/article_layout.html", "article_layout.txt"),
            ("pages/nested_div.html", "article_nested_div.txt"),
            ("pages/event_jsonld.html", "article_event_jsonld.txt"),
        ] {
            let body = extract_article_body(&crate::testing::fixture(page));

            for footer in ["Impressum", "Datenschutz", "Start Polizei Presse"] {
                assert!(!body.contains(footer), "{page}: {footer:?} in {body:?}");
            }
            crate::testing::assert_golden(golden, &body);
        }
    }

    #[test]
    fn page_metadata_comes_from_the_fixtures() {
        let police = crate::testing::fixture("pages/article_layout.html");
        let event = crate::testing::fixture("pages/event_jsonld.html");

        assert_eq!(page_district(&police).as_deref(), Some("YourDistrict"));
        assert_eq!(page_published_at(&police), Some(1_795_158_000));
        assert_eq!(
            page_canonical(&police).as_deref(),
            Some("https://example.com/police/1001")
        );
        let venue = page_event_location(&event);
        assert_eq!(venue.coords, Some((52.4745, 13.4205)));
        assert_eq!(
            venue.address.as_deref(),
            Some("Musterstraße 1 10000 Yourcity")
        );
    }

    #[test]
    fn rolling_link_gets_note_when_title_no_longer_matches() {
        let html = r#"
            <html><head><link rel="canonical" href="https://example.test/live/ticker" /></head>
            <body><main>Aktuelle Meldungen zur Verkehrslage ohne den alten Eintrag.</main></body></html>
        "#;
        let (link, note) = resolve_article_link(
            "https://example.test/liveticker",
            html,
            "Aktuelle Meldungen zur Verkehrslage ohne den alten Eintrag.",
            "Brand in der Perleberger Straße",
        );

        assert_eq!(link.as_deref(), Some("https://example.test/live/ticker"));
        assert_eq!(
            note.as_deref(),
            Some("rolling source link may no longer show this item")
        );
    }

    #[test]
    fn google_news_link_resolves_to_external_publisher_when_available() {
        let html = r#"
            <html><head><link rel="canonical" href="https://news.google.com/rss/articles/abc" /></head>
            <body><a href="https://www.google.com/url?url=https%3A%2F%2Fpublisher.test%2Farticle%2F1">Publisher</a></body></html>
        "#;
        let (link, note) = resolve_article_link(
            "https://news.google.com/rss/articles/abc",
            html,
            "Brand in der Perleberger Straße mit Feuerwehreinsatz",
            "Brand in der Perleberger Straße",
        );

        assert_eq!(link.as_deref(), Some("https://publisher.test/article/1"));
        assert_eq!(note, None);
    }

    /// Runs `read_page` on a thread with the stack of a tokio blocking thread (2 MiB) and
    /// returns how long it took. A stack overflow aborts the whole test binary: that is the
    /// failure this guards against, a panic would not show it.
    fn read_on_small_stack(html: String) -> (Duration, bool) {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let started = Instant::now();
                let read = read_page("https://example.com/a", &html, "Titel");
                (started.elapsed(), matches!(read, Err(TooComplex)))
            })
            .expect("spawn")
            .join()
            .expect("read_page returned")
    }

    #[test]
    fn deeply_nested_or_huge_pages_return_quickly() {
        let nested_divs = format!(
            "<html><body>{}<p>Text</p></body></html>",
            "<div>".repeat(100_000)
        );
        let nested_spans = format!(
            "<html><body><article>{}Text</article></body></html>",
            "<span>".repeat(100_000)
        );
        let siblings = format!(
            "<html><body>{}{}{}</body></html>",
            "<div class=\"wrap\">".repeat(30),
            "<div>Ein Satz mit einigen Wörtern.</div>".repeat(50_000),
            "</div>".repeat(30)
        );
        for (name, html, too_complex) in [
            ("100,000 nested div", nested_divs, Some(true)),
            (
                "100,000 nested span in an article",
                nested_spans,
                Some(true),
            ),
            // Ordinary in shape; whether a debug build parses it within the budget depends
            // on the machine.
            ("50,000 sibling div under 30 wrappers", siblings, None),
        ] {
            let (took, was_too_complex) = read_on_small_stack(html);
            assert!(took < Duration::from_secs(2), "{name}: {took:?}");
            if let Some(expected) = too_complex {
                assert_eq!(was_too_complex, expected, "{name}");
            }
        }
    }

    #[test]
    fn the_innermost_container_with_the_article_wins_and_teasers_stay_out() {
        for (page, golden) in [
            ("pages/wrapper_teasers.html", "article_wrapper_teasers.txt"),
            ("pages/main_aside.html", "article_main_aside.txt"),
            ("pages/more_on_topic.html", "article_more_on_topic.txt"),
        ] {
            let body = extract_article_body(&crate::testing::fixture(page));

            for furniture in [
                "Startseite",
                "Messerangriff",
                "Fahrraddiebstahl",
                "Mehr zum Thema",
                "Wohnungsbrand",
                "Newsletter",
            ] {
                assert!(
                    !body.contains(furniture),
                    "{page}: {furniture:?} in {body:?}"
                );
            }
            crate::testing::assert_golden(golden, &body);
        }
    }

    #[test]
    fn blocks_are_separated_and_soft_hyphens_dropped() {
        let html = "<html><body><article><h1>Brand in Moabit</h1><p>Die Feuerwehr war in \
                    Charlotten&shy;burg im Einsatz, Stra\u{200B}ße frei.</p></article></body></html>";
        let body = extract_article_body(html);

        assert!(
            body.starts_with("Brand in Moabit Die Feuerwehr"),
            "{body:?}"
        );
        assert!(body.contains("Charlottenburg im Einsatz"), "{body:?}");
        assert!(body.contains("Straße frei."), "{body:?}");
    }

    #[test]
    fn article_extraction_prefers_article_like_content_over_navigation() {
        let html = r#"
            <html><body>
                <div class="nav">Start Politik Sport Wetter Navigation Werbung</div>
                <div class="article-body">
                    <p>Brand in der Perleberger Straße. Die Feuerwehr war am Abend im Einsatz.</p>
                    <p>Mehrere Menschen bemerkten Rauch in einem Wohnhaus nahe der Kreuzung.</p>
                    <p>Die Polizei sperrte den Bereich zeitweise ab und leitete den Verkehr um.</p>
                    <p>Weitere Details sollen im Laufe des Tages bekannt gegeben werden.</p>
                </div>
                <footer>Impressum Datenschutz Kontakt Newsletter</footer>
            </body></html>
        "#;

        let body = extract_article_body(html);

        assert!(body.contains("Brand in der Perleberger Straße"));
        assert!(!body.contains("Impressum Datenschutz"));
    }

    #[test]
    fn an_articles_own_header_with_its_lead_is_kept() {
        let html = r#"<html><body><header>Startseite Lokales Polizei</header>
            <article><header><h1>Wasserrohrbruch</h1>
            <p class="lead">In der Britzer Allee ist eine Leitung gebrochen.</p></header>
            <p>Die Wasserbetriebe haben die Leitung am Vormittag abgesperrt. Rund vierzig Wohnungen
            waren bis zum Nachmittag ohne Wasser, ein Tankwagen stand an der Ecke Musterstraße.
            Die Straße bleibt bis zum Freitag zwischen den beiden Kreuzungen gesperrt.</p>
            <footer>Von unserer Redaktion</footer></article></body></html>"#;

        let body = extract_article_body(html);

        assert!(
            body.starts_with("Wasserrohrbruch In der Britzer Allee"),
            "{body}"
        );
        assert!(body.ends_with("Von unserer Redaktion"), "{body}");
        assert!(!body.contains("Startseite"), "{body}");
    }

    #[test]
    fn containers_are_scored_on_their_tokens() {
        let doc = Html::parse_document(
            r#"<div class="article-lead" id="x"></div><div class="download-box"></div>
               <div data-role="article" style="content"></div><div class="articleBody"></div>
               <div class="comments-list"></div>"#,
        );
        let score = |class: &str| {
            let element = doc
                .select(&selector("div"))
                .find(|d| {
                    d.value().attr("class") == Some(class)
                        || d.value().attr("data-role") == Some(class)
                })
                .unwrap();
            container_score(element, 0)
        };

        assert_eq!(score("article-lead"), 4, "not penalised for \"ad\"");
        assert_eq!(score("download-box"), 0);
        assert_eq!(
            score("article"),
            0,
            "data- and style attributes do not count"
        );
        assert_eq!(score("articleBody"), 8);
        assert_eq!(score("comments-list"), -5);
    }

    #[test]
    fn the_publish_date_is_the_articles_own() {
        let html = r#"<script>window.state = {"related": [{"datePublished": "2019-03-01T08:00:00Z"}]};</script>
            <script type="application/ld+json">{"@graph": [
              {"@type": "ItemList", "itemListElement": [{"@type": "NewsArticle", "datePublished": "2020-01-01T08:00:00Z"}]},
              {"@type": "NewsArticle", "headline": "Brand", "datePublished": "2024-03-01T07:00:00Z"}]}</script>"#;

        assert_eq!(page_published_at(html), Some(1_709_276_400));
    }

    #[test]
    fn a_page_without_a_header_charset_is_read_by_its_meta_charset() {
        let mut page = b"<html><head><meta charset=\"iso-8859-1\"></head><body><p>Stra".to_vec();
        page.push(0xDF);
        page.extend_from_slice(b"e</p></body></html>");

        let named = crate::fetch::Response::for_test(200, Some("text/html"), page.clone());
        let header =
            crate::fetch::Response::for_test(200, Some("text/html; charset=windows-1252"), page);

        assert!(page_text(&named).contains("Straße"));
        assert!(page_text(&header).contains("Straße"));
    }

    #[tokio::test]
    async fn a_paywalled_page_is_not_the_article() {
        let page = r#"<html><head><script type="application/ld+json">{"@type": "NewsArticle",
            "isAccessibleForFree": false, "datePublished": "2026-11-20T08:00:00+01:00"}</script></head>
            <body><article><p>Nur für Abonnenten: Brand in der Perleberger Straße. Lesen Sie weiter
            mit unserem Digitalabo, jetzt einen Monat kostenlos testen und alle Artikel lesen.
            Jederzeit kündbar, keine Mindestlaufzeit, alle Inhalte auf allen Geräten.</p></article>
            </body></html>"#;
        let http = Http::recorded(std::sync::Arc::new(crate::fetch::Recorded::new().route(
            "https://paper.test/",
            200,
            page,
        )));

        let fetched = fetch_article(&http, "https://paper.test/a", "Brand").await;

        assert!(matches!(fetched, Page::NotAnArticle(why) if why == "paywalled"));
    }
}
