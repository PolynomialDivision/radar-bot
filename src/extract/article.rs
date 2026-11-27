//! Fetching article pages, reading their text and metadata, resolving aggregator links.
//!
//! A page is parsed once (`scraper`, an HTML5 parser) and every reading works on that document.

use std::time::Duration;

use scraper::{node::Node, ElementRef, Html, Selector};

use crate::feed::parse_feed_date;
use crate::fetch::{Http, MAX_PAGE_BYTES};
use crate::filter::normalize;

// ── Article text ──────────────────────────────────────────────────────────────

/// Elements whose text is page furniture, never the article.
const NOT_TEXT: &[&str] = &[
    "script", "style", "nav", "header", "footer", "aside", "noscript", "template",
];

/// Candidate containers of the article, in the order they are considered.
const CONTAINERS: &[&str] = &["article", "main", "section", "div"];

fn selector(css: &str) -> Selector {
    Selector::parse(css).expect("a valid selector")
}

/// The text of `element` without the text of [`NOT_TEXT`] elements inside it, whitespace
/// collapsed.
fn visible_text(element: ElementRef<'_>) -> String {
    fn walk(element: ElementRef<'_>, out: &mut String) {
        for child in element.children() {
            if let Some(child_element) = ElementRef::wrap(child) {
                if !NOT_TEXT.contains(&child_element.value().name()) {
                    walk(child_element, out);
                }
            } else if let Node::Text(text) = child.value() {
                out.push_str(text);
            }
        }
    }
    let mut out = String::new();
    walk(element, &mut out);
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The opening tag as text (`<div class="article-body">`), for [`article_container_score`].
fn opening_tag(element: ElementRef<'_>) -> String {
    let attrs: Vec<String> = element
        .value()
        .attrs()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect();
    format!("<{} {}>", element.value().name(), attrs.join(" "))
}

pub(crate) fn article_container_score(tag: &str, text: &str) -> i32 {
    let tag = tag.to_lowercase();
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
        if tag.contains(needle) {
            score += 4;
        }
    }
    for needle in [
        "nav", "menu", "footer", "header", "aside", "comment", "related", "teaser", "ad", "advert",
        "social",
    ] {
        if tag.contains(needle) {
            score -= 5;
        }
    }
    let words = text.split_whitespace().count() as i32;
    score + (words / 80).min(8)
}

/// Extract only the article body from a full HTML page.
#[cfg(test)]
pub(crate) fn extract_article_body(html: &str) -> String {
    article_body_in(&Html::parse_document(html))
}

/// The best-scoring container's text (semantic containers first, page furniture left out), or
/// the whole page's text when no container qualifies.
fn article_body_in(doc: &Html) -> String {
    let mut best: Option<(i32, String)> = None;
    for name in CONTAINERS {
        for element in doc.select(&selector(name)) {
            let text = visible_text(element);
            if text.len() < 200 {
                continue;
            }
            let score = article_container_score(&opening_tag(element), &text);
            if score > 0
                && best.as_ref().is_none_or(|(best_score, best_text)| {
                    score > *best_score || (score == *best_score && text.len() > best_text.len())
                })
            {
                best = Some((score, text));
            }
        }
    }
    best.map(|(_, text)| text)
        .unwrap_or_else(|| visible_text(doc.root_element()))
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
    for script in doc.select(&selector("script[type]")) {
        let is_json_ld = script
            .value()
            .attr("type")
            .is_some_and(|t| t.trim().eq_ignore_ascii_case("application/ld+json"));
        if !is_json_ld {
            continue;
        }
        let body: String = script.text().collect();
        let Ok(json) = serde_json::from_str::<serde_json::Value>(body.trim()) else {
            continue;
        };
        if let Some(found) = find_event_location(&json) {
            return found;
        }
    }
    EventLocation::default()
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
    published_at_in(&Html::parse_document(html), html)
}

fn published_at_in(doc: &Html, html: &str) -> Option<i64> {
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
        .or_else(|| json_ld_date_published(html))
        .and_then(|value| parse_page_date(&value))
}

fn json_ld_date_published(html: &str) -> Option<String> {
    let pos = html.find("\"datePublished\"")?;
    let rest = &html[pos + "\"datePublished\"".len()..];
    let rest = rest.trim_start().strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    Some(rest[..rest.find('"')?].to_owned())
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
    /// The link is no web page (a PDF, a video): the item is decided on its feed text.
    NotAnArticle(String),
    /// Unreachable, too large, or no text: tried again next poll.
    Failed,
}

/// Whether a content type is a web page.
fn is_html(content_type: &str) -> bool {
    content_type.to_ascii_lowercase().contains("html")
}

/// Fetch a URL and return the article body plus resolved link metadata.
pub(crate) async fn fetch_article(http: &Http, url: &str, title: &str) -> Page {
    let response = match http
        .get_capped(url, &[], Duration::from_secs(10), MAX_PAGE_BYTES)
        .await
    {
        Ok(response) => response,
        Err(e) => {
            tracing::debug!("article fetch failed {url}: {e}");
            return Page::Failed;
        }
    };
    if let Some(content_type) = response.content_type.as_deref() {
        if !is_html(content_type) {
            return Page::NotAnArticle(content_type.to_owned());
        }
    }
    let html = response.body;

    // Parsing a large page takes a while: off the async runtime.
    let title = title.to_owned();
    let feed_url = url.to_owned();
    let article = tokio::task::spawn_blocking(move || read_page(&feed_url, &html, &title))
        .await
        .ok()
        .flatten();

    article.map_or(Page::Failed, Page::Article)
}

/// Everything the bot reads from a page, parsed once. `None` when it has no text.
fn read_page(feed_url: &str, html: &str, title: &str) -> Option<ArticleFetch> {
    let doc = Html::parse_document(html);
    let text = article_body_in(&doc).trim().to_owned();
    if text.is_empty() {
        return None;
    }
    let (resolved_link, link_note) = resolve_link_in(feed_url, &doc, &text, title);
    Some(ArticleFetch {
        resolved_link,
        link_note,
        published_at: published_at_in(&doc, html),
        event: event_location_in(&doc),
        district: district_in(&doc),
        text,
    })
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
}
