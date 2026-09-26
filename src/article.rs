//! Fetching article pages and resolving aggregator links.

use super::*;

// ── Article fetcher ───────────────────────────────────────────────────────────

pub(crate) fn tag_blocks<'a>(html: &'a str, tag_name: &str) -> Vec<(&'a str, &'a str)> {
    let lower = html.to_lowercase();
    let open_str = format!("<{tag_name}");
    let close_str = format!("</{tag_name}>");
    let mut out = Vec::new();
    let mut offset = 0;

    while let Some(pos) = lower[offset..].find(&open_str) {
        let start = offset + pos;
        let Some(gt_rel) = html[start..].find('>') else {
            break;
        };
        let content_start = start + gt_rel + 1;
        let Some(close_rel) = lower[content_start..].find(&close_str) else {
            break;
        };
        let content_end = content_start + close_rel;
        let tag = &html[start..content_start];
        let inner = &html[content_start..content_end];
        out.push((tag, inner));
        offset = content_end + close_str.len();
    }

    out
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

/// Remove all `<tag>…</tag>` blocks from html (nav, header, footer, aside, script, style).
pub(crate) fn remove_blocks(html: &str, tags: &[&str]) -> String {
    let mut result = html.to_owned();
    for tag in tags {
        let open_str = format!("<{tag}");
        let close_str = format!("</{tag}>");
        // Keep both original and lowercase in sync so to_lowercase() runs once per removal,
        // not once per loop iteration over the whole string.
        let mut lower = result.to_lowercase();
        loop {
            match (lower.find(&open_str), lower.find(&close_str)) {
                (Some(s), Some(e)) if s < e => {
                    let end = e + close_str.len();
                    result = format!("{}{}", &result[..s], &result[end..]);
                    lower = format!("{}{}", &lower[..s], &lower[end..]);
                }
                _ => break,
            }
        }
    }
    result
}

/// Extract only the article body from a full HTML page.
/// Tries semantic containers first, then strips navigation blocks.
pub(crate) fn extract_article_body(html: &str) -> String {
    let cleaned = remove_blocks(
        html,
        &[
            "script", "style", "nav", "header", "footer", "aside", "noscript",
        ],
    );

    let mut best: Option<(i32, String)> = None;
    for tag_name in &["article", "main", "section", "div"] {
        for (tag, inner) in tag_blocks(&cleaned, tag_name) {
            let text = strip_html(inner);
            let text = text.trim();
            if text.len() < 200 {
                continue;
            }
            let score = article_container_score(tag, text);
            if score > 0
                && best.as_ref().is_none_or(|(best_score, best_text)| {
                    score > *best_score || (score == *best_score && text.len() > best_text.len())
                })
            {
                best = Some((score, text.to_owned()));
            }
        }
    }

    best.map(|(_, text)| text)
        .unwrap_or_else(|| strip_html(&cleaned))
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
/// objects are read — publisher `Organization` blocks carry the newsroom's
/// own address, which says nothing about the story.
pub(crate) fn page_event_location(html: &str) -> EventLocation {
    for (tag, body) in tag_blocks(html, "script") {
        if !attr_value(tag, "type").is_some_and(|t| t.eq_ignore_ascii_case("application/ld+json")) {
            continue;
        }
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
/// `<p class="polizeimeldung" title="Ereignisort">Friedrichshain-Kreuzberg</p>`.
pub(crate) fn page_district(html: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let pos = lower.find("title=\"ereignisort\"")?;
    let start = pos + html[pos..].find('>')? + 1;
    let end = start + html[start..].find('<')?;
    let district = decode_entities(html[start..end].trim());
    (!district.is_empty()).then_some(district)
}

/// Publish date from the page's metadata: Open Graph / article meta tags or
/// schema.org JSON-LD `datePublished`. Modification dates are ignored — an
/// updated old article is still an old event.
pub(crate) fn page_published_at(html: &str) -> Option<i64> {
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
        .find_map(|name| meta_property_value(html, name))
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

pub(crate) fn attr_value(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_lowercase();
    let needle = format!("{attr}=");
    let pos = lower.find(&needle)?;
    let rest = &tag[pos + needle.len()..];
    let mut chars = rest.chars();
    let quote = chars.next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value: String = chars.take_while(|c| *c != quote).collect();
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

pub(crate) fn link_rel_value(html: &str, rel_name: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find("<link") {
        let start = offset + pos;
        let Some(end_rel) = lower[start..].find('>') else {
            break;
        };
        let end = start + end_rel + 1;
        let tag = &html[start..end];
        let rel_matches = attr_value(tag, "rel")
            .map(|rel| {
                rel.split_whitespace()
                    .any(|part| part.eq_ignore_ascii_case(rel_name))
            })
            .unwrap_or(false);
        if rel_matches {
            if let Some(href) = attr_value(tag, "href") {
                return Some(href);
            }
        }
        offset = end;
    }
    None
}

pub(crate) fn meta_property_value(html: &str, prop_name: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find("<meta") {
        let start = offset + pos;
        let Some(end_rel) = lower[start..].find('>') else {
            break;
        };
        let end = start + end_rel + 1;
        let tag = &html[start..end];
        let prop_matches = attr_value(tag, "property")
            .or_else(|| attr_value(tag, "name"))
            .map(|prop| prop.eq_ignore_ascii_case(prop_name))
            .unwrap_or(false);
        if prop_matches {
            if let Some(content) = attr_value(tag, "content") {
                return Some(content);
            }
        }
        offset = end;
    }
    None
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

pub(crate) fn external_href_from_google_html(html: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find("<a") {
        let start = offset + pos;
        let Some(end_rel) = lower[start..].find('>') else {
            break;
        };
        let end = start + end_rel + 1;
        let tag = &html[start..end];
        if let Some(href) = attr_value(tag, "href") {
            let href = decode_entities(&href);
            let candidate = decode_url_parameter(&href, &["url", "q"]).unwrap_or(href);
            if (candidate.starts_with("http://") || candidate.starts_with("https://"))
                && !looks_like_google_url(&candidate)
                && !looks_like_google_url(&candidate)
            {
                return Some(candidate);
            }
        }
        offset = end;
    }
    None
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

pub(crate) fn resolve_article_link(
    feed_url: &str,
    html: &str,
    article_text: &str,
    title: &str,
) -> (Option<String>, Option<String>) {
    let canonical = link_rel_value(html, "canonical")
        .or_else(|| meta_property_value(html, "og:url"))
        .filter(|url| url.starts_with("http://") || url.starts_with("https://"));

    let mut link = canonical.unwrap_or_else(|| feed_url.to_owned());
    if looks_like_google_url(&link) || looks_like_google_url(feed_url) {
        if let Some(external) = external_href_from_google_html(html) {
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

/// Fetch a URL and return the article body plus resolved link metadata.
pub(crate) async fn fetch_article(
    http: &reqwest::Client,
    url: &str,
    title: &str,
) -> Option<ArticleFetch> {
    let resp = tokio::time::timeout(Duration::from_secs(10), http.get(url).send())
        .await
        .ok()?
        .ok()?;

    let html = tokio::time::timeout(Duration::from_secs(10), resp.text())
        .await
        .ok()?
        .ok()?;

    // extract_article_body calls remove_blocks which is CPU-intensive on large HTML pages.
    // Run it on the blocking thread pool so it doesn't stall the async runtime.
    let title = title.to_owned();
    let feed_url = url.to_owned();
    let text = tokio::task::spawn_blocking(move || {
        let t = extract_article_body(&html);
        let t = t.trim().to_owned();
        if t.is_empty() {
            None
        } else {
            let (resolved_link, link_note) = resolve_article_link(&feed_url, &html, &t, &title);
            Some(ArticleFetch {
                text: t,
                resolved_link,
                link_note,
                published_at: page_published_at(&html),
                event: page_event_location(&html),
                district: page_district(&html),
            })
        }
    })
    .await
    .ok()??;

    Some(text)
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
            {"@type":"MusicEvent","name":"Konzert","location":{"@type":"Place","name":"Cassiopeia",
             "address":{"@type":"PostalAddress","streetAddress":"Revaler Str. 99","postalCode":"10245","addressLocality":"Berlin"},
             "geo":{"@type":"GeoCoordinates","latitude":"52.5075","longitude":13.4539}}}]}</script>"#;

        let event = page_event_location(html);

        assert_eq!(event.coords, Some((52.5075, 13.4539)));
        assert_eq!(
            event.address.as_deref(),
            Some("Revaler Str. 99 10245 Berlin")
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
        let html = r#"<p class="polizeimeldung">Polizeimeldung vom 26.09.2026</p><p class="polizeimeldung" title="Ereignisort">Friedrichshain-Kreuzberg</p>"#;

        assert_eq!(
            page_district(html).as_deref(),
            Some("Friedrichshain-Kreuzberg")
        );
        assert_eq!(page_district("<p>kein Tag</p>"), None);
    }
}
