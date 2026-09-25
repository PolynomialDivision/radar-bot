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
                "berlin" | "polizei" | "meldung" | "update" | "heute"
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
            })
        }
    })
    .await
    .ok()??;

    Some(text)
}
