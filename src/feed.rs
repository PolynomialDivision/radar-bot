//! Feed items and the RSS/Atom/JSON feed parser.

// ── Feed item ─────────────────────────────────────────────────────────────────

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct FeedItem {
    pub(crate) guid: String,
    pub(crate) title: String,
    pub(crate) link: Option<String>,
    /// Optional user-facing note about the link, e.g. rolling/live ticker pages.
    #[serde(default)]
    pub(crate) link_note: Option<String>,
    /// Short snippet from the RSS/social feed: shown to the user.
    pub(crate) description: Option<String>,
    /// Full stripped text used only for filtering/geocoding. Pre-filled by
    /// adapters that already have the full text (e.g. Bluesky posts).
    #[serde(default)]
    pub(crate) article_text: Option<String>,
    pub(crate) source_name: String,
    #[serde(default)]
    pub(crate) score: i32,
    /// Sum of all possible area group scores (from config).
    #[serde(default)]
    pub(crate) max_score: i32,
    /// Distance in metres from the reference point (if geocoded).
    #[serde(default)]
    pub(crate) distance_meters: Option<f64>,
    /// Human-readable place that produced the distance/fallback score.
    #[serde(default)]
    pub(crate) location_label: Option<String>,
    /// Unix timestamp from <pubDate> / <published> / <updated>. None if missing or unparseable.
    #[serde(default)]
    pub(crate) published_at: Option<i64>,
    /// Coordinates the source itself attaches (GeoRSS in the feed, schema.org
    /// Event geo on the page), as (lat, lon).
    #[serde(default)]
    pub(crate) coords: Option<(f64, f64)>,
    /// Street address of an event venue declared on the page (schema.org Event).
    #[serde(default)]
    pub(crate) venue: Option<String>,
    /// District the source files the item under, e.g. the "Ereignisort" of a
    /// Polizei Berlin report.
    #[serde(default)]
    pub(crate) district: Option<String>,
    /// From a source without location filter (`filter = false`): delivered whatever its
    /// score, never dropped by the digest threshold.
    #[serde(default)]
    pub(crate) deliver_always: bool,
}

// ── HTML utilities ────────────────────────────────────────────────────────────

pub(crate) fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Strip HTML tags and decode entities from RSS descriptions.
pub(crate) fn strip_html(html: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    let out = decode_entities(&out);
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn decode_entities(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        if let Some(semi) = rest.find(';') {
            let entity = &rest[1..semi]; // between & and ;
            let replaced = if let Some(hex) = entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
            {
                u32::from_str_radix(hex, 16)
                    .ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
            } else if let Some(dec) = entity.strip_prefix('#') {
                dec.parse::<u32>()
                    .ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
            } else {
                match entity {
                    "amp" => Some("&".into()),
                    "lt" => Some("<".into()),
                    "gt" => Some(">".into()),
                    "quot" => Some("\"".into()),
                    "apos" => Some("'".into()),
                    "nbsp" => Some(" ".into()),
                    "shy" => Some("".into()),
                    // dashes & ellipsis
                    "mdash" => Some("—".into()),
                    "ndash" => Some("–".into()),
                    "hellip" => Some("…".into()),
                    // typographic quotes
                    "ldquo" => Some("\u{201C}".into()),
                    "rdquo" => Some("\u{201D}".into()),
                    "lsquo" => Some("\u{2018}".into()),
                    "rsquo" => Some("\u{2019}".into()),
                    "laquo" => Some("«".into()),
                    "raquo" => Some("»".into()),
                    // German-specific
                    "auml" => Some("ä".into()),
                    "ouml" => Some("ö".into()),
                    "uuml" => Some("ü".into()),
                    "Auml" => Some("Ä".into()),
                    "Ouml" => Some("Ö".into()),
                    "Uuml" => Some("Ü".into()),
                    "szlig" => Some("ß".into()),
                    // other common
                    "eacute" => Some("é".into()),
                    "egrave" => Some("è".into()),
                    "ecirc" => Some("ê".into()),
                    "euro" => Some("€".into()),
                    "pound" => Some("£".into()),
                    "copy" => Some("©".into()),
                    "reg" => Some("®".into()),
                    "trade" => Some("™".into()),
                    "bull" => Some("•".into()),
                    "middot" => Some("·".into()),
                    _ => None,
                }
            };
            if let Some(r) = replaced {
                out.push_str(&r);
                rest = &rest[semi + 1..];
            } else {
                // unknown entity: emit as-is
                out.push('&');
                rest = &rest[1..];
            }
        } else {
            // no closing semicolon: emit literally
            out.push('&');
            rest = &rest[1..];
        }
    }
    out.push_str(rest);
    out
}

// ── RSS / Atom / JSON Feed parser ─────────────────────────────────────────────

/// The items of a feed; a feed the parser cannot read is logged and yields none.
pub(crate) fn parse_feed(xml: &str, source_name: &str) -> Vec<FeedItem> {
    match parse_feed_entries(xml, source_name) {
        Ok(items) if !items.is_empty() => items,
        Ok(_) => {
            warn!("Feed parser returned no entries for '{source_name}'");
            Vec::new()
        }
        Err(e) => {
            warn!("Feed parser failed for '{source_name}': {e}");
            Vec::new()
        }
    }
}

/// The items of a feed, or why it is not one.
pub(crate) fn parse_feed_entries(xml: &str, source_name: &str) -> Result<Vec<FeedItem>, String> {
    let feed = feed_rs::parser::parse(xml.as_bytes()).map_err(|e| e.to_string())?;
    let coords = feed_item_coords(xml);
    Ok(feed
        .entries
        .into_iter()
        .filter_map(|entry| normalize_feed_entry(entry, source_name))
        .map(|mut item| {
            item.coords = coords.get(&item.guid).copied().or_else(|| {
                item.link
                    .as_ref()
                    .and_then(|link| coords.get(link))
                    .copied()
            });
            item
        })
        .collect())
}

/// Feed text at least this long (characters) is the article itself: its page is not fetched.
pub(crate) const FULL_TEXT_CHARS: usize = 600;

pub(crate) fn normalize_feed_entry(
    entry: feed_rs::model::Entry,
    source_name: &str,
) -> Option<FeedItem> {
    let title = entry
        .title
        .as_ref()
        .map(|t| text_to_plain(&t.content))
        .filter(|t| !t.is_empty())
        .or_else(|| {
            entry
                .summary
                .as_ref()
                .map(|s| {
                    text_to_plain(&s.content)
                        .chars()
                        .take(120)
                        .collect::<String>()
                })
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            entry
                .content
                .as_ref()
                .and_then(|c| c.body.as_ref())
                .map(|body| text_to_plain(body).chars().take(120).collect::<String>())
                .filter(|s| !s.is_empty())
        })?;

    let link = preferred_entry_link(&entry);
    let description = entry
        .summary
        .as_ref()
        .map(|s| text_to_plain(&s.content))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            entry
                .content
                .as_ref()
                .and_then(|c| c.body.as_ref())
                .map(|body| text_to_plain(body))
                .filter(|s| !s.is_empty())
        });

    let article_text = entry
        .content
        .as_ref()
        .and_then(|c| c.body.as_deref())
        .map(text_to_plain)
        .filter(|text| text.chars().count() >= FULL_TEXT_CHARS);

    let guid = if entry.id.trim().is_empty() {
        link.clone()
            .unwrap_or_else(|| format!("{source_name}::{title}"))
    } else {
        entry.id
    };

    let published_at = entry.published.or(entry.updated).map(|dt| dt.timestamp());

    Some(FeedItem {
        guid,
        title,
        link,
        link_note: None,
        description,
        article_text,
        source_name: source_name.to_owned(),
        score: 0,
        max_score: 0,
        distance_meters: None,
        location_label: None,
        published_at,
        ..Default::default()
    })
}

/// Per-item coordinates from GeoRSS (`<georss:point>lat lon</georss:point>`)
/// or W3C geo (`<geo:lat>`/`<geo:long>`), keyed by the item's guid/id and
/// link. feed-rs does not expose these extensions.
pub(crate) fn feed_item_coords(xml: &str) -> HashMap<String, (f64, f64)> {
    let mut map = HashMap::new();
    for tag in ["item", "entry"] {
        let open = format!("<{tag}");
        let close = format!("</{tag}>");
        for block in xml.split(&open).skip(1) {
            // Skip e.g. "<itemizedlist": the tag name must end here.
            if !block.starts_with(['>', ' ', '\n', '\r', '\t']) {
                continue;
            }
            let item = &block[..block.find(&close).unwrap_or(block.len())];
            let point = xml_text(item, "georss:point").and_then(|p| {
                let mut parts = p.split_whitespace().map(str::parse::<f64>);
                Some((parts.next()?.ok()?, parts.next()?.ok()?))
            });
            let point = point.or_else(|| {
                let lat = xml_text(item, "geo:lat")?.parse().ok()?;
                let lon = xml_text(item, "geo:long")?.parse().ok()?;
                Some((lat, lon))
            });
            let Some(point) = point else { continue };
            let keys = [
                xml_text(item, "guid"),
                xml_text(item, "id"),
                xml_text(item, "link"),
                xml_attr_of(item, "link", "href"),
            ];
            for key in keys.into_iter().flatten() {
                map.insert(key, point);
            }
        }
    }
    map
}

/// Trimmed text content of the first `<tag …>…</tag>` in `xml`, CDATA unwrapped.
pub(crate) fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut from = 0;
    while let Some(pos) = xml[from..].find(&open) {
        let start = from + pos + open.len();
        from = start;
        if !xml[start..].starts_with(['>', ' ']) {
            continue; // "<link" must not match "<linkx"
        }
        let content_start = start + xml[start..].find('>')? + 1;
        if xml[..content_start].ends_with("/>") {
            continue;
        }
        let end = content_start + xml[content_start..].find(&close)?;
        let text = xml[content_start..end].trim();
        let text = text
            .strip_prefix("<![CDATA[")
            .and_then(|t| t.strip_suffix("]]>"))
            .unwrap_or(text)
            .trim();
        return (!text.is_empty()).then(|| decode_entities(text));
    }
    None
}

fn xml_attr_of(xml: &str, tag: &str, attr: &str) -> Option<String> {
    let start = xml.find(&format!("<{tag} "))?;
    let end = start + xml[start..].find('>')?;
    tag_attribute(&xml[start..=end], attr).map(|v| decode_entities(&v))
}

/// The quoted value of `attr` in the opening tag `tag`, trimmed; `None` when empty.
fn tag_attribute(tag: &str, attr: &str) -> Option<String> {
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
    Some(value.trim().to_owned()).filter(|v| !v.is_empty())
}

pub(crate) fn preferred_entry_link(entry: &feed_rs::model::Entry) -> Option<String> {
    entry
        .links
        .iter()
        .find(|link| link.rel.as_deref().is_none_or(|rel| rel == "alternate"))
        .or_else(|| entry.links.first())
        .map(|link| link.href.trim().to_owned())
        .filter(|href| !href.is_empty())
}

pub(crate) fn text_to_plain(s: &str) -> String {
    strip_html(s).trim().to_owned()
}

/// Parse RFC 2822 (RSS pubDate) or RFC 3339/ISO 8601 (Atom) date strings to Unix timestamp.
/// Returns None if the string is missing, empty, or unparseable. Callers treat None as "keep".
pub(crate) fn parse_feed_date(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // RFC 3339 / ISO 8601 (Atom: "2024-01-15T10:30:00Z", "2024-01-15T10:30:00+01:00")
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    // RFC 2822 (RSS 2.0: "Mon, 15 Jan 2024 10:30:00 +0000")
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(s) {
        return Some(dt.timestamp());
    }
    None
}

#[cfg(test)]
mod feed_tests {
    use super::*;

    #[test]
    fn parses_rss_with_namespaced_content() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Local</title>
    <link>https://example.test/</link>
    <description>Local feed</description>
    <item>
      <title><![CDATA[Incident &amp; update]]></title>
      <link>https://example.test/news/1</link>
      <guid isPermaLink="false">abc-1</guid>
      <pubDate>Mon, 15 Jan 2024 10:30:00 +0000</pubDate>
      <description><![CDATA[Short <b>summary</b>]]></description>
      <content:encoded><![CDATA[Full <strong>article</strong> body]]></content:encoded>
    </item>
  </channel>
</rss>"#;

        let items = parse_feed(xml, "Test RSS");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].guid, "abc-1");
        assert_eq!(items[0].title, "Incident & update");
        assert_eq!(
            items[0].link.as_deref(),
            Some("https://example.test/news/1")
        );
        assert_eq!(items[0].description.as_deref(), Some("Short summary"));
        assert_eq!(items[0].published_at, Some(1_705_314_600));
    }

    #[test]
    fn parses_atom_link_href_and_updated_date() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Feed</title>
  <id>feed-id</id>
  <updated>2024-01-15T10:00:00Z</updated>
  <entry>
    <title>Atom entry</title>
    <id>tag:example.test,2024:entry-1</id>
    <updated>2024-01-15T11:30:00+01:00</updated>
    <link rel="alternate" href="https://example.test/atom/1" />
    <summary type="html">Atom &lt;b&gt;summary&lt;/b&gt;</summary>
  </entry>
</feed>"#;

        let items = parse_feed(xml, "Test Atom");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].guid, "tag:example.test,2024:entry-1");
        assert_eq!(items[0].title, "Atom entry");
        assert_eq!(
            items[0].link.as_deref(),
            Some("https://example.test/atom/1")
        );
        assert_eq!(items[0].description.as_deref(), Some("Atom summary"));
        assert_eq!(items[0].published_at, Some(1_705_314_600));
    }

    #[test]
    fn georss_and_w3c_geo_coordinates_are_attached_to_items() {
        let xml = r#"<?xml version="1.0"?>
<rss version="2.0" xmlns:georss="http://www.georss.org/georss" xmlns:geo="http://www.w3.org/2003/01/geo/wgs84_pos#">
  <channel><title>T</title><link>https://t.test/</link><description>d</description>
    <item><title>A</title><link>https://t.test/a</link><guid isPermaLink="false">a-1</guid>
      <georss:point>52.4736 13.4205</georss:point></item>
    <item><title>B</title><link>https://t.test/b</link>
      <geo:lat>52.50</geo:lat><geo:long>13.40</geo:long></item>
    <item><title>C</title><link>https://t.test/c</link></item>
  </channel>
</rss>"#;

        let items = parse_feed(xml, "Geo");

        assert_eq!(items[0].coords, Some((52.4736, 13.4205)));
        assert_eq!(items[1].coords, Some((52.50, 13.40)));
        assert_eq!(items[2].coords, None);
    }

    #[test]
    fn the_feed_fixtures_parse_as_today() {
        let feed = |path: &str, name: &str| parse_feed(&crate::testing::fixture(path), name);

        let police = feed("feeds/police_title_only.xml", "Local Police");
        assert_eq!(police.len(), 4);
        assert!(police.iter().all(|i| i.description.is_none()));
        assert_eq!(police[0].guid, "police-1001");

        let broadcaster = feed(
            "feeds/broadcaster_full_text.xml",
            "Local Public Broadcaster",
        );
        assert_eq!(broadcaster.len(), 3);
        // The teaser stays the description; full text of 600 characters or more is the
        // article (its page is not fetched), shorter text is not.
        assert_eq!(
            broadcaster[0].description.as_deref(),
            Some("In der Perleberger Straße hat es in der Nacht gebrannt.")
        );
        assert!(broadcaster[0]
            .article_text
            .as_deref()
            .is_some_and(|t| t.starts_with("In der Perleberger Straße in Yourcity-YourDistrict")));
        assert!(broadcaster[1..].iter().all(|i| i.article_text.is_none()));

        let national = feed("feeds/national_atom.xml", "National News");
        assert_eq!(national.len(), 3);
        assert_eq!(
            national[2].link.as_deref(),
            Some("https://example.com/national/konzert")
        );

        let local = feed("feeds/neighbourhood_georss.xml", "Neighbourhood Blog");
        let coords: Vec<_> = local.iter().map(|i| i.coords).collect();
        assert_eq!(
            coords,
            vec![Some((52.4740, 13.4210)), Some((52.4800, 13.4300)), None]
        );

        let google = feed("feeds/google_news.xml", "Google News");
        assert_eq!(google.len(), 2);
        assert!(google[0]
            .link
            .as_deref()
            .is_some_and(|l| l.starts_with("https://news.google.com/rss/articles/")));
    }
}
