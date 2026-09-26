//! Feed items and the RSS/Atom/JSON feed parser.

use super::*;

// ── Feed item ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FeedItem {
    pub(crate) guid: String,
    pub(crate) title: String,
    pub(crate) link: Option<String>,
    /// Optional user-facing note about the link, e.g. rolling/live ticker pages.
    #[serde(default)]
    pub(crate) link_note: Option<String>,
    /// Short snippet from the RSS/social feed — shown to the user.
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
                // unknown entity — emit as-is
                out.push('&');
                rest = &rest[1..];
            }
        } else {
            // no closing semicolon — emit literally
            out.push('&');
            rest = &rest[1..];
        }
    }
    out.push_str(rest);
    out
}

// ── RSS / Atom / JSON Feed parser ─────────────────────────────────────────────

pub(crate) fn parse_feed(xml: &str, source_name: &str) -> Vec<FeedItem> {
    match feed_rs::parser::parse(xml.as_bytes()) {
        Ok(feed) => {
            let items: Vec<FeedItem> = feed
                .entries
                .into_iter()
                .filter_map(|entry| normalize_feed_entry(entry, source_name))
                .collect();
            if !items.is_empty() {
                return items;
            }
            warn!("Feed parser returned no entries for '{source_name}'");
        }
        Err(e) => {
            warn!("Feed parser failed for '{source_name}': {e}");
        }
    }

    Vec::new()
}

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
        article_text: None,
        source_name: source_name.to_owned(),
        score: 0,
        max_score: 0,
        distance_meters: None,
        location_label: None,
        published_at,
    })
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
/// Returns None if the string is missing, empty, or unparseable — callers treat None as "keep".
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
    fn digest_includes_location_with_distance() {
        let item = DigestItem {
            guids: vec!["1".to_owned()],
            source_name: "Local".to_owned(),
            source_count: 1,
            title: "Street closed".to_owned(),
            link: None,
            link_note: None,
            score: 4,
            max_score: 5,
            distance_meters: Some(350.0),
            location_label: Some("Boxhagener Platz".to_owned()),
        };

        let (plain, html) = format_digest(&[item], "Test");

        assert!(plain.contains("~350m · Boxhagener Platz"));
        assert!(html.contains("~350m · Boxhagener Platz"));
    }

    #[test]
    fn clustering_collapses_multi_source_same_incident_and_boosts_score() {
        let a = db::DbItem {
            guid: "a".to_owned(),
            source_name: "Source A".to_owned(),
            title: "Brand in der Rigaer Straße".to_owned(),
            link: Some("https://a.test/1".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(700.0),
            location_label: Some("Rigaer Straße".to_owned()),
        };
        let b = db::DbItem {
            guid: "b".to_owned(),
            source_name: "Source B".to_owned(),
            title: "Feuerwehreinsatz Rigaer Straße nach Brand".to_owned(),
            link: Some("https://b.test/2".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(730.0),
            location_label: Some("Rigaer Straße".to_owned()),
        };

        let clustered = cluster_digest_items(vec![a, b]);

        assert_eq!(clustered.len(), 1);
        assert_eq!(clustered[0].guids, vec!["a", "b"]);
        assert_eq!(clustered[0].source_count, 2);
        assert_eq!(clustered[0].score, 4);
        assert!(clustered[0].source_name.contains("Source A"));
        assert!(clustered[0].source_name.contains("Source B"));
    }

    #[test]
    fn clustering_collapses_translated_same_source_event_announcement() {
        let a = db::DbItem {
            guid: "de".to_owned(),
            source_name: "Bluesky Rigaer".to_owned(),
            title: "💥Ankündigung! Montag 06.07.2026💥".to_owned(),
            link: Some("https://bsky.app/profile/example/post/de".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(559.0),
            location_label: Some("Rigaer 94".to_owned()),
        };
        let b = db::DbItem {
            guid: "en".to_owned(),
            source_name: "Bluesky Rigaer".to_owned(),
            title: "💥Announcement! Monday 06.07.2026💥".to_owned(),
            link: Some("https://bsky.app/profile/example/post/en".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(559.0),
            location_label: Some("Rigaer 94".to_owned()),
        };

        let clustered = cluster_digest_items(vec![a, b]);

        assert_eq!(clustered.len(), 1);
        assert_eq!(clustered[0].guids, vec!["de", "en"]);
        assert_eq!(clustered[0].source_count, 1);
    }

    #[test]
    fn clustering_keeps_same_source_event_without_shared_date_separate() {
        let a = db::DbItem {
            guid: "a".to_owned(),
            source_name: "Bluesky Rigaer".to_owned(),
            title: "Ankündigung! Montag 06.07.2026".to_owned(),
            link: Some("https://bsky.app/profile/example/post/a".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(559.0),
            location_label: Some("Rigaer 94".to_owned()),
        };
        let b = db::DbItem {
            guid: "b".to_owned(),
            source_name: "Bluesky Rigaer".to_owned(),
            title: "Announcement! Monday 13.07.2026".to_owned(),
            link: Some("https://bsky.app/profile/example/post/b".to_owned()),
            link_note: None,
            score: 3,
            max_score: 5,
            distance_meters: Some(559.0),
            location_label: Some("Rigaer 94".to_owned()),
        };

        let clustered = cluster_digest_items(vec![a, b]);

        assert_eq!(clustered.len(), 2);
    }

    #[tokio::test]
    async fn candidate_clustering_promotes_multi_source_weak_items() {
        let path = std::env::temp_dir().join(format!(
            "radar-bot-candidate-test-{}-{}.db",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let db = Db::open(&path).expect("open temp db");

        let a = FeedItem {
            guid: "candidate-a".to_owned(),
            title: "Brand in der Rigaer Straße".to_owned(),
            link: Some("https://a.test/1".to_owned()),
            link_note: None,
            description: None,
            article_text: None,
            source_name: "Source A".to_owned(),
            score: 2,
            max_score: 5,
            distance_meters: Some(1500.0),
            location_label: Some("Rigaer Straße".to_owned()),
            published_at: None,
        };
        let mut b = a.clone();
        b.guid = "candidate-b".to_owned();
        b.source_name = "Source B".to_owned();
        b.title = "Feuerwehreinsatz Rigaer Straße nach Brand".to_owned();
        b.link = Some("https://b.test/2".to_owned());

        db.insert_candidate(&a, "score 2 below digest threshold 3")
            .await
            .expect("insert candidate a");
        db.insert_candidate(&b, "score 2 below digest threshold 3")
            .await
            .expect("insert candidate b");

        let promoted = promote_candidate_clusters(&db, 3, 2, 24)
            .await
            .expect("promote candidates");
        let items = db.take_for_digest(3).await.expect("take digest");

        assert_eq!(promoted, 2);
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|item| item.score >= 3));

        drop(db);
        let _ = std::fs::remove_file(path);
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
            "Brand in der Rigaer Straße",
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
            "Brand in der Rigaer Straße mit Feuerwehreinsatz",
            "Brand in der Rigaer Straße",
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
                    <p>Brand in der Rigaer Straße. Die Feuerwehr war am Abend im Einsatz.</p>
                    <p>Mehrere Menschen bemerkten Rauch in einem Wohnhaus nahe der Kreuzung.</p>
                    <p>Die Polizei sperrte den Bereich zeitweise ab und leitete den Verkehr um.</p>
                    <p>Weitere Details sollen im Laufe des Tages bekannt gegeben werden.</p>
                </div>
                <footer>Impressum Datenschutz Kontakt Newsletter</footer>
            </body></html>
        "#;

        let body = extract_article_body(html);

        assert!(body.contains("Brand in der Rigaer Straße"));
        assert!(!body.contains("Impressum Datenschutz"));
    }

    #[test]
    fn configured_place_distance_matches_alias() {
        let filter = FilterConfig {
            places: vec![PlaceConfig {
                name: "RAW-Gelände".to_owned(),
                lat: 52.5070,
                lon: 13.4540,
                aliases: vec!["RAW Gelände".to_owned(), "RAW".to_owned()],
            }],
            ..Default::default()
        };
        let item = FeedItem {
            guid: "place".to_owned(),
            title: "Feuerwehreinsatz am RAW".to_owned(),
            link: None,
            link_note: None,
            description: None,
            article_text: None,
            source_name: "Test".to_owned(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            location_label: None,
            published_at: None,
        };

        let hits = find_configured_places(&ItemText::of(&item), &filter, Some((52.5070, 13.4540)));

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].label, "RAW-Gelände");
        assert!(hits[0].prominent);
    }

    #[test]
    fn configured_place_alias_does_not_match_inside_words() {
        let filter = FilterConfig {
            places: vec![PlaceConfig {
                name: "RAW-Gelände".to_owned(),
                lat: 52.5070,
                lon: 13.4540,
                aliases: vec!["RAW Gelände".to_owned(), "RAW".to_owned()],
            }],
            ..Default::default()
        };
        let item = FeedItem {
            guid: "place".to_owned(),
            title: "The chaotic sprawl of pixels resolved".to_owned(),
            link: None,
            link_note: None,
            description: Some("No local venue is mentioned here.".to_owned()),
            article_text: None,
            source_name: "Test".to_owned(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            location_label: None,
            published_at: None,
        };

        let hits = find_configured_places(&ItemText::of(&item), &filter, Some((52.5070, 13.4540)));

        assert!(hits.is_empty());
    }

    #[test]
    fn keyword_area_matches_berlin_prefixed_district_name() {
        let filter = FilterConfig {
            area: vec![AreaGroup {
                implied_meters: 1500.0,
                terms: vec!["Friedrichshain".to_owned()],
            }],
            ..Default::default()
        };
        let item = FeedItem {
            guid: "district".to_owned(),
            title: "Einsatz in Berlin-Friedrichshain".to_owned(),
            link: None,
            link_note: None,
            description: None,
            article_text: None,
            source_name: "Test".to_owned(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            location_label: None,
            published_at: None,
        };

        let hit = keyword_check(&item, &filter, &[]).expect("keyword check should pass");

        assert_eq!(hit.evidence.len(), 1);
        assert_eq!(hit.evidence[0].meters, 1500.0);
        assert_eq!(hit.evidence[0].label, "Friedrichshain");
        assert!(hit.evidence[0].prominent);
        assert_eq!(hit.matched, vec!["\"Friedrichshain\" (1500m)"]);
    }

    #[test]
    fn street_evidence_ignores_boilerplate_after_impressum() {
        let evidence = extract_street_evidence(
            "Brand in der Rigaer Straße",
            None,
            Some("Weitere Details folgen. Impressum Kontakt Alexanderplatz 1 Datenschutz."),
        );
        let candidates: Vec<String> = evidence.into_iter().map(|e| e.candidate).collect();

        assert!(candidates.iter().any(|c| c.contains("Rigaer Straße")));
        assert!(!candidates.iter().any(|c| c.contains("Alexanderplatz")));
    }
}
