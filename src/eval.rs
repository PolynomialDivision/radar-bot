//! The labelled corpus (`eval/corpus.jsonl`) through today's scoring, offline: streets come
//! from `eval/geography.json` instead of Nominatim. Prints how far the result is from the
//! expected ring; categories are listed but not assigned before the classifier exists.
//! No floor yet (`cargo test eval_corpus -- --nocapture` shows the table).

use crate::polling::PollCtx;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use serde::Deserialize;

use crate::{
    config::{AreaGroup, FilterConfig, PlaceConfig, SourceConfig},
    feed::FeedItem,
    fetch::Http,
    geo::{
        extract_street_evidence, street_name_matches, street_without_house_number, GeocodeCache,
        GeocodeHit,
    },
    polling::{process_item, ProcessOutcome},
    sources::SourceType,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub(crate) id: String,
    pub(crate) source_scope: String,
    pub(crate) social: bool,
    pub(crate) title: String,
    pub(crate) teaser: String,
    pub(crate) body: String,
    pub(crate) ring: i32,
    pub(crate) category: String,
    pub(crate) note: String,
}

#[derive(Deserialize)]
struct Geography {
    reference: (f64, f64),
    city: String,
    home_borough: String,
    areas: Vec<AreaGroup>,
    places: Vec<PlaceConfig>,
    elsewhere: Vec<String>,
    streets: HashMap<String, Vec<Segment>>,
}

#[derive(Deserialize)]
struct Segment {
    suburb: String,
    borough: String,
    path: Vec<(f64, f64)>,
}

fn eval_file(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("eval")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

pub(crate) fn corpus() -> Vec<Entry> {
    eval_file("corpus.jsonl")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("corpus line {l}: {e}")))
        .collect()
}

fn geography() -> Geography {
    serde_json::from_str(&eval_file("geography.json")).expect("eval/geography.json")
}

/// Today's source settings for a scope: `local` as the narrow social searches, `city` as the
/// city's police and broadcaster, `wide` as national feeds behind the city keyword.
fn source(entry: &Entry) -> SourceConfig {
    let (required, base) = match entry.source_scope.as_str() {
        "local" => (Some(vec![]), Some(1_500.0)),
        "city" => (Some(vec![]), Some(9_000.0)),
        "wide" => (None, None),
        other => panic!("{}: unknown source_scope {other}", entry.id),
    };
    SourceConfig {
        name: format!("eval {}", entry.source_scope),
        source_type: if entry.social {
            SourceType::Bluesky
        } else {
            SourceType::Rss
        },
        url: None,
        query: None,
        limit: None,
        max_age_hours: None,
        filter: true,
        base_implied_meters: base,
        required,
        stale_after_days: None,
    }
}

fn item(entry: &Entry) -> FeedItem {
    let (description, article_text) = if entry.social {
        (Some(entry.title.clone()), Some(entry.title.clone()))
    } else {
        (
            Some(entry.teaser.clone()).filter(|t| !t.is_empty()),
            Some(entry.body.clone()).filter(|t| !t.is_empty()),
        )
    };
    FeedItem {
        guid: entry.id.clone(),
        title: entry.title.lines().next().unwrap_or_default().to_owned(),
        description,
        article_text,
        source_name: format!("eval {}", entry.source_scope),
        published_at: Some(chrono::Utc::now().timestamp()),
        ..Default::default()
    }
}

/// Every street candidate of `item` answered from the geography, as Nominatim would.
fn geocode_cache(item: &FeedItem, geo: &Geography) -> GeocodeCache {
    let cache: GeocodeCache = Arc::new(DashMap::new());
    let candidates = extract_street_evidence(
        &item.title,
        item.description.as_deref(),
        item.article_text.as_deref(),
    );
    for candidate in candidates {
        let street = street_without_house_number(&candidate.candidate);
        let hits = geo
            .streets
            .iter()
            .filter(|(name, _)| street_name_matches(street, name))
            .flat_map(|(name, segments)| {
                segments.iter().map(move |s| GeocodeHit {
                    lat: s.path[0].0,
                    lon: s.path[0].1,
                    display_name: Some(format!("{name}, {}, {}", s.suburb, s.borough)),
                    suburb: Some(s.suburb.clone()),
                    borough: Some(s.borough.clone()),
                    shape: vec![s.path.clone()],
                })
            })
            .collect();
        cache.insert(candidate.candidate, hits);
    }
    cache
}

/// The ring today's code gives each entry.
async fn rings_today() -> Vec<(Entry, i32)> {
    let geo = geography();
    let filter = Arc::new(FilterConfig {
        required: vec![geo.city.clone()],
        digest_threshold: 3,
        candidate_min_score: 1,
        area: geo.areas.clone(),
        places: geo.places.clone(),
        elsewhere: geo.elsewhere.clone(),
        home_borough: Some(geo.home_borough.clone()),
        geocode_city: Some(format!("{}, Germany", geo.city)),
        ..Default::default()
    });
    let mut out = Vec::new();
    for entry in corpus() {
        let item = item(&entry);
        let mut ctx = PollCtx::new(Http::offline(), (*filter).clone(), Some(geo.reference));
        ctx.geocoder.cache = geocode_cache(&item, &geo);
        let outcome = process_item(ctx, source(&entry), item, true).await;
        let ring = match outcome {
            ProcessOutcome::Queued(item)
            | ProcessOutcome::Candidate { item, .. }
            | ProcessOutcome::Dropped { item, .. }
            | ProcessOutcome::Retry { item, .. } => item.score,
        };
        out.push((entry, ring));
    }
    out
}

#[tokio::test]
async fn eval_corpus() {
    let results = rings_today().await;
    assert!(results.len() >= 30, "the corpus has at least 30 entries");

    let exact = results.iter().filter(|(e, r)| *r == e.ring).count();
    let within_one = results
        .iter()
        .filter(|(e, r)| (r - e.ring).abs() <= 1)
        .count();
    println!(
        "{:<22} {:>8} {:>5}  {:<10} note",
        "id", "expected", "today", "category"
    );
    for (entry, ring) in &results {
        let mark = if *ring == entry.ring { " " } else { "✗" };
        println!(
            "{:<22} {:>8} {:>5}{mark} {:<10} {}",
            entry.id, entry.ring, ring, entry.category, entry.note
        );
    }
    let n = results.len();
    println!(
        "ring exact {exact}/{n} ({:.2}), within one {within_one}/{n} ({:.2}); category: not assigned yet",
        exact as f64 / n as f64,
        within_one as f64 / n as f64
    );
}
