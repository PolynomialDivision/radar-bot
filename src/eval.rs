//! The labelled corpus (`eval/corpus.jsonl`) through today's scoring and the new decision,
//! offline: streets come from `eval/geography.json` instead of Nominatim. Holds floors on the
//! precision and recall of "delivered" (expected class `urgent` or `briefing`), on category
//! accuracy and on the ring within one (`cargo test eval_corpus -- --nocapture` shows the
//! table).

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
    /// One of the nine categories, or "noise".
    pub(crate) category: String,
    /// `urgent`, `briefing`, `archive` or `drop`.
    pub(crate) class: String,
    /// Entries with the same label must end in one story (S3).
    #[serde(default)]
    pub(crate) story: Option<String>,
    /// From an official source (police, fire brigade, district office).
    #[serde(default)]
    pub(crate) official: bool,
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
        id: None,
        scope: crate::model::Scope::parse(&entry.source_scope),
        official: entry.official,
        category: None,
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

/// What the code gives an entry: today's ring, and the new category, noise and class.
struct Result_ {
    ring: i32,
    category: String,
    class: String,
}

/// Today's ring and the shadow decision for each entry.
async fn rings_today() -> Vec<(Entry, Result_)> {
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
        let source = source(&entry);
        let outcome = process_item(ctx, source.clone(), item, true).await;
        let item = match outcome {
            ProcessOutcome::Queued(item)
            | ProcessOutcome::Candidate { item, .. }
            | ProcessOutcome::Dropped { item, .. }
            | ProcessOutcome::Retry { item, .. } => item,
        };
        let now = chrono::Utc::now().timestamp();
        let observation = crate::pipeline::observation(&source, &item, "fetch", now);
        let category = if observation.noise.is_some() {
            "noise".to_owned()
        } else {
            observation.category
        };
        out.push((
            entry,
            Result_ {
                ring: item.score,
                category,
                class: observation.class,
            },
        ));
    }
    out
}

/// The floors: measured minus 0.03 (round 2, S2: precision 0.89, recall 0.94, category 0.89,
/// ring within one 0.97 on 62 entries).
const PRECISION_FLOOR: f64 = 0.86;
const RECALL_FLOOR: f64 = 0.91;
const CATEGORY_FLOOR: f64 = 0.86;
const RING_FLOOR: f64 = 0.94;

fn delivered(class: &str) -> bool {
    matches!(class, "urgent" | "briefing")
}

#[tokio::test]
async fn eval_corpus() {
    let results = rings_today().await;
    assert!(results.len() >= 60, "the corpus has at least 60 entries");

    println!(
        "{:<22} {:>4} {:>4}  {:<10} {:<10}  {:<9} {:<9} note",
        "id", "ring", "got", "category", "got", "class", "got"
    );
    for (entry, got) in &results {
        let mark = |ok: bool| if ok { " " } else { "✗" };
        println!(
            "{:<22} {:>4} {:>4}{} {:<10} {:<10}{} {:<9} {:<9}{} {}",
            entry.id,
            entry.ring,
            got.ring,
            mark((got.ring - entry.ring).abs() <= 1),
            entry.category,
            got.category,
            mark(got.category == entry.category),
            entry.class,
            got.class,
            mark(delivered(&got.class) == delivered(&entry.class)),
            entry
                .story
                .as_deref()
                .map_or(entry.note.clone(), |story| format!(
                    "[{story}] {}",
                    entry.note
                ))
        );
    }
    let n = results.len() as f64;
    let count =
        |f: &dyn Fn(&(Entry, Result_)) -> bool| results.iter().filter(|r| f(r)).count() as f64;
    let true_positive = count(&|(e, g)| delivered(&e.class) && delivered(&g.class));
    let precision = true_positive / count(&|(_, g)| delivered(&g.class)).max(1.0);
    let recall = true_positive / count(&|(e, _)| delivered(&e.class)).max(1.0);
    let category = count(&|(e, g)| e.category == g.category) / n;
    let ring = count(&|(e, g)| (g.ring - e.ring).abs() <= 1) / n;
    let exact_class = count(&|(e, g)| e.class == g.class) / n;
    println!(
        "delivered: precision {precision:.2}, recall {recall:.2}; category {category:.2}; \
         ring within one {ring:.2}; class exact {exact_class:.2} ({n} entries)"
    );
    assert!(precision >= PRECISION_FLOOR, "precision {precision:.2}");
    assert!(recall >= RECALL_FLOOR, "recall {recall:.2}");
    assert!(
        category >= CATEGORY_FLOOR,
        "category accuracy {category:.2}"
    );
    assert!(ring >= RING_FLOOR, "ring within one {ring:.2}");
}
