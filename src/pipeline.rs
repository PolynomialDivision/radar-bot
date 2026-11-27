//! The observation of every decided item (S2): written after `process_item` decided the item
//! as before, with its text, the evidence, the category and the class the new policy would
//! give it, and what the gate (D7) would have done with its page. Nothing here changes what is
//! posted: the digest still reads `items`.

use crate::classify::{classify, SourceFacts};
use crate::config::{FilterConfig, SourceConfig};
use crate::feed::FeedItem;
use crate::filter::{find_term, normalize, TermMatch};
use crate::geo::{best_evidence, EvidenceKind, LocationEvidence};
use crate::model::{Precision, RawItem, Scope};
use crate::policy::{decide, DecisionInput};
use crate::store::Observation;

/// What the gate of D7 (as amended in round 2) would do with the item's page: a `wide` item is
/// fetched only when its title or teaser names a configured area term or place (not the
/// city), a `city` item unless the feed has the full text, a `local` item never.
pub(crate) fn gate(source: &SourceConfig, item: &FeedItem, filter: &FilterConfig) -> &'static str {
    let fetch = match source.scope() {
        Scope::Local => false,
        Scope::City => item.article_text.is_none(),
        Scope::Wide => {
            let text = normalize(&format!(
                "{} {}",
                item.title,
                item.description.as_deref().unwrap_or("")
            ));
            let terms = filter.area.iter().flat_map(|group| group.terms.iter());
            let places = filter
                .places
                .iter()
                .flat_map(|place| std::iter::once(&place.name).chain(&place.aliases));
            item.article_text.is_none()
                && terms
                    .chain(places)
                    .any(|term| find_term(&text, &normalize(term), TermMatch::Place).is_some())
        }
    };
    if fetch {
        "fetch"
    } else {
        "skip"
    }
}

/// How precisely a piece of evidence locates.
fn precision(evidence: Option<&LocationEvidence>) -> Precision {
    match evidence.map(|e| e.kind) {
        Some(EvidenceKind::Coordinates) => Precision::Point,
        Some(EvidenceKind::Geocoded) => Precision::Street,
        Some(EvidenceKind::Place) => Precision::Place,
        Some(EvidenceKind::Keyword) => Precision::Area,
        Some(EvidenceKind::SourceDefault) => Precision::City,
        None => Precision::None,
    }
}

/// The normalized name of the street or place the best evidence points at.
fn place_key(evidence: Option<&LocationEvidence>) -> Option<String> {
    let evidence =
        evidence.filter(|e| matches!(e.kind, EvidenceKind::Geocoded | EvidenceKind::Place))?;
    let name = evidence.label.split(',').next()?.trim();
    let name = crate::geo::street_without_house_number(name);
    (!name.is_empty()).then(|| normalize(name))
}

fn evidence_json(evidence: &[LocationEvidence]) -> String {
    let list: Vec<serde_json::Value> = evidence
        .iter()
        .map(|e| {
            serde_json::json!({
                "kind": format!("{:?}", e.kind),
                "label": e.label,
                "meters": e.meters,
                "prominent": e.prominent,
                "penalty": e.penalty,
                "score": e.score(),
            })
        })
        .collect();
    serde_json::Value::Array(list).to_string()
}

/// The observation of `item` of `source`, decided `now`.
pub(crate) fn observation(
    source: &SourceConfig,
    item: &FeedItem,
    gate: &'static str,
    now: i64,
) -> Observation {
    let social = source.social();
    let raw = RawItem::from(item);
    let teaser = raw.teaser.clone().unwrap_or_default();
    let body = raw.text.clone().unwrap_or_default();
    let facts = SourceFacts {
        scope: source.scope(),
        social,
        category: source.category,
    };
    let (category, noise) = classify(&raw.title, &teaser, &body, &facts);
    let best = best_evidence(item.evidence.clone());
    let precision = precision(best.as_ref());
    let input = DecisionInput {
        category,
        noise,
        ring: item.score,
        precision,
        scope: facts.scope,
        official: source.official,
        social,
        // Non-social publishers of a single observation.
        publishers: u32::from(!social),
        happened_at: item.published_at,
        first_seen_at: now,
        now,
        title: &item.title,
    };
    let (class, mut record) = decide(&input);
    record.gate = Some(gate);
    Observation {
        source_id: source.id(),
        ext_id: raw.ext_id,
        first_seen_at: now,
        url: raw.url,
        linked_url: raw.linked_url,
        social,
        title: raw.title,
        teaser: raw.teaser,
        body: raw.text,
        published_at: raw.published_at,
        category: category.as_str().to_owned(),
        noise: noise.map(|n| n.as_str().to_owned()),
        lat: item.coords.map(|c| c.0),
        lon: item.coords.map(|c| c.1),
        distance_m: item.distance_meters,
        precision: precision.as_str().to_owned(),
        place_key: place_key(best.as_ref()),
        ring: item.score,
        label: item.location_label.clone(),
        evidence: evidence_json(&item.evidence),
        class: class.as_str().to_owned(),
        decision: serde_json::to_string(&record).unwrap_or_else(|_| "{}".to_owned()),
    }
}
