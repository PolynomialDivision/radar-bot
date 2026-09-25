//! Keyword filtering.

use super::*;

// ── Filtering ─────────────────────────────────────────────────────────────────

pub(crate) fn normalize(s: &str) -> String {
    s.to_lowercase()
        .replace('ß', "ss")
        // street abbreviations: "str." and "str " both normalize to "strasse"
        // so "Examplestr.", "Examplestrasse", "Examplestraße" all match
        .replace("strasse", "str")
        .replace("straße", "str")
        .replace("str.", "str")
}

/// (closest matching area as (implied_meters, term), matched terms).
pub(crate) type KeywordMatch = (Option<(f64, String)>, Vec<String>);

/// Check keywords for blocklist/required/area matches.
/// Returns `None` if the item should be dropped.
/// Returns `Some((implied_meters, matched_terms))` where `implied_meters` is the
/// closest matching area group's distance, or `None` if no area groups are configured.
pub(crate) fn keyword_check(
    item: &FeedItem,
    filter: &FilterConfig,
    required: &[String],
) -> Option<KeywordMatch> {
    let text = normalize(&format!(
        "{} {} {}",
        item.title,
        item.description.as_deref().unwrap_or(""),
        item.article_text.as_deref().unwrap_or(""),
    ));

    for b in &filter.blocklist {
        if text.contains(&normalize(b)) {
            return None;
        }
    }

    if !required.is_empty() && !required.iter().any(|r| text.contains(&normalize(r))) {
        return None;
    }

    if filter.area.is_empty() {
        return Some((None, vec![]));
    }

    // Find the group with the smallest implied_meters (= closest = highest score)
    let mut best: Option<(f64, String)> = None;
    let mut matched: Vec<String> = Vec::new();

    for group in &filter.area {
        if let Some(term) = group.terms.iter().find(|t| text.contains(&normalize(t))) {
            matched.push(format!("\"{}\" ({}m)", term, group.implied_meters as u32));
            if best.as_ref().is_none_or(|(d, _)| group.implied_meters < *d) {
                best = Some((group.implied_meters, term.clone()));
            }
        }
    }

    // No area match is not a hard drop — caller can fall back to source.base_implied_meters.
    Some((best, matched))
}

// ── Seen-items store (append-only file) ───────────────────────────────────────
