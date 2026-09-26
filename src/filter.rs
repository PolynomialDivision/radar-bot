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

/// Article text beyond this many bytes (of the boilerplate-trimmed body) no
/// longer counts as the lead. Local incidents name their place in the first
/// sentences; mentions further down are usually asides ("…also planned at the
/// Warschauer Brücke") or teasers that slipped past extraction.
pub(crate) const ARTICLE_LEAD_BYTES: usize = 1_200;

/// Normalized item text split by prominence: title, feed description and the
/// article lead versus the remaining article body.
pub(crate) struct ItemText {
    pub(crate) prominent: String,
    pub(crate) rest: String,
}

impl ItemText {
    pub(crate) fn of(item: &FeedItem) -> Self {
        let (lead, rest) = split_lead(first_relevant_article_text(item.article_text.as_deref()));
        ItemText {
            prominent: normalize(&format!(
                "{} {} {}",
                item.title,
                item.description.as_deref().unwrap_or(""),
                lead
            )),
            rest: normalize(rest),
        }
    }

    pub(crate) fn contains(&self, term: &str) -> bool {
        self.mention(term, TermMatch::Keyword).is_some()
    }

    /// `Some(true)` for a prominent mention, `Some(false)` for one only in
    /// the article body beyond the lead.
    pub(crate) fn mention(&self, term: &str, mode: TermMatch) -> Option<bool> {
        let term = normalize(term);
        if find_term(&self.prominent, &term, mode).is_some() {
            Some(true)
        } else if find_term(&self.rest, &term, mode).is_some() {
            Some(false)
        } else {
            None
        }
    }
}

/// Split article text into the lead (about `ARTICLE_LEAD_BYTES`, ended at a
/// word boundary) and the rest.
pub(crate) fn split_lead(text: &str) -> (&str, &str) {
    if text.len() <= ARTICLE_LEAD_BYTES {
        return (text, "");
    }
    let mut cut = ARTICLE_LEAD_BYTES;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    let cut = text[cut..]
        .find(char::is_whitespace)
        .map_or(text.len(), |ws| cut + ws);
    text.split_at(cut)
}

/// Second halves of hyphenated names that still refer to the term itself
/// ("Simon-Dach-Straße", "Ostkreuz-Kiez"); any other hyphenated continuation
/// is a different, usually larger place ("Friedrichshain-Kreuzberg").
const SAME_PLACE_HYPHEN_SUFFIXES: &[&str] = &[
    "str", "allee", "platz", "weg", "ufer", "damm", "ring", "gasse", "chaussee", "brücke",
    "brucke", "kiez", "tor", "park", "bahnhof",
];

/// How strictly `find_term` treats what follows a match.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TermMatch {
    /// Plain keyword (required/blocklist): anything but a letter/digit
    /// continuing a short term may follow.
    Keyword,
    /// Sub-city place (area terms): additionally rejects hyphenated
    /// compounds with another place.
    Place,
}

/// Byte position of the first real mention of `term` in `text`, both already
/// normalized. A mention must start at a word boundary, so "Gärtnerstr" does
/// not match "Kleingärtnerstr" and "Berlin" does not match "Oberlin".
/// Terms of five or more letters may carry an inflection ("Friedrichshains",
/// "Friedrichshainer"); shorter terms and terms ending in a digit (postal
/// codes) must end at a boundary. With `TermMatch::Place`, a hyphenated
/// compound with another place does not count ("Friedrichshain-Kreuzberg" is
/// the whole borough) while "Berlin-Friedrichshain" and "Simon-Dach-Straße" do.
pub(crate) fn find_term(text: &str, term: &str, mode: TermMatch) -> Option<usize> {
    let term = term.trim();
    if term.is_empty() {
        return None;
    }
    let strict_end =
        term.chars().count() < 5 || term.chars().next_back().is_some_and(|c| c.is_ascii_digit());
    let mut from = 0;
    while let Some(offset) = text[from..].find(term) {
        let start = from + offset;
        let end = start + term.len();
        from = start + term.chars().next().map_or(1, char::len_utf8);

        if text[..start]
            .chars()
            .next_back()
            .is_some_and(char::is_alphanumeric)
        {
            continue;
        }
        let after = &text[end..];
        let mut next = after.chars();
        match next.next() {
            Some(c) if strict_end && c.is_alphanumeric() => continue,
            Some('-')
                if mode == TermMatch::Place && next.next().is_some_and(char::is_alphabetic) =>
            {
                let tail = &after[1..];
                if !SAME_PLACE_HYPHEN_SUFFIXES
                    .iter()
                    .any(|suffix| tail.starts_with(suffix))
                {
                    continue;
                }
            }
            _ => {}
        }
        return Some(start);
    }
    None
}

/// Result of the keyword stage: location evidence from matching area groups,
/// plus human-readable matched terms for logging.
pub(crate) struct KeywordMatch {
    pub(crate) evidence: Vec<LocationEvidence>,
    pub(crate) matched: Vec<String>,
}

/// Check keywords for blocklist/required/area matches.
/// Returns `None` if the item should be dropped, otherwise one piece of
/// evidence per matching area group: its implied distance, and whether the
/// term appears prominently or only deep in the article body.
pub(crate) fn keyword_check(
    item: &FeedItem,
    filter: &FilterConfig,
    required: &[String],
) -> Option<KeywordMatch> {
    let text = ItemText::of(item);

    if filter.blocklist.iter().any(|b| text.contains(b)) {
        return None;
    }

    if !required.is_empty() && !required.iter().any(|r| text.contains(r)) {
        return None;
    }

    let mut evidence = Vec::new();
    let mut matched = Vec::new();
    for group in &filter.area {
        // Within a group all terms imply the same distance, so a prominent
        // mention of any term is the best this group can offer.
        let mut best: Option<(&String, bool)> = None;
        for term in &group.terms {
            if let Some(prominent) = text.mention(term, TermMatch::Place) {
                if best.is_none_or(|(_, p)| prominent && !p) {
                    best = Some((term, prominent));
                }
            }
        }
        if let Some((term, prominent)) = best {
            matched.push(format!(
                "\"{}\" ({}m{})",
                term,
                group.implied_meters as u32,
                if prominent { "" } else { ", body only" }
            ));
            evidence.push(LocationEvidence {
                meters: group.implied_meters,
                label: term.clone(),
                kind: EvidenceKind::Keyword,
                prominent,
                penalty: 0,
            });
        }
    }

    // No area match is not a hard drop — caller can fall back to source.base_implied_meters.
    Some(KeywordMatch { evidence, matched })
}

// ── Seen-items store (append-only file) ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn item(title: &str, article: &str) -> FeedItem {
        FeedItem {
            guid: "t".to_owned(),
            title: title.to_owned(),
            link: None,
            link_note: None,
            description: None,
            article_text: Some(article.to_owned()),
            source_name: "Test".to_owned(),
            score: 0,
            max_score: 0,
            distance_meters: None,
            location_label: None,
            published_at: None,
            ..Default::default()
        }
    }

    fn area(meters: f64, terms: &[&str]) -> AreaGroup {
        AreaGroup {
            implied_meters: meters,
            terms: terms.iter().map(|t| (*t).to_owned()).collect(),
        }
    }

    #[test]
    fn place_terms_skip_compounds_with_other_places_but_keep_inflections() {
        let t = |text: &str, term: &str| {
            find_term(&normalize(text), &normalize(term), TermMatch::Place)
        };

        assert!(t("Bezirk Friedrichshain-Kreuzberg", "Friedrichshain").is_none());
        assert!(t("Einsatz in Berlin-Friedrichshain", "Friedrichshain").is_some());
        assert!(t("Friedrichshains Bürgermeisterin", "Friedrichshain").is_some());
        assert!(t("im Friedrichshainer Kiez", "Friedrichshain").is_some());
        assert!(t("Bar in der Simon-Dach-Straße", "Simon-Dach").is_some());
        assert!(t("Kleingärtnerstraße 3", "Gärtnerstr").is_none());
        assert!(t("Tel. 030 102470", "10247").is_none());
        assert!(t("10247 Berlin", "10247").is_some());
        assert!(t("rawdata", "RAW").is_none());
    }

    #[test]
    fn keyword_terms_accept_hyphenated_compounds() {
        let t = |text: &str| find_term(&normalize(text), "berlin", TermMatch::Keyword);

        assert!(t("Wahl in Berlin-Mitte").is_some());
        assert!(t("Oberlin").is_none());
    }

    #[test]
    fn police_borough_tag_and_contact_address_are_not_local_evidence() {
        // Shape of a real Polizei Berlin report from Kreuzberg: the borough tag
        // and the witness-appeal contact address used to score it as ~800m.
        let filter = FilterConfig {
            area: vec![
                area(800.0, &["10243", "10245", "10247"]),
                area(1500.0, &["Friedrichshain", "Wedekindstr"]),
            ],
            ..Default::default()
        };
        let report = item(
            "Mann bei Raub mit Messer verletzt",
            "Friedrichshain-Kreuzberg Nr. 1262 In der vergangenen Nacht wurde ein Mann bei \
             einem Raub in Kreuzberg verletzt. Wer hat die Tat beobachtet? Hinweise nimmt der \
             Verkehrsermittlungsdienst in der Wedekindstraße 10, 10243 Berlin entgegen.",
        );

        let hit = keyword_check(&report, &filter, &[]).expect("passes required check");

        assert!(hit.evidence.is_empty(), "{:?}", hit.matched);
    }

    #[test]
    fn mentions_beyond_the_lead_are_marked_not_prominent() {
        let filter = FilterConfig {
            area: vec![area(1500.0, &["Ostkreuz"])],
            ..Default::default()
        };
        let long_article = format!(
            "{} Für den Neubau soll die Linie 21 zum Bahnhof Ostkreuz geführt werden.",
            "Der Senat berät über den Haushalt. ".repeat(60)
        );
        let aside = item("Haushalt beschlossen", &long_article);
        let lead = item("Haushalt beschlossen", "Am Ostkreuz wurde heute gebaut.");

        let aside_hit = keyword_check(&aside, &filter, &[]).unwrap();
        let lead_hit = keyword_check(&lead, &filter, &[]).unwrap();

        assert!(!aside_hit.evidence[0].prominent);
        assert_eq!(aside_hit.evidence[0].score(), 1);
        assert!(lead_hit.evidence[0].prominent);
        assert_eq!(lead_hit.evidence[0].score(), 2);
    }

    #[test]
    fn split_lead_ends_at_a_word_boundary() {
        let text = "wort ".repeat(400);
        let (lead, rest) = split_lead(&text);

        assert!(lead.len() >= ARTICLE_LEAD_BYTES);
        assert!(rest.starts_with(' '));
        assert_eq!(lead.len() + rest.len(), text.len());
        assert_eq!(split_lead("kurz"), ("kurz", ""));
    }
}
