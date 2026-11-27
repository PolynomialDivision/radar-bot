//! Delivery classes (D5): a constant table per category, three rules above it, and the
//! conditions for an urgent push (handover S4u item 2). Pure: a decision and the record of its
//! inputs, so that "why" is a lookup.

use serde::Serialize;

use crate::classify::NoiseRule;
use crate::filter::{find_word, normalize};
use crate::model::{Category, Class, Precision, Scope};

/// What a decision is made from.
#[derive(Debug, Clone)]
pub(crate) struct DecisionInput<'a> {
    pub(crate) category: Category,
    pub(crate) noise: Option<NoiseRule>,
    /// Proximity 0 to 5 from the best evidence.
    pub(crate) ring: i32,
    pub(crate) precision: Precision,
    pub(crate) scope: Scope,
    pub(crate) official: bool,
    pub(crate) social: bool,
    /// Non-social publishers reporting it (1 for a single observation).
    pub(crate) publishers: u32,
    pub(crate) happened_at: Option<i64>,
    pub(crate) first_seen_at: i64,
    pub(crate) now: i64,
    pub(crate) title: &'a str,
}

/// The decision and what it was made from, stored as JSON with the observation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct DecisionRecord {
    pub(crate) class: Class,
    /// The rule that decided.
    pub(crate) rule: String,
    pub(crate) category: Category,
    pub(crate) ring: i32,
    pub(crate) precision: Precision,
    pub(crate) scope: Scope,
    pub(crate) official: bool,
    pub(crate) social: bool,
    pub(crate) publishers: u32,
    pub(crate) happened_at: Option<i64>,
    pub(crate) first_seen_at: i64,
    /// Minutes between the event and the bot seeing it.
    pub(crate) seen_delay_min: Option<i64>,
    /// Why it is not urgent, for the categories that can be.
    pub(crate) not_urgent: Option<&'static str>,
    /// What the gate (D7) would have done with the page: "fetch" or "skip".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) gate: Option<&'static str>,
}

/// One row of the table: the ring from which a category is in the briefing, and from which it
/// is kept for commands (below: dropped).
struct Row {
    category: Category,
    briefing_ring: i32,
    archive_ring: i32,
}

const TABLE: &[Row] = &[
    row(Category::Incident, 3, 1),
    row(Category::Utility, 3, 2),
    row(Category::Traffic, 4, 2),
    row(Category::Transit, 4, 2),
    row(Category::Assembly, 3, 2),
    row(Category::Event, 3, 2),
    row(Category::Authority, 3, 2),
    row(Category::Community, 3, 2),
    row(Category::Other, 4, 2),
];

const fn row(category: Category, briefing_ring: i32, archive_ring: i32) -> Row {
    Row {
        category,
        briefing_ring,
        archive_ring,
    }
}

/// A social transit or traffic post is perishable: after this it is only archived.
const PERISHABLE_SECS: i64 = 2 * 3600;
/// An urgent report is at most this old.
const URGENT_WITHIN_SECS: i64 = 3 * 3600;
/// Words in a title that widen the urgent ring to 3.
const RAISING: &[&str] = &[
    "evakuierung",
    "evakuiert",
    "großbrand",
    "schüsse",
    "explosion",
    "lebensgefährlich",
];
/// Words in a title that make a report never urgent.
const LOWERING: &[&str] = &[
    "zeugen gesucht",
    "bilanz",
    "prozess",
    "urteil",
    "kommentar",
    "jahrestag",
];

/// The class of a report and the record of why.
pub(crate) fn decide(input: &DecisionInput<'_>) -> (Class, DecisionRecord) {
    let (class, rule) = class_of(input);
    let record = DecisionRecord {
        class,
        rule,
        category: input.category,
        ring: input.ring,
        precision: input.precision,
        scope: input.scope,
        official: input.official,
        social: input.social,
        publishers: input.publishers,
        happened_at: input.happened_at,
        first_seen_at: input.first_seen_at,
        seen_delay_min: input
            .happened_at
            .map(|happened| (input.first_seen_at - happened) / 60),
        not_urgent: matches!(input.category, Category::Incident | Category::Utility)
            .then(|| urgent_refusal(input))
            .flatten(),
        gate: None,
    };
    (class, record)
}

fn class_of(input: &DecisionInput<'_>) -> (Class, String) {
    if let Some(noise) = input.noise {
        return (Class::Drop, format!("noise: {}", noise.as_str()));
    }
    let floor = if input.scope == Scope::Local {
        Class::Briefing
    } else {
        Class::Drop
    };
    let ended = input.social
        && matches!(input.category, Category::Transit | Category::Traffic)
        && input
            .happened_at
            .is_some_and(|happened| input.now - happened > PERISHABLE_SECS);
    if ended {
        return (Class::Archive.max(floor), "window ended".to_owned());
    }
    if matches!(input.category, Category::Incident | Category::Utility)
        && urgent_refusal(input).is_none()
    {
        return (Class::Urgent, format!("urgent: {}", input.category));
    }
    let row = TABLE
        .iter()
        .find(|r| r.category == input.category)
        .expect("every category has a row");
    let corroborated = input.category == Category::Incident
        && input.ring == row.briefing_ring - 1
        && input.publishers >= 2;
    let (class, why) = if input.ring >= row.briefing_ring || corroborated {
        (Class::Briefing, "briefing")
    } else if input.ring >= row.archive_ring {
        (Class::Archive, "archive")
    } else {
        (Class::Drop, "drop")
    };
    if floor > class {
        return (
            floor,
            format!("local source, {} ring {}", input.category, input.ring),
        );
    }
    (
        class,
        format!("table: {} ring {} → {why}", input.category, input.ring),
    )
}

/// Why an incident or utility report is not urgent; `None` when it is.
fn urgent_refusal(input: &DecisionInput<'_>) -> Option<&'static str> {
    let title = normalize(input.title);
    let has = |words: &[&str]| {
        words
            .iter()
            .any(|word| find_word(&title, &normalize(word), &[]).is_some())
    };
    if input.noise.is_some() {
        return Some("noise");
    }
    if has(LOWERING) {
        return Some("lowering word");
    }
    let ring = if input.category == Category::Incident && has(RAISING) {
        3
    } else {
        4
    };
    if input.ring < ring {
        return Some("not near enough");
    }
    if input.category == Category::Incident && input.precision < Precision::Street {
        return Some("not located at a street");
    }
    match input.happened_at {
        Some(happened) if input.now - happened <= URGENT_WITHIN_SECS => {}
        _ => return Some("older than 3 hours"),
    }
    if input.social && input.publishers == 0 {
        return Some("social only");
    }
    if !(input.official || input.publishers >= 2) {
        return Some("one unofficial publisher");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(category: Category, ring: i32) -> DecisionInput<'static> {
        DecisionInput {
            category,
            noise: None,
            ring,
            precision: Precision::Street,
            scope: Scope::City,
            official: false,
            social: false,
            publishers: 1,
            happened_at: Some(1_000_000),
            first_seen_at: 1_000_600,
            now: 1_000_600,
            title: "Meldung",
        }
    }

    #[test]
    fn the_table_decides_by_category_and_ring() {
        assert_eq!(decide(&input(Category::Incident, 3)).0, Class::Briefing);
        assert_eq!(decide(&input(Category::Incident, 1)).0, Class::Archive);
        assert_eq!(decide(&input(Category::Incident, 0)).0, Class::Drop);
        assert_eq!(decide(&input(Category::Other, 3)).0, Class::Archive);
        assert_eq!(decide(&input(Category::Other, 4)).0, Class::Briefing);
        let mut two = input(Category::Incident, 2);
        two.publishers = 2;
        assert_eq!(
            decide(&two).0,
            Class::Briefing,
            "ring 2 with two publishers"
        );
    }

    #[test]
    fn noise_drops_and_a_local_source_has_a_floor() {
        let mut advert = input(Category::Other, 5);
        advert.noise = Some(NoiseRule::Advert);
        assert_eq!(decide(&advert).0, Class::Drop);
        let mut local = input(Category::Other, 2);
        local.scope = Scope::Local;
        let (class, record) = decide(&local);
        assert_eq!(class, Class::Briefing);
        assert!(record.rule.starts_with("local source"), "{}", record.rule);
    }

    #[test]
    fn an_old_social_transit_post_is_archived() {
        let mut post = input(Category::Transit, 5);
        post.social = true;
        post.now = post.happened_at.unwrap() + 3 * 3600;
        assert_eq!(decide(&post).0, Class::Archive);
    }

    #[test]
    fn urgent_needs_near_fresh_official_or_two_publishers() {
        let mut fire = input(Category::Incident, 4);
        fire.official = true;
        let (class, record) = decide(&fire);
        assert_eq!(class, Class::Urgent);
        assert_eq!(record.seen_delay_min, Some(10));

        let mut one = fire.clone();
        one.official = false;
        assert_eq!(decide(&one).1.not_urgent, Some("one unofficial publisher"));
        let mut old = fire.clone();
        old.now += 4 * 3600;
        assert_eq!(decide(&old).1.not_urgent, Some("older than 3 hours"));
        let mut witnesses = fire.clone();
        witnesses.title = "Zeugen gesucht nach Brand";
        assert_eq!(decide(&witnesses).0, Class::Briefing);
        let mut far = fire.clone();
        far.ring = 3;
        assert_eq!(decide(&far).0, Class::Briefing);
        far.title = "Großbrand in Lagerhalle";
        assert_eq!(
            decide(&far).0,
            Class::Urgent,
            "a raising word widens the ring"
        );
        let mut film = fire.clone();
        film.category = Category::Event;
        film.ring = 5;
        assert_eq!(decide(&film).0, Class::Briefing, "an event is never urgent");
    }
}
