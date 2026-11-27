//! Categories (D6), assigned by rules: first the noise rules (adverts, social posts of tags and
//! links only, captions), then keyword lists in a fixed order, then the source's default, else
//! `other`. Keywords match as words ([`find_word`]): "Brand" is not "Brandenburg".

use crate::filter::{find_word, normalize};
use crate::model::{Category, Scope};

/// Why a report is noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoiseRule {
    /// A job or property advert.
    Advert,
    /// A social post of hashtags, mentions and links only.
    TagsOnly,
    /// A short social post without anything that happened (a photo caption).
    Caption,
}

impl NoiseRule {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            NoiseRule::Advert => "advert",
            NoiseRule::TagsOnly => "tags only",
            NoiseRule::Caption => "caption",
        }
    }
}

/// What the classifier knows about the source.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceFacts {
    pub(crate) scope: Scope,
    pub(crate) social: bool,
    /// The category the source implies (a police feed: `incident`).
    pub(crate) category: Option<Category>,
}

/// Where a keyword counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Title,
    /// Title or teaser, never the body alone (incidents: a body names other events).
    TitleTeaser,
    /// Title, teaser or the lead of the body.
    Any,
}

/// A keyword with what it needs and what rules it out.
#[derive(Debug, Clone, Copy)]
struct Keyword {
    term: &'static str,
    /// Compounds that still mean the term ("brand" + "stiftung").
    tails: &'static [&'static str],
    /// Overrides the category's field.
    field: Option<Field>,
    /// Words in the same field that make it something else ("Konto gesperrt").
    not_with: &'static [&'static str],
    /// A word directly before it that makes it something else ("erste Lesung").
    not_after: &'static [&'static str],
    /// A word directly after it that makes it something else ("Explosion der Mieten").
    not_before: &'static [&'static str],
    /// Endings that make it another word ("Raucher").
    not_endings: &'static [&'static str],
    /// It counts only with one of these in the same sentence ("Aufzug" of a demonstration).
    needs: &'static [&'static str],
}

const fn kw(term: &'static str) -> Keyword {
    Keyword {
        term,
        tails: &[],
        field: None,
        not_with: &[],
        not_after: &[],
        not_before: &[],
        not_endings: &[],
        needs: &[],
    }
}

impl Keyword {
    const fn tails(self, tails: &'static [&'static str]) -> Self {
        Keyword { tails, ..self }
    }
    const fn title_only(self) -> Self {
        Keyword {
            field: Some(Field::Title),
            ..self
        }
    }
    const fn not_with(self, not_with: &'static [&'static str]) -> Self {
        Keyword { not_with, ..self }
    }
    const fn not_after(self, not_after: &'static [&'static str]) -> Self {
        Keyword { not_after, ..self }
    }
    const fn not_before(self, not_before: &'static [&'static str]) -> Self {
        Keyword { not_before, ..self }
    }
    const fn not_endings(self, not_endings: &'static [&'static str]) -> Self {
        Keyword {
            not_endings,
            ..self
        }
    }
    const fn needs(self, needs: &'static [&'static str]) -> Self {
        Keyword { needs, ..self }
    }
}

const NOT_ONLINE: &[&str] = &["konto", "account", "accounts", "profil", "nutzer", "app"];
const ASSEMBLY_WORDS: &[&str] = &[
    "demo",
    "demonstration",
    "demonstranten",
    "kundgebung",
    "versammlung",
    "protest",
    "teilnehmer",
    "teilnehmende",
];

/// The rule sets in their order: the first category with a matching keyword wins.
const RULES: &[(Category, Field, &[Keyword])] = &[
    (
        Category::Utility,
        Field::Any,
        &[
            kw("stromausfall"),
            kw("wasserrohrbruch"),
            kw("fernwärme"),
            kw("gasleck"),
            kw("gasgeruch"),
            kw("heizungsausfall"),
        ],
    ),
    (
        Category::Incident,
        Field::TitleTeaser,
        &[
            kw("brand").tails(&[
                "stiftung", "anschlag", "stifter", "ursache", "ort", "stelle",
            ]),
            // Forms no inflection ending makes, and compounds that end in the keyword.
            kw("brände"),
            kw("brennt"),
            kw("brannte"),
            kw("gebrannt"),
            kw("großbrand"),
            kw("wohnungsbrand"),
            kw("feuer"),
            kw("feuerwehreinsatz"),
            kw("rauch").not_endings(&["er", "ern"]),
            kw("explosion").not_before(&["der", "bei", "von"]),
            kw("unfall").tails(&["flucht", "ort", "stelle", "fahrer", "wagen"]),
            kw("verkehrsunfall"),
            kw("verletzt"),
            kw("schwerverletzt"),
            kw("tot"),
            kw("leiche"),
            kw("polizeieinsatz"),
            kw("festnahme"),
            kw("festgenommen"),
            kw("messer").tails(&["angriff", "attacke", "stecherei"]),
            kw("schuss"),
            kw("schüsse"),
            kw("raub").tails(&["überfall"]),
            kw("überfall"),
            kw("überfallen"),
            kw("einbruch"),
            kw("angriff"),
            kw("angegriffen"),
            kw("schlägerei"),
            kw("bombe"),
            kw("evakuiert"),
            kw("evakuierung"),
            kw("razzia"),
            kw("durchsuchung"),
            kw("vermisst").title_only(),
        ],
    ),
    (
        Category::Assembly,
        Field::Any,
        &[
            kw("demo"),
            kw("demonstration"),
            kw("kundgebung"),
            kw("aufzug").needs(ASSEMBLY_WORDS),
            kw("protest"),
            kw("mahnwache"),
            kw("blockade"),
        ],
    ),
    (
        Category::Transit,
        Field::Any,
        &[
            kw("bvg"),
            kw("s-bahn").tails(&["hof", "linie", "verkehr"]),
            kw("u-bahn").tails(&["hof", "linie", "verkehr"]),
            kw("tram"),
            kw("straßenbahn"),
            kw("ersatzverkehr"),
            kw("pendelverkehr"),
            kw("warnstreik"),
        ],
    ),
    (
        Category::Traffic,
        Field::Any,
        &[
            kw("baustelle"),
            kw("sperrung").title_only().not_with(NOT_ONLINE),
            kw("gesperrt").title_only().not_with(NOT_ONLINE),
            kw("vollsperrung"),
            kw("umleitung"),
            kw("bauarbeiten"),
        ],
    ),
    (
        Category::Event,
        Field::Any,
        &[
            kw("straßenfest"),
            kw("kiezfest"),
            kw("hoffest"),
            kw("flohmarkt"),
            kw("wochenmarkt"),
            kw("weihnachtsmarkt"),
            kw("konzert"),
            kw("festival"),
            kw("lesung")
                .title_only()
                .not_after(&["erste", "zweite", "dritte", "ersten", "zweiten", "dritten"]),
            kw("ausstellung"),
            kw("eröffnung").title_only(),
            kw("filmabend"),
            kw("filmvorführung"),
        ],
    ),
    (
        Category::Authority,
        Field::Any,
        &[
            kw("bezirksamt"),
            kw("bvv"),
            kw("senat"),
            kw("bebauungsplan"),
            kw("milieuschutz"),
            kw("ordnungsamt"),
            kw("beteiligung").title_only(),
            kw("parkraumbewirtschaftung"),
            kw("spielstraße"),
        ],
    ),
    (
        Category::Community,
        Field::Any,
        &[
            kw("miete"),
            kw("mieter"),
            kw("räumung"),
            kw("hausprojekt"),
            kw("verdrängung"),
            kw("initiative").title_only(),
            kw("nachbarschaft"),
        ],
    ),
];

/// How much of the body counts as its lead for the keywords.
const LEAD_CHARS: usize = 600;

/// The category of a report and, before it, whether it is noise. Pure: the same text always
/// gives the same answer.
pub(crate) fn classify(
    title: &str,
    teaser: &str,
    body: &str,
    source: &SourceFacts,
) -> (Category, Option<NoiseRule>) {
    let title_n = normalize(title);
    let teaser_n = normalize(teaser);
    let lead: String = body.chars().take(LEAD_CHARS).collect();
    let lead_n = normalize(&lead);
    let category = RULES
        .iter()
        .find(|(_, default_field, keywords)| {
            keywords.iter().any(|keyword| {
                let fields: &[&str] = match keyword.field.unwrap_or(*default_field) {
                    Field::Title => &[&title_n],
                    Field::TitleTeaser => &[&title_n, &teaser_n],
                    Field::Any => &[&title_n, &teaser_n, &lead_n],
                };
                fields.iter().any(|text| matches(keyword, text))
            })
        })
        .map(|(category, ..)| *category)
        .or(source.category)
        .unwrap_or(Category::Other);
    let noise = noise(title, teaser, source, category);
    (category, noise)
}

/// Whether `keyword` occurs in the normalized `text` under its conditions.
fn matches(keyword: &Keyword, text: &str) -> bool {
    let term = normalize(keyword.term);
    let tails: Vec<String> = keyword.tails.iter().map(|t| normalize(t)).collect();
    let tails: Vec<&str> = tails.iter().map(String::as_str).collect();
    if keyword
        .not_with
        .iter()
        .any(|word| find_word(text, word, &[]).is_some())
    {
        return false;
    }
    let mut from = 0;
    while let Some(offset) = find_word(&text[from..], &term, &tails) {
        let start = from + offset;
        let word_end = start
            + text[start..]
                .char_indices()
                .find(|(_, c)| !(c.is_alphanumeric() || *c == '-'))
                .map_or(text.len() - start, |(i, _)| i);
        from = word_end;
        let ending = &text[start + term.len()..word_end];
        if keyword.not_endings.contains(&ending) {
            continue;
        }
        let before = text[..start].split_whitespace().next_back().unwrap_or("");
        if keyword.not_after.contains(&before) {
            continue;
        }
        let after = text[word_end..].split_whitespace().next().unwrap_or("");
        if keyword.not_before.contains(&after) {
            continue;
        }
        if !keyword.needs.is_empty() {
            let sentence_start = sentence_ends(&text[..start]).last().map_or(0, |i| i + 1);
            let sentence_end = sentence_ends(&text[word_end..])
                .next()
                .map_or(text.len(), |i| word_end + i);
            let sentence = &text[sentence_start..sentence_end];
            if !keyword
                .needs
                .iter()
                .any(|word| find_word(sentence, word, &[]).is_some())
            {
                continue;
            }
        }
        return true;
    }
    false
}

/// Where sentences end in `text`: a full stop, `!` or `?` followed by a space or the end (so
/// "2.000" goes on).
fn sentence_ends(text: &str) -> impl Iterator<Item = usize> + '_ {
    text.char_indices()
        .filter(|(i, c)| {
            matches!(c, '.' | '!' | '?')
                && text[i + c.len_utf8()..]
                    .chars()
                    .next()
                    .is_none_or(char::is_whitespace)
        })
        .map(|(i, _)| i)
}

/// Adverts, social posts of tags and links only, captions. On title and teaser only: a page's
/// furniture (a publisher's own job teaser) must not make an article noise.
fn noise(title: &str, teaser: &str, source: &SourceFacts, category: Category) -> Option<NoiseRule> {
    let text = format!("{title} {teaser}");
    let lower = text.to_lowercase();
    let advert = [
        "(m/w/d)",
        "(w/m/d)",
        "(m/w/x)",
        "(d/m/w)",
        "#kitajobs",
        "#jobs",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
        || ["zimmerwohnung", "bezugsfrei", "nachmieter"]
            .iter()
            .any(|word| find_word(&lower, word, &[]).is_some())
        || ((lower.contains("m²") || lower.contains(" qm"))
            && (lower.contains('€') || lower.contains("euro")));
    if advert {
        return Some(NoiseRule::Advert);
    }
    if !source.social {
        return None;
    }
    let words: Vec<&str> = text
        .split_whitespace()
        .filter(|w| !(w.starts_with('#') || w.starts_with('@') || w.starts_with("http")))
        .filter(|w| w.chars().any(char::is_alphabetic))
        .collect();
    if words.is_empty() {
        return Some(NoiseRule::TagsOnly);
    }
    let body = words.join(" ");
    let something_happened = matches!(category, Category::Incident | Category::Utility)
        || words.iter().any(|w| looks_like_a_verb(w));
    if body.chars().count() < 40 && !something_happened {
        return Some(NoiseRule::Caption);
    }
    None
}

/// Lower-case words that are not articles or prepositions: in German, mostly verbs.
fn looks_like_a_verb(word: &str) -> bool {
    const SMALL: &[&str] = &[
        "der", "die", "das", "den", "dem", "des", "ein", "eine", "einen", "einem", "und", "oder",
        "in", "im", "am", "an", "auf", "aus", "bei", "mit", "von", "vom", "zu", "zum", "zur",
        "für", "über", "unter", "vor", "nach", "heute", "gestern", "morgen", "the", "a", "an",
        "of", "at", "on", "and", "in",
    ];
    let word = word.trim_matches(|c: char| !c.is_alphanumeric());
    word.chars().next().is_some_and(char::is_lowercase)
        && word.chars().count() > 2
        && !SMALL.contains(&word)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn category(title: &str, teaser: &str, body: &str) -> Category {
        let facts = SourceFacts {
            scope: Scope::City,
            social: false,
            category: None,
        };
        classify(title, teaser, body, &facts).0
    }

    /// The R2-10 table: each false friend next to a true one.
    #[test]
    fn keywords_match_words_not_their_false_friends() {
        for (title, expected) in [
            ("Brandenburg beschließt Haushalt", Category::Other),
            ("Brand in der Britzer Allee", Category::Incident),
            ("Brandschutz: Neue Regeln für Altbauten", Category::Other),
            ("Silvester: Wo Feuerwerk verboten ist", Category::Other),
            (
                "Feuer in Keller an der Perleberger Straße",
                Category::Incident,
            ),
            ("Unfallstatistik: Weniger Verkehrstote", Category::Other),
            ("Unfallflucht in der Musterstraße", Category::Incident),
            ("Jahrestag des Angriffskriegs", Category::Other),
            ("Angriff auf Radfahrer", Category::Incident),
            (
                "Explosion der Mieten in der Innenstadt",
                Category::Community,
            ),
            ("Explosion in Wohnhaus", Category::Incident),
            ("Raucherkneipen: neue Regeln", Category::Other),
            ("Rauch aus Kellerfenster", Category::Incident),
            ("Aufzug am S-Bahnhof außer Betrieb", Category::Transit),
            (
                "Aufzug mit 2.000 Teilnehmern zieht durch die Stadt",
                Category::Assembly,
            ),
            ("Protestanten wählen neue Bischöfin", Category::Other),
            ("Protest gegen Räumung", Category::Assembly),
            ("Erste Lesung im Abgeordnetenhaus", Category::Other),
            ("Lesung im Nachbarschaftshaus", Category::Event),
            ("Konto gesperrt: Betrugsmasche mit SMS", Category::Other),
            (
                "Lübecker Straße wegen Bauarbeiten gesperrt",
                Category::Traffic,
            ),
            ("Sperrung der Britzer Allee ab Montag", Category::Traffic),
        ] {
            assert_eq!(category(title, "", ""), expected, "{title}");
        }
    }

    #[test]
    fn some_keywords_count_only_in_the_title_or_never_in_the_body_alone() {
        assert_eq!(
            category("Kiezkolumne", "", "Der Bäcker wird im Kiez vermisst."),
            Category::Other
        );
        assert_eq!(
            category("Kiezkolumne", "", "Es gab einen Brand."),
            Category::Other
        );
        assert_eq!(
            category("Kiezkolumne", "Es gab einen Brand.", ""),
            Category::Incident
        );
        assert_eq!(
            category("Neue Fabrik", "", "Zur Eröffnung kam der Minister."),
            Category::Other
        );
    }

    #[test]
    fn adverts_and_captions_are_noise() {
        let social = SourceFacts {
            scope: Scope::Local,
            social: true,
            category: None,
        };
        let noise = |title: &str| classify(title, "", "", &social).1;
        assert_eq!(
            noise("Erzieher (m/w/d) in 10000 Yourcity #KitaJobs"),
            Some(NoiseRule::Advert)
        );
        assert_eq!(
            noise("Bezugsfreie Zimmerwohnung, 81 m² für 900 €"),
            Some(NoiseRule::Advert)
        );
        assert_eq!(
            noise("#Urban #Kiez @someone https://example.com"),
            Some(NoiseRule::TagsOnly)
        );
        assert_eq!(
            noise("Sonnenuntergang am Westkreuz"),
            Some(NoiseRule::Caption)
        );
        assert_eq!(
            noise("Feuer in der Musterstraße!"),
            None,
            "a short incident is no caption"
        );
        assert_eq!(noise("Die Tram fährt wieder"), None);
    }
}
