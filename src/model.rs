//! The types the decision works with: what a source reported, its category, how precisely it
//! is located, the scope of its source, and the class it is delivered in.

use serde::{Deserialize, Serialize};

use crate::feed::FeedItem;

/// One report of a prose source (news, social post), as the decision sees it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RawItem {
    /// Unique within the source (the feed's guid).
    pub(crate) ext_id: String,
    pub(crate) url: Option<String>,
    pub(crate) title: String,
    /// Shown to readers.
    pub(crate) teaser: Option<String>,
    /// The full text, from the feed or the page.
    pub(crate) text: Option<String>,
    pub(crate) published_at: Option<i64>,
    /// District or postal code the source files it under.
    pub(crate) area_hint: Option<String>,
    /// A social post's linked article.
    pub(crate) linked_url: Option<String>,
}

impl From<&FeedItem> for RawItem {
    fn from(item: &FeedItem) -> Self {
        RawItem {
            ext_id: item.guid.clone(),
            url: item.link.clone(),
            title: item.title.clone(),
            teaser: item.description.clone(),
            text: item.article_text.clone(),
            published_at: item.published_at,
            area_hint: item.district.clone(),
            linked_url: None,
        }
    }
}

macro_rules! names {
    ($ty:ident { $($variant:ident => $name:literal),* $(,)? }) => {
        impl $ty {
            pub(crate) fn as_str(self) -> &'static str {
                match self {
                    $($ty::$variant => $name),*
                }
            }

            // Reads stored values back (tests now, the stories of S3).
            #[allow(dead_code)]
            pub(crate) fn parse(name: &str) -> Option<Self> {
                match name {
                    $($name => Some($ty::$variant),)*
                    _ => None,
                }
            }
        }

        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

/// What a report is about (D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Category {
    Incident,
    Utility,
    Traffic,
    Transit,
    Assembly,
    Event,
    Authority,
    Community,
    Other,
}

names!(Category {
    Incident => "incident",
    Utility => "utility",
    Traffic => "traffic",
    Transit => "transit",
    Assembly => "assembly",
    Event => "event",
    Authority => "authority",
    Community => "community",
    Other => "other",
});

/// How a report is delivered (D5): pushed at once, in the briefing, on request only, or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Class {
    Drop,
    Archive,
    Briefing,
    Urgent,
}

names!(Class {
    Drop => "drop",
    Archive => "archive",
    Briefing => "briefing",
    Urgent => "urgent",
});

/// How precisely the best evidence locates a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Precision {
    None,
    City,
    Area,
    Place,
    Street,
    Point,
}

names!(Precision {
    None => "none",
    City => "city",
    Area => "area",
    Place => "place",
    Street => "street",
    Point => "point",
});

/// What a source covers: the neighbourhood, the city, or more (D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Scope {
    Local,
    City,
    Wide,
}

names!(Scope {
    Local => "local",
    City => "city",
    Wide => "wide",
});
