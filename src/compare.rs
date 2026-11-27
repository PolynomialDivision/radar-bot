//! The shadow comparison (`!radar vergleich`, the dry run): what the digest delivered next to
//! the class the new policy gives the same observations.

use crate::store::Compared;

/// Titles listed per group, at most.
const TITLES: usize = 10;

fn shadow_delivers(row: &Compared) -> bool {
    matches!(row.class.as_str(), "briefing" | "urgent")
}

/// The three groups with their counts and up to ten titles each, for `days` days.
pub(crate) fn comparison_report(rows: &[Compared], days: u32) -> String {
    let groups: [(&str, Vec<&Compared>, bool); 3] = [
        (
            "Geliefert, im Schatten auch",
            rows.iter()
                .filter(|r| r.delivered && shadow_delivers(r))
                .collect(),
            false,
        ),
        (
            "Geliefert, im Schatten nicht (archive/drop)",
            rows.iter()
                .filter(|r| r.delivered && !shadow_delivers(r))
                .collect(),
            true,
        ),
        (
            "Nicht geliefert, im Schatten schon (briefing/urgent)",
            rows.iter()
                .filter(|r| !r.delivered && shadow_delivers(r))
                .collect(),
            true,
        ),
    ];
    let mut lines = vec![format!(
        "🔍 Vergleich, letzte {days} Tage: {} Beobachtungen",
        rows.len()
    )];
    for (label, group, with_rule) in groups {
        lines.push(String::new());
        lines.push(format!("{label}: {}", group.len()));
        for row in group.iter().take(TITLES) {
            if with_rule {
                lines.push(format!("• {} ({}: {})", row.title, row.class, row.rule));
            } else {
                lines.push(format!("• {} ({})", row.title, row.class));
            }
        }
        if group.len() > TITLES {
            lines.push(format!("… und {} weitere", group.len() - TITLES));
        }
    }
    lines.join("\n")
}

/// `!radar vergleich [days]`: the days asked for, 3 by default, at most 14.
pub(crate) fn days_of(args: &str) -> u32 {
    args.split_whitespace()
        .next()
        .and_then(|d| d.parse::<u32>().ok())
        .unwrap_or(3)
        .clamp(1, 14)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(title: &str, class: &str, delivered: bool) -> Compared {
        Compared {
            title: title.to_owned(),
            class: class.to_owned(),
            rule: format!("table: other ring 2 → {class}"),
            delivered,
        }
    }

    #[tokio::test]
    async fn the_comparison_lists_three_groups() {
        let (db, _dir) = crate::testing::test_db();
        let now = chrono::Utc::now().timestamp();
        let item = |guid: &str, title: &str| crate::FeedItem {
            guid: guid.to_owned(),
            title: title.to_owned(),
            source_name: "Feed".to_owned(),
            score: 4,
            max_score: 5,
            ..Default::default()
        };
        // Delivered by the digest: posted in `items`.
        for (guid, title) in [
            ("a", "Brand in der Britzer Allee"),
            ("b", "Wohnung zu vermieten"),
        ] {
            db.insert_queued(&item(guid, title)).await.unwrap();
        }
        let taken: Vec<String> = db
            .take_for_digest(1)
            .await
            .unwrap()
            .into_iter()
            .map(|i| i.guid)
            .collect();
        db.mark_posted(&taken).await.unwrap();
        for (guid, title, class) in [
            ("a", "Brand in der Britzer Allee", "briefing"),
            ("b", "Wohnung zu vermieten", "drop"),
            ("c", "Straßenfest in der Musterstraße", "briefing"),
            ("d", "Senat beschließt Haushalt", "archive"),
        ] {
            let observation = crate::store::Observation {
                source_id: "feed".to_owned(),
                ext_id: guid.to_owned(),
                first_seen_at: now,
                title: title.to_owned(),
                category: "other".to_owned(),
                precision: "none".to_owned(),
                evidence: "[]".to_owned(),
                class: class.to_owned(),
                decision: format!(r#"{{"rule": "table: other ring 2 → {class}"}}"#),
                ..Default::default()
            };
            db.insert_observation(observation).await.unwrap();
        }

        let rows = db.compare_since(now - 3 * 86_400).await.unwrap();

        crate::testing::assert_golden("vergleich.txt", &comparison_report(&rows, 3));
    }

    #[test]
    fn long_groups_are_cut_at_ten_titles() {
        let rows: Vec<Compared> = (0..12)
            .map(|i| row(&format!("Titel {i}"), "briefing", false))
            .collect();
        let report = comparison_report(&rows, 7);
        assert!(report.contains("… und 2 weitere"), "{report}");
        assert_eq!(days_of(""), 3);
        assert_eq!(days_of("30"), 14);
    }
}
