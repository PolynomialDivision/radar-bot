use std::collections::{HashMap, HashSet};

use serde_json::json;

use super::dwd::*;
use super::nina::*;
use super::post::*;
use super::quakes::{fetch_gdacs, fetch_usgs};
use super::*;
use crate::fetch::Recorded;
use crate::store::PostKey;

fn recorded(routes: &[(&str, u16, &str)]) -> (Http, std::sync::Arc<Recorded>) {
    let recorded = routes
        .iter()
        .fold(Recorded::new(), |r, (prefix, status, body)| {
            r.route(prefix, *status, body)
        });
    let recorded = std::sync::Arc::new(recorded);
    (Http::recorded(recorded.clone()), recorded)
}

#[tokio::test]
async fn a_failing_dwd_file_is_an_error_of_the_dwd_check() {
    use mxbot_common::supervise::{report, snapshot};
    let (http, _) = recorded(&[(DWD_WARNINGS_URL, 500, "<html>Internal Server Error</html>")]);

    let error = fetch_dwd_warnings(&http).await.unwrap_err();
    let alive = report("radar alerts, DWD under test");
    report_round(&alive, &[("DWD", format!("{error:#}"))]);

    assert!(format!("{error:#}").contains("HTTP 500"), "{error:#}");
    let task = snapshot()
        .into_iter()
        .find(|t| t.name == "radar alerts, DWD under test")
        .unwrap();
    assert!(task
        .last_err_reason
        .as_deref()
        .is_some_and(|r| r.starts_with("DWD check failed")));
}

#[tokio::test]
async fn an_unchanged_dwd_file_is_the_last_one_again() {
    let file = crate::testing::fixture("warnings/dwd_warnings.json");
    let recorded = std::sync::Arc::new(
        Recorded::new()
            .route(DWD_WARNINGS_URL, 200, &file)
            .with_etag("\"w1\""),
    );
    let http = Http::recorded(recorded.clone());
    let (db, _dir) = crate::testing::test_db();
    let active_before = db.list_active_dwd_warnings().await.unwrap().len();

    let first = fetch_dwd_warnings(&http).await.unwrap();
    let second = fetch_dwd_warnings(&http).await.unwrap();

    assert_eq!(first, second);
    assert_eq!(
        recorded.calls().len(),
        2,
        "the second request was answered 304"
    );
    assert_eq!(
        db.list_active_dwd_warnings().await.unwrap().len(),
        active_before
    );
}

#[tokio::test]
async fn warning_and_forecast_fetches_refuse_bad_answers() {
    let nina = crate::testing::fixture("warnings/nina_dashboard.json");
    let dashboard = "https://warnung.bund.de/api31/dashboard/";
    let (http, _) = recorded(&[(dashboard, 200, &nina)]);
    assert!(fetch_nina_dashboard(&http, "110000000000")
        .await
        .unwrap()
        .is_array());
    for (status, body) in [
        (503, "busy"),
        (200, "{\"not\": \"a list\"}"),
        (200, "<html>"),
    ] {
        let (http, _) = recorded(&[(dashboard, status, body)]);
        assert!(
            fetch_nina_dashboard(&http, "110000000000").await.is_err(),
            "NINA {status} {body}"
        );
    }

    let usgs = "https://earthquake.usgs.gov/";
    let (http, _) = recorded(&[(usgs, 200, "{\"features\": []}")]);
    assert!(fetch_usgs(&http).await.is_ok());
    let (http, _) = recorded(&[(usgs, 502, "{\"features\": []}")]);
    assert!(fetch_usgs(&http).await.is_err());

    let gdacs = "https://www.gdacs.org/";
    let rss = "<rss version=\"2.0\"><channel><title>G</title><item><title>Flood</title>\
               <guid>g1</guid></item></channel></rss>";
    let (http, _) = recorded(&[(gdacs, 200, rss)]);
    assert_eq!(fetch_gdacs(&http).await.unwrap().len(), 1);
    let (http, _) = recorded(&[(gdacs, 404, rss)]);
    assert!(fetch_gdacs(&http).await.is_err());
    let (http, _) = recorded(&[]);
    assert!(fetch_gdacs(&http).await.is_err());
}

#[test]
fn alert_rounds_are_reported_for_the_status_file() {
    use mxbot_common::supervise::{report, snapshot};
    let alive = report("radar alerts under test");
    report_round(&alive, &[]);
    report_round(
        &alive,
        &[
            ("NINA", "NINA fetch timed out".to_owned()),
            ("USGS", "USGS request failed".to_owned()),
        ],
    );
    let task = snapshot()
        .into_iter()
        .find(|t| t.name == "radar alerts under test")
        .unwrap();
    assert_eq!((task.ok_total, task.failed_total), (1, 1));
    assert_eq!(
        task.last_err_reason.as_deref(),
        Some("NINA, USGS check failed: NINA fetch timed out")
    );
}

#[test]
fn known_nina_ags_resolves_berlin_dashboard_key() {
    assert_eq!(known_nina_ags("Berlin"), Some("110000000000"));
}

#[test]
fn nina_active_key_groups_versioned_messages() {
    assert_eq!(
        nina_active_key("mow.DE-BE-B-SE017-20260620-017-000"),
        "mow.DE-BE-B-SE017-20260620-017"
    );
    assert_eq!(nina_active_key("custom-id"), "custom-id");
}

#[test]
fn nina_skips_dwd_weather_warning_ids() {
    assert!(is_nina_dwd_weather_warning(
        "dwd.2.49.0.0.276.0.DWD.PVW.1782032160000.33c52a9b-797d-4677-83c2-78cecbfce586.MUL"
    ));
    assert!(is_nina_dwd_weather_warning("mow.DE-BE-B-SE017.DWD.test"));
    assert!(!is_nina_dwd_weather_warning(
        "mow.DE-BE-B-SE017-20260620-017-000"
    ));
}

#[test]
fn a_warning_that_comes_back_is_posted_again() {
    let stable = "dwd:warnings:Berlin:WIND:3";
    let room = "!room:matrix.org";
    let txn = |key: &str| stable_alert_transaction_id(key, room);
    let storm = dwd_post_key(stable, 1_780_000_000_000, None);
    // Retried (one room failed): the same transaction ID.
    assert_eq!(
        txn(&storm),
        txn(&dwd_post_key(stable, 1_780_000_000_000, None))
    );
    // The same warning a week later: a new one, not swallowed.
    let next_week = dwd_post_key(stable, 1_780_604_800_000, None);
    assert_ne!(txn(&storm), txn(&next_week));
    // A repost of a deleted message: new, and new again for the next.
    let repost = dwd_post_key(stable, 1_780_000_000_000, Some(r#"{"!r":"$a"}"#));
    let again = dwd_post_key(stable, 1_780_000_000_000, Some(r#"{"!r":"$b"}"#));
    assert_ne!(txn(&storm), txn(&repost));
    assert_ne!(txn(&repost), txn(&again));
    // NINA: each version of a warning has its own key.
    let first = serde_json::json!({"sent": "2026-06-20T10:00:00+02:00"});
    let later = serde_json::json!({"sent": "2026-06-27T10:00:00+02:00"});
    assert_ne!(
        txn(&nina_alert_key(
            "mow.DE-BE-B-SE017-20260620-017-000",
            &first
        )),
        txn(&nina_alert_key(
            "mow.DE-BE-B-SE017-20260627-018-000",
            &later
        ))
    );
}

fn temp_db(_what: &str) -> (Db, tempfile::TempDir) {
    crate::testing::test_db()
}

const STORM: &str = "dwd:warnings:Berlin:WIND:3";
const ROOM: &str = "!room:matrix.org";
const START: i64 = 1_780_000_000_000;
const END: i64 = 1_780_010_000_000;
const LATER_END: i64 = 1_780_020_000_000;

/// Store `id` as delivered (the poll's `mark_dwd_alert_posted`).
async fn delivered(db: &Db, id: &str, first: &PostKey) {
    db.mark_dwd_alert_posted(
        id,
        "Sturmböen",
        "Berlin",
        "WIND",
        r#"{"!room:matrix.org":"$a"}"#,
        first.start_ms,
        first.end_ms,
        "plain",
        "html",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_dwd_retry_after_the_end_moved_sends_the_first_attempts_post() {
    let (db, _dir) = temp_db("dwd-retry");
    let txn = |key: &PostKey| stable_alert_transaction_id(&key.key, ROOM);
    // One room failed: not stored. DWD extends the warning before the retry.
    let first = dwd_occurrence_key(&db, STORM, START, END, None)
        .await
        .unwrap();
    assert_eq!(first.key, dwd_post_key(STORM, END, None));
    let retry = dwd_occurrence_key(&db, STORM, START + 60_000, LATER_END, None)
        .await
        .unwrap();
    // Same transaction ID: the room that has it gets no second copy.
    assert_eq!(txn(&retry), txn(&first));
    // Stored with the first attempt's window, so the next poll sees the
    // new end and edits every room's message to it.
    assert_eq!((retry.start_ms, retry.end_ms), (START, END));
    delivered(&db, STORM, &retry).await;
    let active = db.list_active_dwd_warnings().await.unwrap();
    assert_eq!(active[0].end_ms, END);
    assert_ne!(active[0].end_ms, LATER_END, "differs: edited next poll");
}

#[tokio::test]
async fn a_dwd_warning_that_comes_back_after_it_ended_is_posted_again() {
    let (db, _dir) = temp_db("dwd-back");
    let next_week = END + 7 * 86_400_000;
    // Delivered, then the all-clear: next week's storm is a new post.
    let first = dwd_occurrence_key(&db, STORM, START, END, None)
        .await
        .unwrap();
    delivered(&db, STORM, &first).await;
    db.mark_dwd_allclear_sent(STORM).await.unwrap();
    let again = dwd_occurrence_key(&db, STORM, START, next_week, None)
        .await
        .unwrap();
    assert_eq!(again.key, dwd_post_key(STORM, next_week, None));
    assert_ne!(again.key, first.key);

    // Never delivered everywhere: while it is in the feed the key stays…
    let (db, _dir2) = temp_db("dwd-back-partial");
    let first = dwd_occurrence_key(&db, STORM, START, END, None)
        .await
        .unwrap();
    let feed: HashSet<String> = [STORM.to_owned()].into();
    assert_eq!(db.forget_post_keys_except("dwd", &feed).await.unwrap(), 0);
    let retry = dwd_occurrence_key(&db, STORM, START, LATER_END, None)
        .await
        .unwrap();
    assert_eq!(retry.key, first.key);
    // …and it goes when the warning leaves the feed (it ended).
    assert_eq!(
        db.forget_post_keys_except("dwd", &HashSet::new())
            .await
            .unwrap(),
        1
    );
    let again = dwd_occurrence_key(&db, STORM, START, next_week, None)
        .await
        .unwrap();
    assert_ne!(again.key, first.key);
    // NINA's rows are not DWD's to drop.
    db.post_key("mow.DE-BE-1", "nina", "nina:v1", 0, 0)
        .await
        .unwrap();
    db.forget_post_keys_except("dwd", &HashSet::new())
        .await
        .unwrap();
    let nina = db.post_key("mow.DE-BE-1", "nina", "nina:v2", 0, 0).await;
    assert_eq!(nina.unwrap().key, "nina:v1");
}

#[tokio::test]
async fn dwd_level_changes_and_reposts_get_keys_of_their_own() {
    let (db, _dir) = temp_db("dwd-update");
    let first = dwd_occurrence_key(&db, STORM, START, END, None)
        .await
        .unwrap();
    // A level change while level 3 is still pending: level 4 is another
    // warning with its own key, as before.
    let level4 = "dwd:warnings:Berlin:WIND:4";
    let promoted = dwd_occurrence_key(&db, level4, START, END, None)
        .await
        .unwrap();
    assert_eq!(promoted.key, dwd_post_key(level4, END, None));
    assert_ne!(promoted.key, first.key);
    // Level 3 delivered, then promoted to 4 by an edit: no key is left
    // behind for either.
    delivered(&db, STORM, &first).await;
    db.promote_dwd_active_warning(
        STORM,
        level4,
        "Orkanböen",
        "Berlin",
        "WIND",
        "{}",
        START,
        END,
        "p",
        "h",
    )
    .await
    .unwrap();
    let later = dwd_occurrence_key(&db, level4, START, LATER_END, None)
        .await
        .unwrap();
    assert_eq!(later.key, dwd_post_key(level4, LATER_END, None));
    // A repost of a delivered warning (its message was deleted) is new.
    let (db, _dir2) = temp_db("dwd-repost");
    let first = dwd_occurrence_key(&db, STORM, START, END, None)
        .await
        .unwrap();
    delivered(&db, STORM, &first).await;
    let repost = dwd_occurrence_key(&db, STORM, START, END, Some(r#"{"!r":"$a"}"#))
        .await
        .unwrap();
    assert_ne!(repost.key, first.key);
    assert_eq!(repost.key, dwd_post_key(STORM, END, Some(r#"{"!r":"$a"}"#)));
}

#[tokio::test]
async fn a_nina_version_retried_after_a_partial_failure_keeps_the_first_key() {
    let (db, _dir) = temp_db("nina-retry");
    let id = "mow.DE-BE-B-SE017-20260620-017";
    let v1 = nina_alert_key(
        "mow.DE-BE-B-SE017-20260620-017-000",
        &json!({"sent": "2026-06-20T10:00:00+02:00"}),
    );
    let v2 = nina_alert_key(
        "mow.DE-BE-B-SE017-20260620-017-001",
        &json!({"sent": "2026-06-20T11:00:00+02:00"}),
    );
    db.post_key(id, NINA_POST_KEYS, &v1, 0, 0).await.unwrap();
    // v2 arrives before every room had v1: v1's key, no second copy.
    let retry = db.post_key(id, NINA_POST_KEYS, &v2, 0, 0).await.unwrap();
    assert_eq!(retry.key, v1);
    // Delivered: the next version is posted (or edited) as its own.
    db.mark_nina_alert_posted(id, "Warnung", "Berlin", "{}", "p", "h")
        .await
        .unwrap();
    let v3 = db.post_key(id, NINA_POST_KEYS, "nina:v3", 0, 0).await;
    assert_eq!(v3.unwrap().key, "nina:v3");
    // An all-clear ends it too.
    db.mark_nina_allclear_sent(id).await.unwrap();
    let v4 = db.post_key(id, NINA_POST_KEYS, "nina:v4", 0, 0).await;
    assert_eq!(v4.unwrap().key, "nina:v4");
}

#[test]
fn alert_transaction_id_is_stable_per_alert_and_room() {
    let a = stable_alert_transaction_id("dwd:warnings:Berlin:WIND:3", "!room:matrix.org");
    let b = stable_alert_transaction_id("dwd:warnings:Berlin:WIND:3", "!room:matrix.org");
    let other_alert =
        stable_alert_transaction_id("dwd:warnings:Berlin:GEWITTER:3", "!room:matrix.org");
    let other_room = stable_alert_transaction_id("dwd:warnings:Berlin:WIND:3", "!other:matrix.org");

    assert_eq!(a, b);
    assert_ne!(a, other_alert);
    assert_ne!(a, other_room);
}

#[test]
fn dwd_description_is_structured_and_concise() {
    let warning = json!({
        "start": 1781946000000i64,
        "end": 1781974800000i64,
        "event": "EXTREME HITZE",
        "level": 51,
        "description": "\nAm Samstag wird eine extreme Wärmebelastung erwartet.\nMit einer zusätzlichen Belastung aufgrund verringerter nächtlicher Abkühlung ist insbesondere im dicht bebauten Stadtgebiet von Berlin zu rechnen.\nNoch ein Satz, der nicht in die kurze Nachricht soll.\n",
        "instruction": "Hitzebelastung kann für den menschlichen Körper gefährlich werden. Vermeiden Sie nach Möglichkeit die Hitze, trinken Sie ausreichend Wasser und halten Sie die Innenräume kühl."
    });

    let desc = format_dwd_description_with_continuations("Berlin", &warning, &[]);

    assert!(desc.contains("📍 Berlin"));
    assert!(desc.contains("🕒"));
    assert!(desc.contains("ℹ️ Am Samstag wird eine extreme Wärmebelastung erwartet."));
    assert!(desc.contains("💡 Hitzebelastung kann für den menschlichen Körper gefährlich werden."));
    assert!(!desc.contains("Noch ein Satz"));
}

#[test]
fn dwd_heat_levels_are_not_labeled_as_unwetter() {
    assert_eq!(
        dwd_level_label_for_event("STARKE HITZE", 50),
        "Starke Hitzewarnung"
    );
    assert_eq!(
        dwd_level_label_for_event("EXTREME HITZE", 51),
        "Extreme Hitzewarnung"
    );

    let warning = json!({
        "event": "STARKE HITZE",
        "level": 50,
        "start": 1781974800000i64,
        "end": 1782061200000i64,
        "description": "Am Samstag wird eine starke Wärmebelastung erwartet.",
        "instruction": "Hitzebelastung kann gefährlich werden."
    });
    let desc = format_dwd_description_with_continuations("Berlin", &warning, &[]);

    assert!(desc.contains("🟠 Starke Hitzewarnung"));
    assert!(!desc.contains("Unwetter"));
}

#[test]
fn dwd_heat_events_share_lifecycle_key() {
    assert_eq!(dwd_event_key("EXTREME HITZE"), "HITZE");
    assert_eq!(dwd_event_key("STARKE HITZE"), "HITZE");

    let extreme = json!({"event": "EXTREME HITZE", "level": 51});
    let strong = json!({"event": "STARKE HITZE", "level": 50});

    assert_eq!(
        dwd_warning_stable_key("warnings", "Berlin", &extreme),
        "dwd:warnings:Berlin:HITZE:51"
    );
    assert_eq!(
        dwd_warning_stable_key("warnings", "Berlin", &strong),
        "dwd:warnings:Berlin:HITZE:50"
    );
}

#[test]
fn dwd_related_weather_events_share_lifecycle_families() {
    assert_eq!(dwd_event_key("STURMBÖEN"), "WIND");
    assert_eq!(dwd_event_key("SCHWERE STURMBÖEN"), "WIND");
    assert_eq!(dwd_event_key("ORKANBÖEN"), "WIND");
    assert_eq!(dwd_event_key("STARKREGEN"), "REGEN");
    assert_eq!(dwd_event_key("DAUERREGEN"), "REGEN");
    assert_eq!(dwd_event_key("SCHNEEFALL"), "SCHNEE_GLAETTE");
    assert_eq!(dwd_event_key("GLATTEIS"), "SCHNEE_GLAETTE");
    assert_eq!(dwd_event_key("FROST"), "FROST");
    assert_eq!(dwd_event_key("NEBEL"), "NEBEL");
    assert_eq!(dwd_event_key("TAUWETTER"), "TAUWETTER");
    assert_eq!(dwd_event_key("UV-STRAHLUNG"), "UV");
    assert_eq!(dwd_event_key("WALDBRANDGEFAHR"), "FIRE_RISK");
    assert_eq!(dwd_event_key("UNBEKANNTE WARNART"), "UNBEKANNTE WARNART");
}

#[test]
fn dwd_edit_error_classifies_missing_original_events() {
    assert!(looks_like_missing_matrix_event(
        "M_NOT_FOUND: event not found"
    ));
    assert!(looks_like_missing_matrix_event("unknown event id"));
    assert!(!looks_like_missing_matrix_event(
        "temporary network timeout"
    ));
}

#[test]
fn dwd_secondary_note_distinguishes_overlap_and_followup() {
    let primary = json!({
        "event": "EXTREME HITZE",
        "level": 51,
        "start": 1781946000000i64,
        "end": 1781974800000i64
    });
    let before = json!({
        "event": "STARKE HITZE",
        "level": 50,
        "start": 1781938800000i64,
        "end": 1781946000000i64
    });
    let overlapping = json!({
        "event": "STARKE HITZE",
        "level": 50,
        "start": 1781953200000i64,
        "end": 1782061200000i64
    });
    let followup = json!({
        "event": "STARKE HITZE",
        "level": 50,
        "start": 1781974800000i64,
        "end": 1782061200000i64
    });

    assert!(format_dwd_secondary_warning(&primary, &before).is_none());
    assert!(format_dwd_secondary_warning(&primary, &overlapping)
        .unwrap()
        .contains("(danach:"));
    assert!(format_dwd_secondary_warning(&primary, &followup)
        .unwrap()
        .contains("(danach:"));
}

#[test]
fn dwd_secondary_note_hides_superseded_thunderstorm_warnings() {
    let primary = json!({
        "event": "SCHWERES GEWITTER mit HEFTIGEM STARKREGEN und HAGEL",
        "level": 4,
        "start": 1782034800000i64,
        "end": 1782037800000i64
    });
    let covered = json!({
        "event": "STARKES GEWITTER",
        "level": 3,
        "start": 1782029460000i64,
        "end": 1782037800000i64
    });
    let continues_after = json!({
        "event": "STARKES GEWITTER",
        "level": 3,
        "start": 1782029460000i64,
        "end": 1782039600000i64
    });

    assert!(format_dwd_secondary_warning(&primary, &covered).is_none());
    let note = format_dwd_secondary_warning(&primary, &continues_after).unwrap();
    assert!(note.starts_with("(danach:"));
    assert!(note.contains("🟠 Starke Gewitterwarnung"));
}

#[test]
fn dwd_warnings_are_relevant_within_the_lead_time_vorab_always() {
    const HOUR: i64 = 3_600_000;
    let now = 100 * HOUR;
    let lead = 3 * HOUR;

    assert!(dwd_warning_is_relevant_now(
        "vorabInformation",
        now + 20 * HOUR,
        now + 30 * HOUR,
        now,
        lead
    ));
    assert!(dwd_warning_is_relevant_now(
        "warnings",
        now + 2 * HOUR,
        now + 5 * HOUR,
        now,
        lead
    ));
    assert!(!dwd_warning_is_relevant_now(
        "warnings",
        now + 4 * HOUR,
        now + 6 * HOUR,
        now,
        lead
    ));
    assert!(!dwd_warning_is_relevant_now(
        "vorabInformation",
        now - 5 * HOUR,
        now - HOUR,
        now,
        lead
    ));
}

#[test]
fn dwd_active_warning_ranks_before_future_vorabinformation() {
    let now = 1_500;
    let active_warning = dwd_primary_rank("warnings", 1_000, 2_000, 3, now);
    let future_vorab = dwd_primary_rank("vorabInformation", 2_500, 4_000, 4, now);

    assert!(active_warning > future_vorab);
}

#[test]
fn dwd_vorabinformation_label_is_not_active_warning_label() {
    let vorab = json!({
        "_radar_bucket": "vorabInformation",
        "event": "SCHWERES GEWITTER",
        "level": 4,
        "start": 1782043200000i64,
        "end": 1782064800000i64
    });

    assert_eq!(
        dwd_level_label_for_warning(&vorab),
        "Vorabinformation Unwetter"
    );
}

#[test]
fn concise_sentences_keeps_closing_parenthesis_after_exclamation() {
    let text =
        "Gefahr durch: Blitzschlag (Lebensgefahr!), umherfliegende Gegenstände und Aquaplaning.";

    let shortened = concise_sentences(text, 1).expect("sentence");

    assert_eq!(shortened, "Gefahr durch: Blitzschlag (Lebensgefahr!)");
}

#[test]
fn dwd_polygon_geometry_contains_point() {
    let geometry = json!({
        "type": "Polygon",
        "coordinates": [[
            [13.0, 52.0],
            [14.0, 52.0],
            [14.0, 53.0],
            [13.0, 53.0],
            [13.0, 52.0]
        ]]
    });

    assert!(geometry_contains_point(&geometry, (52.5, 13.5)));
    assert!(!geometry_contains_point(&geometry, (51.5, 13.5)));
}

#[test]
fn dwd_polygon_hole_excludes_point() {
    let geometry = json!({
        "type": "Polygon",
        "coordinates": [
            [
                [13.0, 52.0],
                [14.0, 52.0],
                [14.0, 53.0],
                [13.0, 53.0],
                [13.0, 52.0]
            ],
            [
                [13.4, 52.4],
                [13.6, 52.4],
                [13.6, 52.6],
                [13.4, 52.6],
                [13.4, 52.4]
            ]
        ]
    });

    assert!(!geometry_contains_point(&geometry, (52.5, 13.5)));
    assert!(geometry_contains_point(&geometry, (52.2, 13.2)));
}

#[test]
fn alert_html_preserves_lines_and_adds_link() {
    let (plain, html) = format_alert(
        "DWD Weather Warnings",
        "Amtliche WARNUNG vor HITZE",
        "📍 Berlin\n💡 Drink water",
        Some("https://www.dwd.de/"),
    );

    assert!(plain.contains("🚨 Amtliche WARNUNG vor HITZE"));
    assert!(plain.contains("📍 Berlin\n💡 Drink water\n🔗 https://www.dwd.de/"));
    assert!(html.contains("📍 Berlin<br>💡 Drink water"));
    assert!(html.contains("🔗 <a href=\"https://www.dwd.de/\">https://www.dwd.de/</a>"));
    assert!(!html.contains("DWD level"));
}

#[test]
fn dwd_allclear_edit_appends_below_hr() {
    let orig_plain = "🚨 Amtliche WARNUNG vor HITZE\n📍 Berlin\nSource: DWD Weather Warnings";
    let orig_html =
            "<b>🚨 Amtliche WARNUNG vor HITZE</b><br>📍 Berlin<br><em>Source: DWD Weather Warnings</em>";

    let (plain, html) = append_allclear_to_warning(orig_plain, orig_html);

    // Original content preserved
    assert!(plain.starts_with(orig_plain));
    assert!(html.starts_with(orig_html));
    // Separator present
    assert!(plain.contains("\n\n---\n"));
    assert!(html.contains("<hr>"));
    // All-clear marker present
    assert!(plain.contains("✅ Entwarnung"));
    assert!(html.contains("✅ Entwarnung"));
    assert!(plain.contains("gilt nicht mehr"));
}

#[test]
fn dwd_level_change_footer_is_separated_and_mentions_new_event() {
    let orig_plain = "🚨 Amtliche WARNUNG vor STARKEM GEWITTER\nSource: DWD Weather Warnings";
    let orig_html =
        "<b>🚨 Amtliche WARNUNG vor STARKEM GEWITTER</b><br><em>Source: DWD Weather Warnings</em>";

    let (plain, html) = append_level_change_to_warning(
        orig_plain,
        orig_html,
        3,
        4,
        "SCHWERES GEWITTER mit HEFTIGEM STARKREGEN und HAGEL",
    );

    assert!(plain.contains("\n\n---\n⬆️ Warnung aktualisiert"));
    assert!(plain.contains("jetzt: SCHWERES GEWITTER mit HEFTIGEM STARKREGEN und HAGEL"));
    assert!(html.contains("</em><br><br><hr><em>⬆️ Warnung aktualisiert"));
    assert!(html.contains("HEFTIGEM STARKREGEN"));
}

#[test]
fn dwd_allclear_references_headline_and_region() {
    let (plain, html) = format_dwd_allclear("Amtliche WARNUNG vor HITZE", "Berlin, Stadt");

    assert!(plain.contains("✅ Entwarnung"));
    assert!(plain.contains("Amtliche WARNUNG vor HITZE"));
    assert!(plain.contains("Berlin, Stadt"));
    assert!(plain.contains("nicht mehr aktiv"));
    assert!(plain.contains("DWD Weather Warnings"));

    assert!(html.contains("✅ Entwarnung"));
    assert!(html.contains("Amtliche WARNUNG vor HITZE"));
    assert!(html.contains("Berlin, Stadt"));
    assert!(html.contains("nicht mehr aktiv"));
}

#[test]
fn dwd_allclear_escapes_html() {
    let (_, html) = format_dwd_allclear("WARNUNG <test>", "Region & Co.");

    assert!(!html.contains("<test>"));
    assert!(html.contains("&lt;test&gt;"));
    assert!(html.contains("Region &amp; Co."));
}

#[test]
fn nina_key_changes_when_warning_is_updated() {
    let first = json!({
        "sent": "2026-06-20T10:00:00+02:00",
        "effective": "2026-06-20T11:00:00+02:00",
        "expires": "2026-06-20T19:00:00+02:00",
        "severity": "Severe"
    });
    let updated = json!({
        "sent": "2026-06-20T12:00:00+02:00",
        "effective": "2026-06-20T11:00:00+02:00",
        "expires": "2026-06-20T20:00:00+02:00",
        "severity": "Severe"
    });

    assert_ne!(
        nina_alert_key("warn-1", &first),
        nina_alert_key("warn-1", &updated)
    );
}

#[test]
fn the_warning_fixtures_parse_as_today() {
    let dwd = parse_dwd_jsonp(&crate::testing::fixture("warnings/dwd_warnings.json"))
        .expect("JSONP unwrapped");
    let keywords = vec!["Yourcity".to_owned()];
    let mine: Vec<&serde_json::Value> = dwd["warnings"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|v| v.as_array().unwrap())
        .filter(|w| matches_region(w["regionName"].as_str().unwrap(), &keywords))
        .collect();
    assert_eq!(mine.len(), 1);
    assert_eq!(
        dwd_warning_stable_key("warnings", "Yourcity", mine[0]),
        "dwd:warnings:Yourcity:FROST:2"
    );
    let early = &dwd["vorabInformation"]["100000001"][0];
    assert_eq!(dwd_event_key(early["event"].as_str().unwrap()), "WIND");

    let nina: serde_json::Value =
        serde_json::from_str(&crate::testing::fixture("warnings/nina_dashboard.json")).unwrap();
    let ids: Vec<&str> = nina
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["id"].as_str().unwrap())
        .collect();
    assert!(!is_nina_dwd_weather_warning(ids[0]));
    assert!(is_nina_dwd_weather_warning(ids[1]));
    assert_eq!(nina_active_key(ids[0]), "mow.DE-XX-EX-W001-20261120");
    let description = format_nina_description(&nina[0]["payload"]["data"]);
    assert!(
        description.contains("📍 Yourcity, YourDistrict"),
        "{description}"
    );
    assert!(description.contains("💡 Halten Sie Fenster und Türen geschlossen."));
}

/// The rooms of a test: posts and edits are recorded and succeed.
#[derive(Default)]
struct Recorder {
    posts: std::sync::Mutex<Vec<(String, String, bool)>>,
    edits: std::sync::Mutex<Vec<String>>,
}

impl AlertRooms for Recorder {
    async fn post(
        &self,
        alert_key: &str,
        plain: &str,
        _html: &str,
        reply_to: &HashMap<String, String>,
    ) -> (bool, HashMap<String, String>) {
        let mut posts = self.posts.lock().unwrap();
        posts.push((alert_key.to_owned(), plain.to_owned(), !reply_to.is_empty()));
        let event = format!("$event{}", posts.len());
        (true, HashMap::from([("!room:test".to_owned(), event)]))
    }

    async fn edit(&self, _event_ids_json: &str, plain: &str, _html: &str) -> DwdEditResult {
        self.edits.lock().unwrap().push(plain.to_owned());
        DwdEditResult::Ok
    }

    async fn post_untracked(&self, _plain: &str, _html: &str) -> bool {
        true
    }
}

/// A DWD file with one active warning of `level` and `event` for the test city.
fn dwd_file(level: i64, event: &str, start_ms: i64, end_ms: i64) -> String {
    let file = json!({
        "time": start_ms,
        "warnings": {"100000001": [{
            "state": "Yourstate", "type": 2, "level": level,
            "start": start_ms, "end": end_ms,
            "regionName": "Yourcity", "event": event,
            "headline": format!("Amtliche WARNUNG vor {event}"),
            "description": "Es treten Sturmböen auf.", "instruction": "",
        }]},
        "vorabInformation": {},
    });
    format!("warnWetter.loadWarnings({file});")
}

#[tokio::test]
async fn a_warning_that_rises_in_level_is_posted_again_and_a_lower_one_only_edited() {
    let (db, _dir) = crate::testing::test_db();
    let rooms = Recorder::default();
    let now = chrono::Utc::now().timestamp_millis();
    let (start, end) = (now - 3_600_000, now + 5 * 3_600_000);
    let mut cache = HashMap::new();
    let round = |file: String| {
        let (http, _) = recorded(&[(DWD_WARNINGS_URL, 200, &file)]);
        (http, file)
    };
    let regions = vec!["Yourcity".to_owned()];
    let lead = std::time::Duration::from_secs(3 * 3600);

    for (level, event) in [
        (2, "STURMBÖEN"),
        (3, "SCHWERE STURMBÖEN"),
        (3, "SCHWERE STURMBÖEN"),
        (2, "STURMBÖEN"),
    ] {
        let (http, _) = round(dwd_file(level, event, start, end));
        check_dwd_weather_warnings(&rooms, &http, &db, &regions, lead, None, &mut cache, false)
            .await
            .unwrap();
        let posts = rooms.posts.lock().unwrap().len();
        let edits = rooms.edits.lock().unwrap().len();
        match (level, posts, edits) {
            // 2: posted; 3: escalation post and edit; 3 again: nothing; 2: edit only.
            (2, 1, 0) | (3, 2, 1) | (2, 2, 2) => {}
            other => panic!("after level {level}: (level, posts, edits) = {other:?}"),
        }
    }

    let posts = rooms.posts.lock().unwrap();
    let (key, plain, is_reply) = &posts[1];
    assert!(key.ends_with(":up:3"), "{key}");
    assert!(*is_reply, "the escalation replies to the original");
    assert!(
        plain.starts_with("⬆️ 🟠 Starke Warnung: SCHWERE STURMBÖEN"),
        "{plain}"
    );
    assert!(plain.contains("wurde verschärft."), "{plain}");
    assert_eq!(plain.lines().count(), 3);
}

#[test]
fn a_nina_update_escalates_only_when_the_severity_rose() {
    assert!(nina_severity_rose(Some("Moderate"), Some("Severe")));
    assert!(nina_severity_rose(Some("Severe"), Some("Extreme")));
    assert!(!nina_severity_rose(Some("Severe"), Some("Moderate")));
    assert!(!nina_severity_rose(Some("Severe"), Some("Severe")));
    // A row from before the severity was stored: the first update has not risen.
    assert!(!nina_severity_rose(None, Some("Extreme")));
    assert_eq!(
        nina_escalation_key("nina:abc:1", "Severe"),
        "nina:abc:1:up:Severe"
    );
    let (plain, _) = format_nina_escalation(
        &json!({"severity": "Extreme", "effective": "2026-11-27T14:00:00+01:00"}),
        "Großbrand",
        0,
    );
    assert!(
        plain.starts_with("⬆️ 🔴 Extreme Gefahr: Großbrand"),
        "{plain}"
    );
}

#[test]
fn quakes_are_off_by_default_and_asked_every_fifteenth_round() {
    assert!(!crate::config::EmergencyConfig::default().quakes);
    let asked: Vec<u64> = (0..31).filter(|n| quake_round(true, *n)).collect();
    assert_eq!(asked, vec![0, 15, 30]);
    assert!((0..31).all(|n| !quake_round(false, n)));
}

#[tokio::test]
async fn without_a_home_profile_the_dwd_file_is_still_asked_for() {
    let (db, _dir) = crate::testing::test_db();
    let recorded = std::sync::Arc::new(Recorded::new());
    let (_publish, home) = tokio::sync::watch::channel(None);
    let config = AlertConfig {
        home,
        dwd_region_keywords: vec!["Yourcity".to_owned()],
        dwd_lead: std::time::Duration::from_secs(3 * 3600),
        max_distance_km: None,
        poll_interval_secs: 3600,
        quakes: false,
    };
    let alive = mxbot_common::supervise::report("radar alerts, no profile under test");

    let task = tokio::spawn(alert_loop(
        Recorder::default(),
        Http::recorded(recorded.clone()),
        db,
        config,
        alive,
    ));
    for _ in 0..500 {
        if !recorded.calls().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    task.abort();

    assert_eq!(recorded.calls(), vec![format!("GET {DWD_WARNINGS_URL}")]);
}
