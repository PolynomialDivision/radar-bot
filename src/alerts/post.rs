//! Posting, editing and formatting warning messages.

use std::collections::HashMap;

use mxbot_common::{
    matrix_sdk::ruma::{
        events::room::message::{ReplacementMetadata, RoomMessageEventContent},
        OwnedEventId, OwnedTransactionId,
    },
    Bot,
};
use tracing::{error, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DwdEditResult {
    Ok,
    MissingOriginal,
    Failed,
}

pub(super) fn format_rfc3339_validity(start: Option<&str>, end: Option<&str>) -> Option<String> {
    let start = start
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Local));
    let end = end
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Local));

    match (start, end) {
        (Some(start), Some(end)) if start.date_naive() == end.date_naive() => Some(format!(
            "{}-{}",
            start.format("%a, %d %b %H:%M"),
            end.format("%H:%M")
        )),
        (Some(start), Some(end)) => Some(format!(
            "{} - {}",
            start.format("%a, %d %b %H:%M"),
            end.format("%a, %d %b %H:%M")
        )),
        (Some(start), None) => Some(format!("from {}", start.format("%a, %d %b %H:%M"))),
        (None, Some(end)) => Some(format!("until {}", end.format("%a, %d %b %H:%M"))),
        (None, None) => None,
    }
}

pub(super) fn concise_sentences(text: &str, max_sentences: usize) -> Option<String> {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return None;
    }

    let mut sentences = Vec::new();
    let mut start = 0usize;
    for (idx, ch) in text.char_indices() {
        if matches!(ch, '.' | '!' | '?') {
            let mut end = idx + ch.len_utf8();
            while end < text.len() {
                let Some(next) = text[end..].chars().next() else {
                    break;
                };
                if matches!(next, ')' | ']' | '}' | '"' | '\'' | '”' | '“' | '’' | '‘') {
                    end += next.len_utf8();
                } else {
                    break;
                }
            }
            let sentence = text[start..end].trim();
            if !sentence.is_empty() && !sentences.contains(&sentence) {
                sentences.push(sentence);
            }
            start = end;
            if sentences.len() >= max_sentences {
                break;
            }
        }
    }

    if sentences.is_empty() {
        Some(text)
    } else {
        Some(sentences.join(" "))
    }
}

/// Returns `(all_ok, room_id_str → event_id_str)`.
/// Collects event IDs from all rooms so they can be stored for future edits.
pub(super) async fn post_alert_to_rooms(
    client: &Bot,
    alert_key: &str,
    plain: &str,
    html: &str,
) -> (bool, HashMap<String, String>) {
    let mut event_ids: HashMap<String, String> = HashMap::new();
    let mut all_ok = true;
    for room in client.broadcast_rooms() {
        let txn_id = stable_alert_transaction_id(alert_key, room.room_id().as_str());
        // retried with the same transaction ID: a lost answer posts no second alert
        match mxbot_common::send::send_with_retry_txn(
            &room,
            RoomMessageEventContent::text_html(plain, html),
            &txn_id,
        )
        .await
        {
            Ok(event_id) => {
                event_ids.insert(room.room_id().to_string(), event_id.to_string());
            }
            Err(e) => {
                error!("Failed to post alert message to {}: {e}", room.room_id());
                all_ok = false;
            }
        }
    }
    (all_ok, event_ids)
}

pub(super) fn stable_alert_transaction_id(alert_key: &str, room_id: &str) -> OwnedTransactionId {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in alert_key
        .as_bytes()
        .iter()
        .chain(b"|".iter())
        .chain(room_id.as_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    OwnedTransactionId::from(format!("radar-alert-{hash:016x}"))
}

/// Edit the original alert message in each room.
/// `event_ids_json` is a JSON object mapping room_id strings to event_id strings.
/// Returns whether all edits succeeded, failed transiently, or likely targeted
/// messages that are gone from the room.
pub(super) async fn edit_alert_message_in_rooms(
    client: &Bot,
    event_ids_json: &str,
    new_plain: &str,
    new_html: &str,
) -> DwdEditResult {
    let map: HashMap<String, String> = match serde_json::from_str(event_ids_json) {
        Ok(m) => m,
        Err(e) => {
            warn!("Could not parse alert event_ids_json for edit: {e}");
            return DwdEditResult::Failed;
        }
    };

    let mut all_ok = true;
    let mut missing_original = false;
    for room in client.broadcast_rooms() {
        let room_id = room.room_id().to_string();
        let Some(event_id_str) = map.get(&room_id) else {
            continue;
        };
        let event_id = match event_id_str.parse::<OwnedEventId>() {
            Ok(id) => id,
            Err(e) => {
                warn!("Invalid event ID {event_id_str} for alert edit: {e}");
                all_ok = false;
                continue;
            }
        };
        let edited = RoomMessageEventContent::text_html(new_plain, new_html)
            .make_replacement(ReplacementMetadata::new(event_id, None));
        if let Err(e) = room.send(edited).await {
            error!("Failed to edit alert message in {room_id}: {e}");
            all_ok = false;
            if looks_like_missing_matrix_event(&e.to_string()) {
                missing_original = true;
            }
        }
    }
    if all_ok {
        DwdEditResult::Ok
    } else if missing_original {
        DwdEditResult::MissingOriginal
    } else {
        DwdEditResult::Failed
    }
}

pub(super) fn looks_like_missing_matrix_event(error: &str) -> bool {
    let e = error.to_lowercase();
    [
        "not found",
        "unknown event",
        "event not found",
        "m_not_found",
        "redacted",
        "deleted",
        "cannot replace",
    ]
    .iter()
    .any(|needle| e.contains(needle))
}

/// Append an all-clear notice to the original warning content.
/// The `<hr>` separator is well-supported in Matrix HTML clients.
pub(super) fn append_allclear_to_warning(
    original_plain: &str,
    original_html: &str,
) -> (String, String) {
    let time = chrono::Local::now().format("%H:%M").to_string();
    let new_plain =
        format!("{original_plain}\n\n---\n✅ Entwarnung ({time}): Diese Warnung gilt nicht mehr.");
    let new_html = format!(
        "{original_html}<hr><em>✅ Entwarnung ({time}): Diese Warnung gilt nicht mehr.</em>"
    );
    (new_plain, new_html)
}

pub(super) fn format_alert(
    source: &str,
    event: &str,
    description: &str,
    link: Option<&str>,
) -> (String, String) {
    let link_plain = link.map(|l| format!("\n🔗 {l}")).unwrap_or_default();
    let link_html = link
        .map(|l| {
            let escaped = alert_html_escape(l);
            format!("<br>🔗 <a href=\"{escaped}\">{escaped}</a>")
        })
        .unwrap_or_default();
    let event_html = alert_html_escape(event);
    let description_html = alert_html_escape(description).replace('\n', "<br>");
    let source_html = alert_html_escape(source);

    let plain = format!("🚨 {event}\n{description}{link_plain}\n📰 Source: {source}");
    let html = format!(
        "<b>🚨 {event_html}</b><br>{description_html}{link_html}<br><em>📰 Source: {source_html}</em>"
    );
    (plain, html)
}

pub(super) fn alert_html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
