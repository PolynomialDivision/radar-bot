//! Input from outside that is broken or built to hurt: every parser of pages and feeds returns,
//! quickly, on the stack of a tokio blocking thread. A stack overflow aborts the process (no
//! panic to catch), so the test runs each case on a thread of that size.

use std::time::{Duration, Instant};

use crate::extract::article::read_page;
use crate::feed::parse_feed_entries;

/// The stack of a tokio blocking thread.
const STACK: usize = 2 * 1024 * 1024;
const WITHIN: Duration = Duration::from_secs(2);

fn rss(items: &str) -> String {
    format!(
        r#"<?xml version="1.0"?><rss version="2.0"><channel><title>T</title>{items}</channel></rss>"#
    )
}

fn cases() -> Vec<(&'static str, Vec<u8>)> {
    let mut nested = String::from("<item><title>Tief</title><description>");
    nested.push_str(&"<x>".repeat(10_000));
    nested.push_str(&"</x>".repeat(10_000));
    nested.push_str("</description></item>");
    let laughs = r#"<?xml version="1.0"?>
<!DOCTYPE rss [
  <!ENTITY a "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa">
  <!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">
  <!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;">
  <!ENTITY d "&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;">
  <!ENTITY e "&d;&d;&d;&d;&d;&d;&d;&d;&d;&d;&d;&d;&d;&d;&d;&d;">
  <!ENTITY f "&e;&e;&e;&e;&e;&e;&e;&e;&e;&e;&e;&e;&e;&e;&e;&e;">
]>
<rss version="2.0"><channel><title>&f;</title><item><title>&f;</title></item></channel></rss>"#;
    vec![
        ("an empty body", Vec::new()),
        (
            "bytes that are not UTF-8",
            b"\xff\xfe\xfd<rss>\xc3\x28\xa0\xa1</rss>".to_vec(),
        ),
        (
            "one 2 MB tag",
            format!("<div class=\"{}\">Text</div>", "a".repeat(2 * 1024 * 1024)).into_bytes(),
        ),
        ("an internal entity definition", laughs.as_bytes().to_vec()),
        (
            "a feed cut in the middle of an item",
            rss("<item><title>Brand in der Musterstr")
                .split("</channel>")
                .next()
                .unwrap()
                .as_bytes()
                .to_vec(),
        ),
        (
            "a feed with 10,000 nested elements",
            rss(&nested).into_bytes(),
        ),
    ]
}

/// A parser under test.
type Parser = fn(&[u8]);

/// Runs `parse` on `input` on a thread with [`STACK`] and returns how long it took.
fn timed(input: Vec<u8>, parse: Parser) -> Duration {
    std::thread::Builder::new()
        .stack_size(STACK)
        .spawn(move || {
            let started = Instant::now();
            parse(&input);
            started.elapsed()
        })
        .expect("spawn")
        .join()
        .expect("returned without a panic")
}

#[test]
fn broken_and_hostile_input_returns_quickly() {
    let parsers: [(&str, Parser); 2] = [
        ("read_page", |input| {
            let html = crate::fetch::decode(input, Some("text/html"));
            let _ = read_page("https://example.com/a", &html, "Titel");
        }),
        ("parse_feed_entries", |input| {
            let _ = parse_feed_entries(input, Some("application/rss+xml"), "Test");
        }),
    ];
    for (parser, parse) in parsers {
        for (case, input) in cases() {
            let took = timed(input, parse);
            assert!(took < WITHIN, "{parser} on {case}: {took:?}");
        }
    }
}
