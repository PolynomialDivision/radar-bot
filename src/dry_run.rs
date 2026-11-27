//! `radar-bot --dry-run`: one poll round and the digest it would post, printed to stdout.
//!
//! It never logs in to Matrix and never touches the bot's own store: it works on an empty
//! database in a temporary directory, or on a copy of `--db FILE`. With `--fixtures DIR` every
//! request is answered from `DIR/routes.toml` instead of the network. Every item is decided in
//! this round (there is no next one to retry in). Logs go to stderr.

use crate::polling::PollCtx;
use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};

use crate::{
    apply_profile, bluesky_context,
    digest::{digest_header, prepare_digest, DigestChunk},
    fetch::{Http, Recorded},
    http_client,
    polling::poll_once,
    profile,
    store::Db,
    Config,
};

/// Logging to stderr, so stdout carries the digest alone.
pub(crate) fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("radar_bot=info,warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

pub(crate) async fn run(config: Config, fixtures: Option<&Path>, db: Option<&Path>) -> Result<()> {
    let http = match fixtures {
        Some(dir) => Http::recorded(Arc::new(Recorded::from_dir(dir)?)),
        None => Http::live(http_client()?, config.http.agents()),
    };
    let scratch = tempfile::tempdir().context("Creating a temporary directory")?;
    let path = scratch.path().join("items.db");
    if let Some(source) = db {
        copy_database(source, &path)?;
    }
    let db = Db::open(&path)?;

    let chunks = round(config, &http, &db, &digest_header()).await?;
    if chunks.is_empty() {
        println!("(no digest: nothing passed the filters)");
    }
    for (i, chunk) in chunks.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{}", chunk.plain);
    }
    Ok(())
}

/// One poll round on `db` and the digest it leads to, every item decided now.
async fn round(mut config: Config, http: &Http, db: &Db, header: &str) -> Result<Vec<DigestChunk>> {
    let home = match profile::HomeConfig::of(&config.filter) {
        Some(home) => Some(
            profile::home_profile_once(db, http, &home)
                .await
                .map_err(|e| anyhow::anyhow!("Reference address: {e}"))?,
        ),
        None => None,
    };
    let ref_point = apply_profile(home.as_ref(), &mut config.filter);
    let bluesky = bluesky_context(&config);
    let ctx = PollCtx::new(http.clone(), config.filter.clone(), ref_point);
    poll_once(&ctx, db, &config.sources, bluesky.as_ref(), true).await;
    prepare_digest(
        db,
        config.filter.digest_threshold,
        config.filter.candidate_min_sources,
        config.filter.candidate_retention_hours,
        header,
    )
    .await
}

/// A consistent copy of the database at `source` (its WAL included), opened read-only.
fn copy_database(source: &Path, target: &Path) -> Result<()> {
    use mxbot_common::sqlite::rusqlite::{Connection, OpenFlags};
    let conn = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("Opening {} read-only", source.display()))?;
    conn.execute("VACUUM INTO ?1", [target.to_string_lossy()])
        .with_context(|| format!("Copying {}", source.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `--dry-run --fixtures tests/fixtures config.toml.example`, without printing.
    #[tokio::test]
    async fn the_example_configuration_runs_offline_on_the_fixtures() {
        let example =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.toml.example");
        let config: Config = mxbot_common::config::load_toml(&example).unwrap();
        let (http, recorded) = crate::testing::fixture_http();
        let (db, _dir) = crate::testing::test_db();

        let chunks = round(config, &http, &db, "Dry run").await.unwrap();

        let digest: String = chunks.iter().map(|c| c.plain.as_str()).collect();
        // The police report and the broadcaster's article are one story (which headline leads
        // depends on which fetch finished first); the Bluesky post and the Google News items are
        // older than the example's max_age_hours (fixed fixture dates).
        let fire: Vec<&str> = digest
            .lines()
            .filter(|l| l.contains("in Wohnhaus"))
            .collect();
        assert_eq!(fire.len(), 1, "one story: {fire:?}");
        assert!(digest.contains("· 👥 2 sources"));
        for expected in [
            "Verkehrsunfall auf der Britzer Allee",
            "Konzert in Yourcity",
            "Hoffest im Hinterhof",
            "Leserbrief: Wohin mit den Lastenrädern?",
        ] {
            assert!(
                digest.contains(expected),
                "{expected:?} missing from\n{digest}"
            );
        }
        for unexpected in [
            "Bundestag debattiert",
            "Einbruch in Ladengeschäft in Othertown",
            "Senat beschließt",
        ] {
            assert!(!digest.contains(unexpected), "{unexpected:?} in\n{digest}");
        }
        let calls = recorded.calls();
        assert!(calls
            .iter()
            .any(|c| c.starts_with("POST https://bsky.social/")));
        assert!(calls
            .iter()
            .any(|c| c == "GET https://example.com/police/1001"));
    }
}
