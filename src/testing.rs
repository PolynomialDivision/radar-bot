//! Test helpers: a store in a temporary directory, the recorded fixtures, golden files.

use std::{path::PathBuf, sync::Arc};

use crate::{
    fetch::{Http, Recorded},
    store::Db,
};

/// A fresh store in a temporary directory, removed when the guard drops.
pub(crate) fn test_db() -> (Db, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temporary directory");
    let db = Db::open(&dir.path().join("items.db")).expect("open test store");
    (db, dir)
}

/// `tests/fixtures/<path>`.
pub(crate) fn fixture_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(path)
}

/// The text of `tests/fixtures/<path>`.
pub(crate) fn fixture(path: &str) -> String {
    std::fs::read_to_string(fixture_path(path)).unwrap_or_else(|e| panic!("fixture {path}: {e}"))
}

/// Every request answered from `tests/fixtures/routes.toml`.
pub(crate) fn fixture_http() -> (Http, Arc<Recorded>) {
    let recorded =
        Arc::new(Recorded::from_dir(&fixture_path("")).expect("tests/fixtures/routes.toml"));
    (Http::recorded(recorded.clone()), recorded)
}

/// `actual` must equal `tests/golden/<name>`. With `UPDATE_GOLDEN=1` the file is written
/// instead: a changed golden file is a decision, reviewed in the diff.
pub(crate) fn assert_golden(name: &str, actual: &str) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    if std::env::var("UPDATE_GOLDEN").is_ok_and(|v| v == "1") {
        std::fs::write(&path, actual).expect("write golden file");
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "golden file {}: {e} (UPDATE_GOLDEN=1 writes it)",
            path.display()
        )
    });
    assert!(
        expected == actual,
        "{name} differs from the golden file (UPDATE_GOLDEN=1 rewrites it)\n--- expected\n{expected}\n--- actual\n{actual}"
    );
}
