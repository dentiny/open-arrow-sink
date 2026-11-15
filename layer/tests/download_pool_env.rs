#![cfg(feature = "download-blob")]

use std::{num::NonZeroUsize, process::Command};

use open_arrow_sink_layer::DownloadPool;
use tokio::sync::Semaphore;

#[test]
fn global_pool_reads_concurrency_from_environment() {
    const EXPECTED: &str = "DOWNLOAD_POOL_TEST_EXPECTED_CONCURRENCY";
    const CONCURRENCY: &str = "OPEN_ARROW_SINK_DOWNLOAD_CONCURRENCY";
    const EXPLICIT_FIRST: &str = "DOWNLOAD_POOL_TEST_EXPLICIT_FIRST";
    if let Ok(expected) = std::env::var(EXPECTED) {
        let explicit = if std::env::var(EXPLICIT_FIRST).as_deref() == Ok("true") {
            Some(DownloadPool::new(
                NonZeroUsize::new(3).unwrap(),
                NonZeroUsize::new(8).unwrap(),
            ))
        } else {
            None
        };
        let pool = DownloadPool::global();
        if expected == "error" {
            assert!(matches!(
                pool,
                Err(open_arrow_sink_core::Error::Unsupported(_))
            ));
        } else {
            let pool = pool.unwrap();
            let expected = expected.parse::<usize>().unwrap();
            assert_eq!(pool.concurrency(), expected);
            if let Some(explicit) = explicit {
                assert_eq!(explicit.unwrap().concurrency(), expected);
            }
            let later = DownloadPool::new(
                NonZeroUsize::new(9).unwrap(),
                NonZeroUsize::new(16).unwrap(),
            )
            .unwrap();
            assert_eq!(later.concurrency(), expected);
            assert_eq!(pool.max_buffered_bytes(), 8 * 1024 * 1024 * 1024);
            assert!(std::sync::Arc::ptr_eq(
                &pool,
                &DownloadPool::global().unwrap()
            ));
        }
        return;
    }

    // Isolate process environment and OnceLock state without mutating this
    // multithreaded test process's environment.
    let oversized = (Semaphore::MAX_PERMITS + 1).to_string();
    for (value, explicit_first, expected) in [
        (None, false, "64"),
        (Some("128"), false, "128"),
        (Some("0"), false, "64"),
        (Some("invalid"), false, "64"),
        (Some(oversized.as_str()), false, "error"),
        (None, true, "3"),
        (Some("128"), true, "128"),
        (Some("0"), true, "3"),
        (Some("invalid"), true, "3"),
    ] {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "global_pool_reads_concurrency_from_environment"])
            .env(EXPECTED, expected)
            .env(EXPLICIT_FIRST, explicit_first.to_string())
            .env_remove(CONCURRENCY);
        if let Some(value) = value {
            child.env(CONCURRENCY, value);
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "override {value:?}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
