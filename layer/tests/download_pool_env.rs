#![cfg(feature = "download-blob")]

use std::process::Command;

use open_arrow_sink_layer::DownloadPool;
use tokio::sync::Semaphore;

#[test]
fn global_pool_reads_concurrency_from_environment() {
    const EXPECTED: &str = "DOWNLOAD_POOL_TEST_EXPECTED_CONCURRENCY";
    const CONCURRENCY: &str = "OPEN_ARROW_SINK_DOWNLOAD_CONCURRENCY";
    if let Ok(expected) = std::env::var(EXPECTED) {
        let pool = DownloadPool::global();
        if expected == "error" {
            assert!(matches!(
                pool,
                Err(open_arrow_sink_core::Error::Unsupported(_))
            ));
        } else {
            let pool = pool.unwrap();
            assert_eq!(pool.concurrency(), expected.parse::<usize>().unwrap());
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
    for (value, expected) in [
        (None, "64"),
        (Some("128"), "128"),
        (Some("0"), "64"),
        (Some("invalid"), "64"),
        (Some(oversized.as_str()), "error"),
    ] {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "global_pool_reads_concurrency_from_environment"])
            .env(EXPECTED, expected)
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
