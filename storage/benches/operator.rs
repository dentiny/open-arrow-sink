//! CPU microbenchmark plus filesystem metadata control. Setup and cleanup are untimed.
use open_arrow_sink_storage::{Storage, Url};
use std::{hint::black_box, time::Instant};

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("data");
        tokio::fs::write(&path, b"data").await.unwrap();
        let url = Url::from_file_path(path).unwrap();
        let storage = Storage::from_url(&url).unwrap();
        println!("operation,sample,iterations,nanoseconds_per_op");
        for sample in 0..=5 {
            let iterations = 1_000_000;
            let start = Instant::now();
            for _ in 0..iterations {
                black_box(black_box(&storage).operator());
            }
            let elapsed = start.elapsed();
            if sample > 0 {
                println!(
                    "operator,{sample},{iterations},{:.3}",
                    elapsed.as_nanos() as f64 / iterations as f64
                );
            }
        }
        for sample in 0..=5 {
            let iterations = 10_000;
            let start = Instant::now();
            for _ in 0..iterations {
                assert_eq!(
                    storage
                        .stat_optional(black_box(&url))
                        .await
                        .unwrap()
                        .unwrap()
                        .content_length(),
                    4
                );
            }
            let elapsed = start.elapsed();
            if sample > 0 {
                println!(
                    "stat_optional,{sample},{iterations},{:.3}",
                    elapsed.as_nanos() as f64 / iterations as f64
                );
            }
        }
    });
}
