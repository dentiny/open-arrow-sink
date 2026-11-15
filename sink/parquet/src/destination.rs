use std::{io, path::PathBuf};

use open_arrow_sink_core::{Result, SchemaRef, WriteMode};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs;

use crate::error::backend;

/// Overwrite stages a complete directory; append owns only its unique files.
/// Callers must exclude other writes or directory replacement for the session lifetime.
pub(crate) struct Destination {
    pub directory: PathBuf,
    pub prefix: String,
    pub remove_directory: bool,
    target: Option<PathBuf>,
    cleanup_on_error: bool,
}

impl Destination {
    pub async fn open(
        path: PathBuf,
        schema: &SchemaRef,
        mode: WriteMode,
        cleanup_on_error: bool,
    ) -> Result<Self> {
        match mode {
            WriteMode::Create => {
                fs::create_dir(&path).await.map_err(backend)?;
                Ok(Self {
                    directory: path,
                    prefix: "part-".into(),
                    remove_directory: true,
                    target: None,
                    cleanup_on_error,
                })
            }
            WriteMode::Append => {
                let mut entries = fs::read_dir(&path).await.map_err(backend)?;
                while let Some(entry) = entries.next_entry().await.map_err(backend)? {
                    if entry.path().extension().is_some_and(|ext| ext == "parquet") {
                        let file = fs::File::open(entry.path()).await.map_err(backend)?;
                        let reader = ParquetRecordBatchStreamBuilder::new(file)
                            .await
                            .map_err(backend)?;
                        if reader.schema() != schema {
                            return Err(open_arrow_sink_core::Error::SchemaMismatch);
                        }
                    }
                }
                Ok(Self {
                    directory: path,
                    prefix: format!("part-{}-", uuid::Uuid::new_v4()),
                    remove_directory: false,
                    target: None,
                    cleanup_on_error,
                })
            }
            WriteMode::Overwrite => {
                match fs::symlink_metadata(&path).await {
                    Ok(metadata) if !metadata.is_dir() => {
                        return Err(backend(io::Error::other(
                            "destination must be a directory, not a file or symlink",
                        )));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(backend(error)),
                }
                let directory =
                    path.with_file_name(format!(".open-arrow-sink-{}", uuid::Uuid::new_v4()));
                fs::create_dir(&directory).await.map_err(backend)?;
                Ok(Self {
                    directory,
                    prefix: "part-".into(),
                    remove_directory: true,
                    target: Some(path),
                    cleanup_on_error,
                })
            }
        }
    }

    pub async fn publish(&self) -> Result<()> {
        let result = self.publish_inner().await;
        if result.is_err() && self.cleanup_on_error && self.target.is_some() {
            // A failed publication owns its cleanup; parallel callers must not
            // abort writers after entering the session committer.
            let _ = fs::remove_dir_all(&self.directory).await;
        }
        result
    }

    async fn publish_inner(&self) -> Result<()> {
        let Some(target) = &self.target else {
            return Ok(());
        };
        let backup =
            target.with_file_name(format!(".open-arrow-sink-old-{}", uuid::Uuid::new_v4()));
        let had_target = match fs::rename(target, &backup).await {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(backend(error)),
        };
        if let Err(error) = fs::rename(&self.directory, target).await {
            if had_target {
                let _ = fs::rename(&backup, target).await;
            }
            return Err(backend(error));
        }
        // Publication succeeded. A backup cleanup failure must not report a failed write.
        if had_target {
            let _ = fs::remove_dir_all(backup).await;
        }
        Ok(())
    }
}
