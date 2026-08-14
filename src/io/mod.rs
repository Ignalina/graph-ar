pub mod csv;
pub mod ipc;
pub mod json;
#[cfg(feature = "orc")]
pub mod orc;
pub mod parquet;

use arrow_array::RecordBatch;
use std::path::Path;

use crate::{error::Result, types::FileType};

pub fn read_chunk(path: impl AsRef<Path>, file_type: &FileType) -> Result<Vec<RecordBatch>> {
    match file_type {
        FileType::Parquet => parquet::read(path),
        FileType::Csv => csv::read(path),
        FileType::Json => json::read(path),
        #[cfg(feature = "orc")]
        FileType::Orc => orc::read(path),
        #[cfg(not(feature = "orc"))]
        FileType::Orc => Err(crate::error::GraphArError::from(std::io::Error::other(
            "graphar was built without the `orc` feature — ORC is not compiled in. \
             Rebuild with `--features orc` (note: it pins arrow 58).",
        ))),
        FileType::ArrowIpc => ipc::read(path),
    }
}

pub fn write_chunk(
    path: impl AsRef<Path>,
    batches: &[RecordBatch],
    file_type: &FileType,
) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }
    match file_type {
        FileType::Parquet => parquet::write(path, batches),
        FileType::Csv => csv::write(path, batches),
        FileType::Json => json::write(path, batches),
        #[cfg(feature = "orc")]
        FileType::Orc => orc::write(path, batches),
        #[cfg(not(feature = "orc"))]
        FileType::Orc => Err(crate::error::GraphArError::from(std::io::Error::other(
            "graphar was built without the `orc` feature — ORC is not compiled in. \
             Rebuild with `--features orc` (note: it pins arrow 58).",
        ))),
        FileType::ArrowIpc => ipc::write(path, batches),
    }
}
