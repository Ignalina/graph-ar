pub mod csv;
pub mod ipc;
pub mod json;
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
        FileType::Orc => orc::read(path),
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
        FileType::Orc => orc::write(path, batches),
        FileType::ArrowIpc => ipc::write(path, batches),
    }
}
