pub mod parquet;
pub mod csv;

use std::path::Path;
use arrow_array::RecordBatch;

use crate::{error::Result, types::FileType};

pub fn read_chunk(path: impl AsRef<Path>, file_type: &FileType) -> Result<Vec<RecordBatch>> {
    match file_type {
        FileType::Parquet => parquet::read(path),
        FileType::Csv => csv::read(path),
        FileType::Json | FileType::Orc => Err(crate::error::GraphArError::UnsupportedFileType(
            file_type.extension().to_string(),
        )),
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
        FileType::Json | FileType::Orc => Err(crate::error::GraphArError::UnsupportedFileType(
            file_type.extension().to_string(),
        )),
    }
}
