use std::{fs::File, path::Path};

use arrow::ipc::{reader::FileReader, writer::FileWriter};
use arrow_array::RecordBatch;

use crate::error::Result;

pub fn read(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn write(path: impl AsRef<Path>, batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let schema = batches[0].schema();
    let file = File::create(path)?;
    let mut writer = FileWriter::try_new(file, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}
