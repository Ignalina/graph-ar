use std::{fs::File, path::Path};

use arrow_array::RecordBatch;
use orc_rust::{ArrowReaderBuilder, ArrowWriterBuilder};

use crate::error::Result;

pub fn read(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = ArrowReaderBuilder::try_new(file)?.build();
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
    let file = File::create(path)?;
    let mut writer = ArrowWriterBuilder::new(file, batches[0].schema()).try_build()?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}
