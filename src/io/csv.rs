use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_csv::{ReaderBuilder, WriterBuilder};

use crate::error::Result;

pub fn read(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path.as_ref())?;

    // Infer schema from first 100 rows then re-read.
    let format = arrow_csv::reader::Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut File::open(path.as_ref())?, Some(100))?;

    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .build(file)?;

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
    let file = BufWriter::new(File::create(path)?);
    let mut writer = WriterBuilder::new().with_header(true).build(file);
    for batch in batches {
        writer.write(batch)?;
    }
    // Flush the BufWriter explicitly so a flush error (e.g. ENOSPC) surfaces as
    // an `Err` here rather than being silently swallowed when the BufWriter is
    // dropped. This mirrors `json::write`'s explicit drain.
    writer.into_inner().flush()?;
    Ok(())
}
