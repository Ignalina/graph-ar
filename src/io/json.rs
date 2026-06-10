use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow::json::{LineDelimitedWriter, ReaderBuilder, reader::infer_json_schema};

use crate::error::Result;

pub fn read(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let mut reader = BufReader::new(File::open(path.as_ref())?);
    let (schema, _) = infer_json_schema(&mut reader, None)?;
    reader.seek(SeekFrom::Start(0))?;

    let reader = ReaderBuilder::new(Arc::new(schema)).build(reader)?;
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
    let mut writer = LineDelimitedWriter::new(file);
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    Ok(())
}
