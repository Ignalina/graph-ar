use std::{
    fs::File,
    io::{BufReader, BufWriter, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use arrow::json::{LineDelimitedWriter, ReaderBuilder, reader::infer_json_schema};
use arrow_array::RecordBatch;

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
    let file = BufWriter::new(File::create(path)?);
    let mut writer = LineDelimitedWriter::new(file);
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    // Flush the BufWriter explicitly so any error surfaces here rather than
    // being swallowed on drop. `finish()` flushes the Arrow writer's own state;
    // `into_inner().flush()` drains the BufWriter into the file.
    writer.into_inner().flush()?;
    Ok(())
}
