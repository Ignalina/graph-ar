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
    write_batches_to(file, batches)
}

/// Write `batches` as CSV into an arbitrary writer, then **flush it
/// explicitly** so a flush error (e.g. ENOSPC) surfaces here rather than being
/// silently swallowed when the buffer is dropped — mirroring `json::write`.
/// Without the explicit flush a partial/failed CSV write could be reported as
/// success. Generic over the writer so the flush-error path is unit-testable.
fn write_batches_to<W: Write>(w: W, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = WriterBuilder::new().with_header(true).build(w);
    for batch in batches {
        writer.write(batch)?;
    }
    writer.into_inner().flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::{self, Write};

    /// A writer that accepts all bytes but always fails on `flush`, emulating a
    /// device that errors only when the buffer is drained (e.g. ENOSPC).
    struct FlushFails;
    impl Write for FlushFails {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::Other, "flush failed"))
        }
    }

    fn sample() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap()]
    }

    #[test]
    fn flush_error_is_propagated_not_swallowed() {
        // FAIL-ON-BUG: the old code never flushed explicitly, so the drop-time
        // flush error was discarded and this returned Ok. PASS-ON-FIX: the
        // explicit `into_inner().flush()?` propagates the error.
        let res = write_batches_to(FlushFails, &sample());
        assert!(
            res.is_err(),
            "flush failure must propagate as an error, got {res:?}"
        );
    }

    #[test]
    fn successful_write_still_ok() {
        // A writer that succeeds on flush still returns Ok (no regression).
        let res = write_batches_to(Vec::<u8>::new(), &sample());
        assert!(res.is_ok());
    }
}
