//! Roundtrip correctness + a light perf-smoke for the graphar chunk codec —
//! `write_chunk_bytes` → `read_chunk_bytes` across EVERY `FileType`. Pure,
//! in-memory, no network, no container: safe in CI and a light baseline for the
//! "loop till times down" bench work (heavy runs stay on the bench box).
//!
//! Drives the codec IN CODE (AI-free at runtime) and emits a `chunk_roundtrip`
//! functional-status cell per format via graphar's `nornir-testmatrix` wrapper.

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use graphar::storage::{read_chunk_bytes, write_chunk_bytes};
use graphar::FileType;

fn sample_batch(rows: usize) -> RecordBatch {
    let ids = Int64Array::from_iter_values(0..rows as i64);
    let names = StringArray::from_iter_values((0..rows).map(|i| format!("n{i}")));
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ids), Arc::new(names)]).unwrap()
}

/// Every FileType preserves row + column count through a write→read roundtrip.
#[test]
fn chunk_roundtrip_all_filetypes_preserve_rows_and_columns() {
    let batch = sample_batch(500);
    let (total_rows, ncols) = (batch.num_rows(), batch.num_columns());

    for ft in [
        FileType::Parquet,
        FileType::ArrowIpc,
        FileType::Csv,
        FileType::Json,
        FileType::Orc,
    ] {
        let label = format!("{ft:?}");
        let bytes = write_chunk_bytes(std::slice::from_ref(&batch), &ft)
            .unwrap_or_else(|e| panic!("{label}: write failed: {e}"));
        assert!(!bytes.is_empty(), "{label}: wrote empty");

        let back = read_chunk_bytes(Bytes::from(bytes), &ft)
            .unwrap_or_else(|e| panic!("{label}: read failed: {e}"));
        let rows: usize = back.iter().map(|b| b.num_rows()).sum();

        assert_eq!(rows, total_rows, "{label}: row count drifted");
        assert_eq!(back[0].num_columns(), ncols, "{label}: column count drifted");

        // remote-observable health cell (no-op unless `--features testmatrix`)
        graphar::functional_status("chunk_roundtrip", &label, true, "rows+cols preserved");
    }
}

/// Light timing smoke over the binary through-line formats — a baseline number,
/// NOT a heavy bench (that's the quiet-box job). Prints µs/iter to stderr.
#[test]
fn chunk_roundtrip_light_timing() {
    let batch = sample_batch(1000);
    let n = 100u32;
    for ft in [FileType::Parquet, FileType::ArrowIpc] {
        let t = Instant::now();
        for _ in 0..n {
            let bytes = write_chunk_bytes(std::slice::from_ref(&batch), &ft).unwrap();
            let _ = read_chunk_bytes(Bytes::from(bytes), &ft).unwrap();
        }
        eprintln!(
            "graphar chunk roundtrip {ft:?}: {:?}/iter (1000-row write+read)",
            t.elapsed() / n
        );
    }
}
