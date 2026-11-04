// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Roundtrip correctness and a light timing smoke for the chunk codec:
//! `write_chunk_bytes` then `read_chunk_bytes` across every `FileType`. Pure,
//! in-memory, no network, no container.

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use graphar::FileType;
use graphar::storage::{read_chunk_bytes, write_chunk_bytes};

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
        FileType::Csv,
        FileType::Json,
        #[cfg(feature = "orc")]
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
        assert_eq!(
            back[0].num_columns(),
            ncols,
            "{label}: column count drifted"
        );
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
