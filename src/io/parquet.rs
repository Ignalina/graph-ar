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

use std::{fs::File, path::Path};

use arrow_array::RecordBatch;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::error::Result;

pub fn read(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Parquet writer settings for chunks: ZSTD at the default level, the GraphAr
/// C++ writer's default.
pub(crate) fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build()
}

pub fn write(path: impl AsRef<Path>, batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let schema = batches[0].schema();
    let file = File::create(path)?;
    let props = writer_properties();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{basic::Compression, file::reader::FileReader};
    use std::sync::Arc;

    /// Parquet chunks are ZSTD-compressed by default, as the GraphAr C++
    /// writer does (`WriterOptions::ParquetOption::compression`).
    #[test]
    fn parquet_chunks_default_to_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chunk0");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(0..1000))],
        )
        .unwrap();
        write(&path, &[batch]).unwrap();
        let reader =
            parquet::file::reader::SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        let col = reader.metadata().row_group(0).column(0).compression();
        assert!(matches!(col, Compression::ZSTD(_)), "got {col:?}");
    }
}
