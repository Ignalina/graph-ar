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

use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    basic::{Compression, Encoding, ZstdLevel},
    file::{metadata::PageIndexPolicy, properties::WriterProperties},
    schema::types::ColumnPath,
};

use crate::error::{GraphArError, Result};

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

/// Read rows `[offset, offset + limit)` of `path`, and only the named
/// `columns` (all columns when `None`).
///
/// This is the pushdown path: the parquet reader skips every row group and,
/// via the offset index the writer emits by default, every data page that
/// holds none of the requested rows, and decodes only the projected columns.
/// Cost is O(pages touched), not O(rows in the file) — a point lookup in a
/// 4M-row adjacency chunk reads a couple of pages instead of the chunk.
pub fn read_rows(
    path: impl AsRef<Path>,
    offset: usize,
    limit: usize,
    columns: Option<&[&str]>,
) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let meta = ArrowReaderMetadata::load(&file, page_index_options())?;
    read_rows_with(file, meta, offset, limit, columns)
}

fn page_index_options() -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional)
}

fn read_rows_with(
    file: File,
    meta: ArrowReaderMetadata,
    offset: usize,
    limit: usize,
    columns: Option<&[&str]>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta);
    if let Some(cols) = columns {
        let schema = builder.schema().clone();
        let idx = cols
            .iter()
            .map(|c| {
                schema
                    .index_of(c)
                    .map_err(|_| GraphArError::PropertyNotFound((*c).to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), idx);
        builder = builder.with_projection(mask);
    }
    let reader = builder.with_offset(offset).with_limit(limit).build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Parsed parquet footers (schema, row groups, page index) keyed by path, so
/// repeated point lookups into the same chunk file skip the footer read and
/// thrift decode. Entries are small (a few KB per chunk file) and immutable;
/// GraphAr chunks never change once written.
#[derive(Default)]
pub struct MetadataCache {
    inner: Mutex<HashMap<PathBuf, Arc<ArrowReaderMetadata>>>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// [`read_rows`] with the footer served from the cache after first use.
    pub fn read_rows(
        &self,
        path: impl AsRef<Path>,
        offset: usize,
        limit: usize,
        columns: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let cached = self.inner.lock().unwrap().get(path).cloned();
        let meta = match cached {
            Some(m) => m,
            None => {
                let m = Arc::new(ArrowReaderMetadata::load(&file, page_index_options())?);
                self.inner
                    .lock()
                    .unwrap()
                    .insert(path.to_path_buf(), m.clone());
                m
            }
        };
        read_rows_with(file, (*meta).clone(), offset, limit, columns)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Parquet writer settings for chunks: ZSTD at the default level (the GraphAr
/// C++ writer's default), and integer columns (`_graphAr*` indexes, offsets,
/// integer properties) `DELTA_BINARY_PACKED` with the dictionary off.
///
/// The default dictionary encoding puts a dictionary page of up to 1 MiB in
/// front of every column chunk of ids (a million distinct values before the
/// size fallback), and a point lookup has to read and decompress that page
/// before it can decode the one page it wants. Delta packing has no such
/// preamble, and the sorted `_graphArSrcIndex` and `_graphArOffset` columns
/// shrink to a few bits per row. Still plain parquet for any reader.
pub(crate) fn writer_properties(schema: &Schema) -> WriterProperties {
    let mut b =
        WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default()));
    for f in schema.fields() {
        if matches!(f.data_type(), DataType::Int32 | DataType::Int64) {
            let col = ColumnPath::from(f.name().as_str());
            b = b
                .set_column_dictionary_enabled(col.clone(), false)
                .set_column_encoding(col, Encoding::DELTA_BINARY_PACKED);
        }
    }
    b.build()
}

pub fn write(path: impl AsRef<Path>, batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let schema = batches[0].schema();
    let file = File::create(path)?;
    let props = writer_properties(&schema);
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
