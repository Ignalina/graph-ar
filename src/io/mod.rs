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

pub mod csv;
pub mod ipc;
pub mod json;
#[cfg(feature = "orc")]
pub mod orc;
pub mod parquet;

use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use std::path::{Path, PathBuf};

use crate::{error::Result, types::FileType};

pub fn read_chunk(path: impl AsRef<Path>, file_type: &FileType) -> Result<Vec<RecordBatch>> {
    match file_type {
        FileType::Parquet => parquet::read(path),
        FileType::Csv => csv::read(path),
        FileType::Json => json::read(path),
        #[cfg(feature = "orc")]
        FileType::Orc => orc::read(path),
        #[cfg(not(feature = "orc"))]
        FileType::Orc => Err(crate::error::GraphArError::from(std::io::Error::other(
            "graphar was built without the `orc` feature — ORC is not compiled in. \
             Rebuild with `--features orc`.",
        ))),
        FileType::ArrowIpc => ipc::read(path),
    }
}

/// Read a chunk, giving the text formats (CSV, JSON) the column types of
/// `declared` (a property group's schema). Self-describing formats ignore it.
pub fn read_chunk_typed(
    path: impl AsRef<Path>,
    file_type: &FileType,
    declared: &Schema,
) -> Result<Vec<RecordBatch>> {
    match file_type {
        FileType::Csv => csv::read_typed(path, Some(declared)),
        FileType::Json => json::read_typed(path, Some(declared)),
        _ => read_chunk(path, file_type),
    }
}

/// Read rows `[offset, offset + limit)` of a chunk, projected to `columns`
/// (all when `None`). Parquet gets the real pushdown ([`parquet::read_rows`]);
/// the other formats read the chunk and slice it, so the result is the same
/// and only the cost differs.
pub fn read_chunk_rows(
    path: impl AsRef<Path>,
    file_type: &FileType,
    offset: usize,
    limit: usize,
    columns: Option<&[&str]>,
) -> Result<Vec<RecordBatch>> {
    read_chunk_rows_cached(path, file_type, offset, limit, columns, None)
}

/// [`read_chunk_rows`] with an optional parquet [`parquet::MetadataCache`]
/// for repeated lookups into the same files.
pub fn read_chunk_rows_cached(
    path: impl AsRef<Path>,
    file_type: &FileType,
    offset: usize,
    limit: usize,
    columns: Option<&[&str]>,
    cache: Option<&parquet::MetadataCache>,
) -> Result<Vec<RecordBatch>> {
    if let FileType::Parquet = file_type {
        return match cache {
            Some(c) => c.read_rows(path, offset, limit, columns),
            None => parquet::read_rows(path, offset, limit, columns),
        };
    }
    let mut out = Vec::new();
    let (mut skip, mut want) = (offset, limit);
    for batch in read_chunk(path, file_type)? {
        if want == 0 {
            break;
        }
        let n = batch.num_rows();
        if skip >= n {
            skip -= n;
            continue;
        }
        let take = (n - skip).min(want);
        let mut b = batch.slice(skip, take);
        if let Some(cols) = columns {
            let idx = cols
                .iter()
                .map(|c| {
                    b.schema()
                        .index_of(c)
                        .map_err(|_| crate::error::GraphArError::PropertyNotFound((*c).to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            b = b.project(&idx)?;
        }
        out.push(b);
        skip = 0;
        want -= take;
    }
    Ok(out)
}

/// `inferred` with the types of `declared` for the columns it names, and
/// int64 for the reserved `_graphAr*` index columns.
pub(crate) fn apply_declared_types(inferred: &Schema, declared: Option<&Schema>) -> Schema {
    use crate::general_params::{DST_INDEX_COL, OFFSET_COL, SRC_INDEX_COL, VERTEX_INDEX_COL};
    let fields: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|f| {
            let name = f.name().as_str();
            if let Some(d) = declared.and_then(|d| d.field_with_name(name).ok()) {
                Field::new(name, d.data_type().clone(), true)
            } else if [VERTEX_INDEX_COL, SRC_INDEX_COL, DST_INDEX_COL, OFFSET_COL].contains(&name) {
                Field::new(name, ArrowDataType::Int64, true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new(fields)
}

pub fn write_chunk(
    path: impl AsRef<Path>,
    batches: &[RecordBatch],
    file_type: &FileType,
) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }
    match file_type {
        FileType::Parquet => parquet::write(path, batches),
        FileType::Csv => csv::write(path, batches),
        FileType::Json => json::write(path, batches),
        #[cfg(feature = "orc")]
        FileType::Orc => orc::write(path, batches),
        #[cfg(not(feature = "orc"))]
        FileType::Orc => Err(crate::error::GraphArError::from(std::io::Error::other(
            "graphar was built without the `orc` feature — ORC is not compiled in. \
             Rebuild with `--features orc`.",
        ))),
        FileType::ArrowIpc => ipc::write(path, batches),
    }
}

/// Find a chunk file on disk. The `gar/v1` layout names chunks `chunk<N>`
/// with no extension; files written by older versions of this crate carry the
/// format's extension (`chunk<N>.parquet`), so that name is tried second.
pub fn resolve_chunk(path: impl AsRef<Path>, file_type: &FileType) -> Option<PathBuf> {
    let path = path.as_ref();
    if path.is_file() {
        return Some(path.to_path_buf());
    }
    let mut legacy = path.as_os_str().to_owned();
    legacy.push(".");
    legacy.push(file_type.extension());
    let legacy = PathBuf::from(legacy);
    legacy.is_file().then_some(legacy)
}

/// Read a `vertex_count` / `edge_count<N>` file: one little-endian int64.
pub fn read_count(path: impl AsRef<Path>) -> Result<u64> {
    let path = path.as_ref();
    let raw = std::fs::read(path)?;
    let bytes: [u8; 8] = raw.as_slice().try_into().map_err(|_| {
        crate::error::GraphArError::Other(format!(
            "{}: a count file holds 8 bytes, found {}",
            path.display(),
            raw.len()
        ))
    })?;
    let n = i64::from_le_bytes(bytes);
    u64::try_from(n).map_err(|_| {
        crate::error::GraphArError::Other(format!("{}: negative count {n}", path.display()))
    })
}

/// Write a count file as one little-endian int64, creating parent dirs.
pub fn write_count(path: impl AsRef<Path>, count: u64) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let n = i64::try_from(count)
        .map_err(|_| crate::error::GraphArError::Other(format!("count {count} exceeds i64")))?;
    std::fs::write(path, n.to_le_bytes())?;
    Ok(())
}
