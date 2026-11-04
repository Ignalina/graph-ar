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

//! Chunk formats behind one trait.
//!
//! A chunk is one file of Arrow batches; [`PayloadFormat`] is how a format
//! turns batches into bytes and back. The formats of the GraphAr `FileType`
//! (Parquet, ORC, CSV, JSON) are built in. Any other format implements the
//! trait in its own crate and calls [`register_format`], which returns the
//! [`FileType::Extension`] to put in the info files:
//!
//! ```text
//! info file: file_type: <name>
//!      │ FileType
//!      ▼
//! format_for ──▶ built-in (spec name) or registry (any other name)
//!      │ Arc<dyn PayloadFormat>
//!      ▼
//! read_chunk / write_chunk (files) · read_bytes / write_bytes (ChunkStore)
//! ```
//!
//! Lookup is one match or one hash probe per chunk, O(1), against the decode
//! of a whole chunk.

use std::{
    collections::HashMap,
    io::Cursor,
    path::Path,
    sync::{Arc, OnceLock, RwLock},
};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    error::{GraphArError, Result},
    io,
    types::FileType,
};

/// How one chunk format reads and writes Arrow batches.
///
/// Only the two byte methods are required. `read_chunk` and `write_chunk`
/// default to them; a format with a faster file path overrides those.
pub trait PayloadFormat: Send + Sync {
    /// The `file_type` value in the info files.
    fn name(&self) -> &str;

    /// The file extension of the format, without a dot. Chunk files have no
    /// extension in the `gar/v1` layout; this is for chunks written outside it.
    fn extension(&self) -> &str;

    /// Encode `batches` (never empty) as one chunk.
    fn write_bytes(&self, batches: &[RecordBatch]) -> Result<Vec<u8>>;

    /// Decode one chunk. `declared` is the property group's schema when the
    /// caller has one; a format without its own types uses it for the columns.
    fn read_bytes(&self, bytes: Bytes, declared: Option<&Schema>) -> Result<Vec<RecordBatch>>;

    /// Read a chunk file.
    fn read_chunk(&self, path: &Path, declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        self.read_bytes(Bytes::from(std::fs::read(path)?), declared)
    }

    /// Write a chunk file. Writing no batches writes no file.
    fn write_chunk(&self, path: &Path, batches: &[RecordBatch]) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        std::fs::write(path, self.write_bytes(batches)?)?;
        Ok(())
    }
}

/// Parquet, the default format.
pub struct Parquet;

impl PayloadFormat for Parquet {
    fn name(&self) -> &str {
        "parquet"
    }
    fn extension(&self) -> &str {
        "parquet"
    }
    fn write_bytes(&self, batches: &[RecordBatch]) -> Result<Vec<u8>> {
        let schema = batches[0].schema();
        let mut out = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            &mut out,
            schema.clone(),
            Some(io::parquet::writer_properties(&schema)),
        )?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
        Ok(out)
    }
    fn read_bytes(&self, bytes: Bytes, _declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        reader.map(|b| Ok(b?)).collect()
    }
    fn read_chunk(&self, path: &Path, _declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        io::parquet::read(path)
    }
    fn write_chunk(&self, path: &Path, batches: &[RecordBatch]) -> Result<()> {
        io::parquet::write(path, batches)
    }
}

/// CSV with a header row.
pub struct Csv;

impl PayloadFormat for Csv {
    fn name(&self) -> &str {
        "csv"
    }
    fn extension(&self) -> &str {
        "csv"
    }
    fn write_bytes(&self, batches: &[RecordBatch]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut writer = arrow_csv::WriterBuilder::new()
            .with_header(true)
            .build(&mut out);
        for batch in batches {
            writer.write(batch)?;
        }
        drop(writer);
        Ok(out)
    }
    fn read_bytes(&self, bytes: Bytes, declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        let format = arrow_csv::reader::Format::default().with_header(true);
        let (inferred, _) = format.infer_schema(Cursor::new(&bytes), Some(100))?;
        let schema = io::apply_declared_types(&inferred, declared);
        let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .build(Cursor::new(&bytes))?;
        reader.map(|b| Ok(b?)).collect()
    }
    fn read_chunk(&self, path: &Path, declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        io::csv::read_typed(path, declared)
    }
    fn write_chunk(&self, path: &Path, batches: &[RecordBatch]) -> Result<()> {
        io::csv::write(path, batches)
    }
}

/// JSON lines.
pub struct Json;

impl PayloadFormat for Json {
    fn name(&self) -> &str {
        "json"
    }
    fn extension(&self) -> &str {
        "json"
    }
    fn write_bytes(&self, batches: &[RecordBatch]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut out);
        writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
        writer.finish()?;
        drop(writer);
        Ok(out)
    }
    fn read_bytes(&self, bytes: Bytes, declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        let mut cursor = Cursor::new(&bytes);
        let (inferred, _) = arrow::json::reader::infer_json_schema(&mut cursor, None)?;
        let schema = io::apply_declared_types(&inferred, declared);
        cursor.set_position(0);
        let reader = arrow::json::ReaderBuilder::new(Arc::new(schema)).build(cursor)?;
        reader.map(|b| Ok(b?)).collect()
    }
    fn read_chunk(&self, path: &Path, declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        io::json::read_typed(path, declared)
    }
    fn write_chunk(&self, path: &Path, batches: &[RecordBatch]) -> Result<()> {
        io::json::write(path, batches)
    }
}

/// ORC (ZSTD), through orc-rust. Without the `orc` feature every call is an
/// error that says so.
pub struct Orc;

#[cfg(not(feature = "orc"))]
fn orc_missing() -> GraphArError {
    GraphArError::from(std::io::Error::other(
        "graphar was built without the `orc` feature — ORC is not compiled in. \
         Rebuild with `--features orc`.",
    ))
}

impl PayloadFormat for Orc {
    fn name(&self) -> &str {
        "orc"
    }
    fn extension(&self) -> &str {
        "orc"
    }
    #[cfg(feature = "orc")]
    fn write_bytes(&self, batches: &[RecordBatch]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        io::orc::write_to(&mut out, batches)?;
        Ok(out)
    }
    #[cfg(not(feature = "orc"))]
    fn write_bytes(&self, _batches: &[RecordBatch]) -> Result<Vec<u8>> {
        Err(orc_missing())
    }
    #[cfg(feature = "orc")]
    fn read_bytes(&self, bytes: Bytes, declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        let reader = orc_rust::ArrowReaderBuilder::try_new(bytes)?.build();
        let batches = reader
            .map(|b| Ok(b?))
            .collect::<Result<Vec<RecordBatch>>>()?;
        io::cast_all(batches, declared)
    }
    #[cfg(not(feature = "orc"))]
    fn read_bytes(&self, _bytes: Bytes, _declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        Err(orc_missing())
    }
    #[cfg(feature = "orc")]
    fn read_chunk(&self, path: &Path, declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        // ORC timestamps come back as nanoseconds.
        io::cast_all(io::orc::read(path)?, declared)
    }
    #[cfg(not(feature = "orc"))]
    fn read_chunk(&self, _path: &Path, _declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
        Err(orc_missing())
    }
    #[cfg(feature = "orc")]
    fn write_chunk(&self, path: &Path, batches: &[RecordBatch]) -> Result<()> {
        io::orc::write(path, batches)
    }
    #[cfg(not(feature = "orc"))]
    fn write_chunk(&self, _path: &Path, _batches: &[RecordBatch]) -> Result<()> {
        Err(orc_missing())
    }
}

type Registry = RwLock<HashMap<String, Arc<dyn PayloadFormat>>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Make a format known by its [`PayloadFormat::name`] and return the
/// [`FileType`] that selects it. Registering a name again replaces the format.
/// The spec's names stay built in: for those this changes nothing and returns
/// the spec's [`FileType`].
pub fn register_format(format: Arc<dyn PayloadFormat>) -> FileType {
    let file_type = FileType::from_name(format.name());
    if let FileType::Extension(name) = &file_type {
        registry()
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.clone(), format);
    }
    file_type
}

/// The format behind a [`FileType`]. An [`FileType::Extension`] nobody
/// registered is an error.
pub fn format_for(file_type: &FileType) -> Result<Arc<dyn PayloadFormat>> {
    Ok(match file_type {
        FileType::Parquet => Arc::new(Parquet),
        FileType::Orc => Arc::new(Orc),
        FileType::Csv => Arc::new(Csv),
        FileType::Json => Arc::new(Json),
        FileType::Extension(name) => registry()
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
            .ok_or_else(|| {
                GraphArError::UnsupportedFileType(format!("`{name}` is not registered"))
            })?,
    })
}
