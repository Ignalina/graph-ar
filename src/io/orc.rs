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

use std::{fs::File, io::Write, path::Path};

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use orc_rust::{ArrowReaderBuilder, ArrowWriterBuilder, compression::CompressionType};

use crate::error::{GraphArError, Result};

pub fn read(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = ArrowReaderBuilder::try_new(file)?.build();
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
    write_to(File::create(path)?, batches)
}

/// Encode `batches` as ZSTD-compressed ORC. orc-rust 0.9 writes strings
/// directly (it has no dictionary encoder) and has no list encoder: its
/// `create_encoder` would panic, so list columns are refused here.
pub(crate) fn write_to<W: Write>(out: W, batches: &[RecordBatch]) -> Result<()> {
    let schema = batches[0].schema();
    if let Some(f) = schema
        .fields()
        .iter()
        .find(|f| matches!(f.data_type(), DataType::List(_)))
    {
        return Err(GraphArError::UnsupportedFileType(format!(
            "ORC: the writer has no list encoder (column `{}`)",
            f.name()
        )));
    }
    let mut writer = ArrowWriterBuilder::new(out, schema)
        .with_compression(CompressionType::Zstd)
        .try_build()?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}
