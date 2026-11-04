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
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch,
    builder::{
        BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListBuilder,
        StringBuilder,
    },
};

use arrow_schema::{DataType as ArrowDataType, Field, Schema};

use crate::{
    error::{GraphArError, Result},
    io,
    metadata::{PropertyGroup, VertexInfo},
    types::DataType,
};

/// In-memory vertex with typed property values.
#[derive(Debug, Clone, Default)]
pub struct Vertex {
    properties: HashMap<String, PropertyValue>,
}

/// Dynamically typed property value.
#[derive(Debug, Clone)]
pub enum PropertyValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(String),
    /// A variable-length list of strings, written as an Arrow `List<Utf8>`
    /// matching the schema declared by [`DataType::List`]. An empty `Vec`
    /// yields an empty (non-null) list; a missing property yields a null list.
    List(Vec<String>),
}

impl Vertex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_property(mut self, name: impl Into<String>, value: PropertyValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }

    pub fn get(&self, name: &str) -> Option<&PropertyValue> {
        self.properties.get(name)
    }
}

/// Builds and writes vertex chunks to disk.
pub struct VerticesBuilder {
    info: VertexInfo,
    base_dir: PathBuf,
    vertices: Vec<Vertex>,
}

impl VerticesBuilder {
    pub fn new(info: VertexInfo, base_dir: impl AsRef<Path>) -> Self {
        Self {
            info,
            base_dir: base_dir.as_ref().to_path_buf(),
            vertices: Vec::new(),
        }
    }

    pub fn add_vertex(&mut self, vertex: Vertex) {
        self.vertices.push(vertex);
    }

    /// Flush all buffered vertices as chunked files, clearing the buffer.
    /// Each chunk of each property group carries the `_graphArVertexIndex`
    /// column, and the `vertex_count` file is written. Returns the number of
    /// vertex chunks written.
    pub fn dump(&mut self) -> Result<u64> {
        let chunk_size = self.info.chunk_size as usize;
        let mut chunk_index = 0u64;

        for chunk in self.vertices.chunks(chunk_size) {
            let start = chunk_index * self.info.chunk_size;
            for group in self.info.property_groups.iter() {
                let batch = with_vertex_index(build_record_batch(chunk, group)?, start)?;
                let rel = self.info.chunk_path(group, chunk_index);
                let full = self.base_dir.join(&rel);
                io::write_chunk(&full, &[batch], &group.file_type)?;
            }
            chunk_index += 1;
        }
        io::write_count(
            self.base_dir.join(self.info.vertex_count_path()),
            self.vertices.len() as u64,
        )?;

        self.vertices.clear();
        Ok(chunk_index)
    }

    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }
}

/// Prepend the absolute `_graphArVertexIndex` column (`start..start+rows`).
pub(crate) fn with_vertex_index(batch: RecordBatch, start: u64) -> Result<RecordBatch> {
    let rows = batch.num_rows() as i64;
    let start = start as i64;
    let index: ArrayRef = Arc::new(Int64Array::from_iter_values(start..start + rows));
    let mut fields = vec![Arc::new(Field::new(
        crate::general_params::VERTEX_INDEX_COL,
        ArrowDataType::Int64,
        false,
    ))];
    fields.extend(batch.schema().fields().iter().cloned());
    let mut columns = vec![index];
    columns.extend(batch.columns().iter().cloned());
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Write a whole vertex table: `table` holds one column per property (by
/// name, in any order; extra columns are ignored), row `i` is vertex `i`.
/// Columns are cast to the declared property type when they differ. Writes
/// every property group's chunks plus `vertex_count`; returns the vertex count.
pub fn write_vertex_table(
    info: &VertexInfo,
    base_dir: impl AsRef<Path>,
    table: &RecordBatch,
) -> Result<u64> {
    let base = base_dir.as_ref();
    let rows = table.num_rows() as u64;
    let chunk_size = info.chunk_size;
    for group in &info.property_groups {
        let schema = group.arrow_schema();
        let columns = project(table, &schema)?;
        let mut chunk = 0u64;
        while chunk * chunk_size < rows {
            let start = chunk * chunk_size;
            let len = chunk_size.min(rows - start) as usize;
            let cols: Vec<ArrayRef> = columns
                .iter()
                .map(|c| c.slice(start as usize, len))
                .collect();
            let batch = with_vertex_index(RecordBatch::try_new(schema.clone(), cols)?, start)?;
            io::write_chunk(
                base.join(info.chunk_path(group, chunk)),
                &[batch],
                &group.file_type,
            )?;
            chunk += 1;
        }
    }
    io::write_count(base.join(info.vertex_count_path()), rows)?;
    Ok(rows)
}

/// Pick `schema`'s columns out of `table` by name, casting where the type differs.
pub(crate) fn project(table: &RecordBatch, schema: &Schema) -> Result<Vec<ArrayRef>> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let col = table
                .column_by_name(f.name())
                .ok_or_else(|| GraphArError::PropertyNotFound(f.name().clone()))?;
            if col.data_type() == f.data_type() {
                Ok(col.clone())
            } else {
                Ok(arrow::compute::cast(col, f.data_type())?)
            }
        })
        .collect()
}

/// Exposed for edge writer reuse.
pub fn build_record_batch_for_edges(
    vertices: &[Vertex],
    group: &PropertyGroup,
) -> Result<RecordBatch> {
    build_record_batch(vertices, group)
}

fn build_record_batch(vertices: &[Vertex], group: &PropertyGroup) -> Result<RecordBatch> {
    let schema = group.arrow_schema();
    let rows = vertices.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(group.properties.len());

    for prop in &group.properties {
        // Resolve the property key once per column; each row only does the
        // HashMap lookup, appending straight into the typed builder (no
        // intermediate `Vec<Option<T>>`). A non-matching variant or a missing
        // key both append a null, exactly as the old `Array::from` path did.
        let name = prop.name.as_str();
        let col: ArrayRef = match &prop.data_type {
            DataType::Bool => {
                let mut b = BooleanBuilder::with_capacity(rows);
                for v in vertices {
                    match v.get(name) {
                        Some(PropertyValue::Bool(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(rows);
                for v in vertices {
                    match v.get(name) {
                        Some(PropertyValue::Int32(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(rows);
                for v in vertices {
                    match v.get(name) {
                        Some(PropertyValue::Int64(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float => {
                let mut b = Float32Builder::with_capacity(rows);
                for v in vertices {
                    match v.get(name) {
                        Some(PropertyValue::Float(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Double => {
                let mut b = Float64Builder::with_capacity(rows);
                for v in vertices {
                    match v.get(name) {
                        Some(PropertyValue::Double(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::String | DataType::Date | DataType::Timestamp | DataType::Time => {
                let mut b = StringBuilder::with_capacity(rows, 0);
                for v in vertices {
                    match v.get(name) {
                        Some(PropertyValue::String(s)) => b.append_value(s.as_str()),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::List => {
                // `DataType::List` declares an Arrow `List<Utf8>` (item field),
                // so build a real ListArray. Each `PropertyValue::List` becomes
                // one (possibly empty) list slot; a missing key or a non-list
                // variant becomes a null list, mirroring the scalar arms above.
                let mut b = ListBuilder::new(StringBuilder::with_capacity(rows, 0));
                for v in vertices {
                    match v.get(name) {
                        Some(PropertyValue::List(items)) => {
                            for item in items {
                                b.values().append_value(item.as_str());
                            }
                            b.append(true);
                        }
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        columns.push(col);
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}
