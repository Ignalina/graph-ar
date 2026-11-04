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
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch,
    builder::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        ListBuilder, StringBuilder, TimestampMillisecondBuilder,
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
///
/// Properties live in a small `Vec` rather than a `HashMap`: a vertex carries
/// a handful of them, a linear scan over a few names beats hashing, and it is
/// one allocation per vertex instead of a hash table (see `docs/perf.md`).
#[derive(Debug, Clone, Default)]
pub struct Vertex {
    properties: Vec<(String, PropertyValue)>,
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
    /// Days since the UNIX epoch, for a `date` property.
    Date(i32),
    /// Milliseconds since the UNIX epoch, for a `timestamp` property.
    Timestamp(i64),
    /// The items of a `list<T>` property. An empty `Vec` is an empty (non-null)
    /// list, a missing property a null list, and an item of another type than
    /// `T` a null item.
    List(Vec<PropertyValue>),
}

impl Vertex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a property; setting an existing name replaces its value.
    pub fn add_property(mut self, name: impl Into<String>, value: PropertyValue) -> Self {
        let name = name.into();
        if let Some(slot) = self.properties.iter_mut().find(|(n, _)| *n == name) {
            slot.1 = value;
        } else {
            self.properties.push((name, value));
        }
        self
    }

    pub fn get(&self, name: &str) -> Option<&PropertyValue> {
        self.properties
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }
}

/// Anything that exposes named [`PropertyValue`]s — the row type the column
/// builders read from. Implemented for [`Vertex`] and
/// [`crate::writer::Edge`], so the edge writer builds property chunks straight
/// from its edges instead of cloning each one into a pseudo-vertex first.
pub trait HasProperties {
    fn get_property(&self, name: &str) -> Option<&PropertyValue>;
}

impl HasProperties for Vertex {
    #[inline]
    fn get_property(&self, name: &str) -> Option<&PropertyValue> {
        self.get(name)
    }
}

impl<T: HasProperties> HasProperties for &T {
    #[inline]
    fn get_property(&self, name: &str) -> Option<&PropertyValue> {
        (**self).get_property(name)
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
pub fn build_record_batch_for_edges<T: HasProperties>(
    rows: &[T],
    group: &PropertyGroup,
) -> Result<RecordBatch> {
    build_record_batch(rows, group)
}

fn build_record_batch<T: HasProperties>(items: &[T], group: &PropertyGroup) -> Result<RecordBatch> {
    let schema = group.arrow_schema();
    let columns = group
        .properties
        .iter()
        .map(|prop| build_column(items, &prop.name, &prop.data_type))
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// One column from the rows. The property key is resolved once per column;
/// each row only does the name lookup and appends straight into the typed
/// builder. A value of another type or a missing key appends a null.
fn build_column<T: HasProperties>(items: &[T], name: &str, ty: &DataType) -> Result<ArrayRef> {
    let rows = items.len();
    macro_rules! scalar {
        ($builder:expr, $variant:ident, $value:expr) => {{
            let mut b = $builder;
            for v in items {
                match v.get_property(name) {
                    Some(PropertyValue::$variant(x)) => b.append_value($value(x)),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    macro_rules! list {
        ($builder:expr, $variant:ident, $value:expr) => {{
            let mut b = ListBuilder::new($builder);
            for v in items {
                match v.get_property(name) {
                    Some(PropertyValue::List(list)) => {
                        for item in list {
                            match item {
                                PropertyValue::$variant(x) => b.values().append_value($value(x)),
                                _ => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    let copy_bool = |x: &bool| *x;
    let copy_i32 = |x: &i32| *x;
    let copy_i64 = |x: &i64| *x;
    let copy_f32 = |x: &f32| *x;
    let copy_f64 = |x: &f64| *x;
    fn text(x: &String) -> &str {
        x
    }
    Ok(match ty {
        DataType::Bool => scalar!(BooleanBuilder::with_capacity(rows), Bool, copy_bool),
        DataType::Int32 => scalar!(Int32Builder::with_capacity(rows), Int32, copy_i32),
        DataType::Int64 => scalar!(Int64Builder::with_capacity(rows), Int64, copy_i64),
        DataType::Float => scalar!(Float32Builder::with_capacity(rows), Float, copy_f32),
        DataType::Double => scalar!(Float64Builder::with_capacity(rows), Double, copy_f64),
        DataType::String => scalar!(StringBuilder::with_capacity(rows, 0), String, text),
        DataType::Date => scalar!(Date32Builder::with_capacity(rows), Date, copy_i32),
        DataType::Timestamp => {
            scalar!(
                TimestampMillisecondBuilder::with_capacity(rows),
                Timestamp,
                copy_i64
            )
        }
        DataType::Time => {
            return Err(GraphArError::Other(format!(
                "property `{name}`: the row builders do not take `time` values"
            )));
        }
        DataType::List(item) => match item.as_ref() {
            DataType::Bool => list!(BooleanBuilder::new(), Bool, copy_bool),
            DataType::Int32 => list!(Int32Builder::new(), Int32, copy_i32),
            DataType::Int64 => list!(Int64Builder::new(), Int64, copy_i64),
            DataType::Float => list!(Float32Builder::new(), Float, copy_f32),
            DataType::Double => list!(Float64Builder::new(), Double, copy_f64),
            DataType::String => list!(StringBuilder::new(), String, text),
            DataType::Date => list!(Date32Builder::new(), Date, copy_i32),
            DataType::Timestamp => list!(TimestampMillisecondBuilder::new(), Timestamp, copy_i64),
            other => {
                return Err(GraphArError::Other(format!(
                    "property `{name}`: the row builders do not take list<{}> values",
                    other.name()
                )));
            }
        },
    })
}
