use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListBuilder,
        StringBuilder,
    },
};

use crate::{
    error::Result,
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
    pub fn dump(&mut self) -> Result<u64> {
        let chunk_size = self.info.chunk_size as usize;
        let mut chunk_index = 0u64;

        for chunk in self.vertices.chunks(chunk_size) {
            for group in self.info.property_groups.iter() {
                let batch = build_record_batch(chunk, group)?;
                let rel = self.info.chunk_path(group, chunk_index);
                let full = self.base_dir.join(&rel);
                io::write_chunk(&full, &[batch], &group.file_type)?;
            }
            chunk_index += 1;
        }

        self.vertices.clear();
        Ok(chunk_index)
    }

    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }
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
