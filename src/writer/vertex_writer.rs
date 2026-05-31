use std::{collections::HashMap, path::{Path, PathBuf}, sync::Arc};

use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
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
            for group in self.info.property_groups.clone().iter() {
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
pub fn build_record_batch_for_edges(vertices: &[Vertex], group: &PropertyGroup) -> Result<RecordBatch> {
    build_record_batch(vertices, group)
}

fn build_record_batch(vertices: &[Vertex], group: &PropertyGroup) -> Result<RecordBatch> {
    let schema = group.arrow_schema();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for prop in &group.properties {
        let col: ArrayRef = match &prop.data_type {
            DataType::Bool => {
                let vals: Vec<Option<bool>> = vertices
                    .iter()
                    .map(|v| match v.get(&prop.name) {
                        Some(PropertyValue::Bool(b)) => Some(*b),
                        _ => None,
                    })
                    .collect();
                Arc::new(BooleanArray::from(vals))
            }
            DataType::Int32 => {
                let vals: Vec<Option<i32>> = vertices
                    .iter()
                    .map(|v| match v.get(&prop.name) {
                        Some(PropertyValue::Int32(i)) => Some(*i),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int32Array::from(vals))
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = vertices
                    .iter()
                    .map(|v| match v.get(&prop.name) {
                        Some(PropertyValue::Int64(i)) => Some(*i),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(vals))
            }
            DataType::Float => {
                let vals: Vec<Option<f32>> = vertices
                    .iter()
                    .map(|v| match v.get(&prop.name) {
                        Some(PropertyValue::Float(f)) => Some(*f),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float32Array::from(vals))
            }
            DataType::Double => {
                let vals: Vec<Option<f64>> = vertices
                    .iter()
                    .map(|v| match v.get(&prop.name) {
                        Some(PropertyValue::Double(d)) => Some(*d),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(vals))
            }
            DataType::String | DataType::Date | DataType::Timestamp | DataType::Time => {
                let vals: Vec<Option<&str>> = vertices
                    .iter()
                    .map(|v| match v.get(&prop.name) {
                        Some(PropertyValue::String(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect();
                Arc::new(StringArray::from(vals))
            }
            DataType::List => {
                // Serialize list items as JSON strings for now.
                let vals: Vec<Option<&str>> = vertices
                    .iter()
                    .map(|v| match v.get(&prop.name) {
                        Some(PropertyValue::String(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect();
                Arc::new(StringArray::from(vals))
            }
        };
        columns.push(col);
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}
