use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};

use crate::{
    error::Result,
    io,
    metadata::{EdgeInfo, PropertyGroup},
    types::AdjListType,
    writer::vertex_writer::{PropertyValue, build_record_batch_for_edges},
};

/// An edge with source/destination and optional properties.
#[derive(Debug, Clone)]
pub struct Edge {
    pub src: i64,
    pub dst: i64,
    properties: HashMap<String, PropertyValue>,
}

impl Edge {
    pub fn new(src: i64, dst: i64) -> Self {
        Self { src, dst, properties: HashMap::new() }
    }

    pub fn add_property(mut self, name: impl Into<String>, value: PropertyValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }

    pub fn get(&self, name: &str) -> Option<&PropertyValue> {
        self.properties.get(name)
    }
}

/// Builds and writes edge chunks (adj list + properties + offsets) to disk.
pub struct EdgesBuilder {
    info: EdgeInfo,
    base_dir: PathBuf,
    adj_type: AdjListType,
    /// Total number of vertices in the aligned dimension (src or dst).
    vertex_count: u64,
    edges: Vec<Edge>,
}

impl EdgesBuilder {
    pub fn new(
        info: EdgeInfo,
        base_dir: impl AsRef<Path>,
        adj_type: AdjListType,
        vertex_count: u64,
    ) -> Self {
        Self {
            info,
            base_dir: base_dir.as_ref().to_path_buf(),
            adj_type,
            vertex_count,
            edges: Vec::new(),
        }
    }

    pub fn add_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
    }

    /// Sort edges by the alignment key and flush as chunked files.
    pub fn dump(&mut self) -> Result<()> {
        // Sort edges by the alignment vertex id.
        if self.adj_type.aligned_by_src() {
            self.edges.sort_unstable_by_key(|e| e.src);
        } else {
            self.edges.sort_unstable_by_key(|e| e.dst);
        }

        let vchunk_size = if self.adj_type.aligned_by_src() {
            self.info.src_chunk_size
        } else {
            self.info.dst_chunk_size
        };
        let num_vertex_chunks = self.vertex_count.div_ceil(vchunk_size);

        let file_type = self
            .info
            .get_adj_list_info(&self.adj_type)
            .map(|a| a.file_type.clone())
            .unwrap_or_default();

        for vc in 0..num_vertex_chunks {
            let v_start = vc * vchunk_size;
            let v_end = ((vc + 1) * vchunk_size).min(self.vertex_count);

            // Edges belonging to this vertex chunk.
            let chunk_edges: Vec<&Edge> = self
                .edges
                .iter()
                .filter(|e| {
                    let key = if self.adj_type.aligned_by_src() { e.src as u64 } else { e.dst as u64 };
                    key >= v_start && key < v_end
                })
                .collect();

            // Write adjacency list chunks.
            let edge_chunk_size = self.info.chunk_size as usize;
            for (ec, edge_slice) in chunk_edges.chunks(edge_chunk_size).enumerate() {
                let batch = build_adj_list_batch(edge_slice)?;
                let path = self
                    .base_dir
                    .join(self.info.adj_list_chunk_path(&self.adj_type, vc, ec as u64)?);
                io::write_chunk(&path, &[batch], &file_type)?;

                // Write property group chunks.
                for group in self.info.property_groups.clone().iter() {
                    let prop_batch = build_edge_prop_batch(edge_slice, group)?;
                    let prop_path = self.base_dir.join(self.info.property_chunk_path(
                        &self.adj_type,
                        group,
                        vc,
                        ec as u64,
                    ));
                    io::write_chunk(&prop_path, &[prop_batch], &group.file_type)?;
                }
            }

            // Write offset chunk for ordered adj lists.
            if self.adj_type.is_ordered() {
                let offset_batch = build_offset_batch(&chunk_edges, v_start, v_end, vchunk_size)?;
                let offset_path =
                    self.base_dir.join(self.info.offset_chunk_path(&self.adj_type, vc)?);
                io::write_chunk(&offset_path, &[offset_batch], &file_type)?;
            }
        }

        self.edges.clear();
        Ok(())
    }
}

fn build_adj_list_batch(edges: &[&Edge]) -> Result<RecordBatch> {
    let srcs: Vec<i64> = edges.iter().map(|e| e.src).collect();
    let dsts: Vec<i64> = edges.iter().map(|e| e.dst).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("src", ArrowDataType::Int64, false),
        Field::new("dst", ArrowDataType::Int64, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(srcs)),
            Arc::new(Int64Array::from(dsts)),
        ],
    )?)
}

fn build_offset_batch(
    chunk_edges: &[&Edge],
    v_start: u64,
    v_end: u64,
    _vchunk_size: u64,
) -> Result<RecordBatch> {
    let count = (v_end - v_start) as usize + 1;
    let mut offsets = vec![0i64; count];
    // Histogram: count edges per vertex slot, then prefix-sum.
    let mut hist = vec![0i64; (v_end - v_start) as usize];
    for edge in chunk_edges {
        // Use src as the key (this fn is called for both src/dst ordered; caller picks edges).
        let slot = (edge.src as u64).saturating_sub(v_start) as usize;
        if slot < hist.len() {
            hist[slot] += 1;
        }
    }
    // Prefix sum -> offsets; first row is always 0.
    let mut running = 0i64;
    offsets[0] = 0;
    for (i, count) in hist.iter().enumerate() {
        running += count;
        offsets[i + 1] = running;
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "offset",
        ArrowDataType::Int64,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(offsets))],
    )?)
}

fn build_edge_prop_batch(edges: &[&Edge], group: &PropertyGroup) -> Result<RecordBatch> {
    // Build pseudo-Vertex objects from edge properties for each property in the group.
    let pseudo_vertices: Vec<crate::writer::vertex_writer::Vertex> = edges
        .iter()
        .map(|e| {
            let mut v = crate::writer::vertex_writer::Vertex::new();
            for prop in &group.properties {
                if let Some(val) = e.get(&prop.name) {
                    v = v.add_property(prop.name.clone(), val.clone());
                }
            }
            v
        })
        .collect();

    build_record_batch_for_edges(&pseudo_vertices, group)
}
