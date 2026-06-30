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
        Self {
            src,
            dst,
            properties: HashMap::new(),
        }
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
        // Chunk membership tests the alignment id as a `u64` (see `key` below),
        // so sort by that same `u64` key. For the only ids that ever land in a
        // chunk — non-negative vertex ids in `[0, vertex_count)` — this orders
        // identically to sorting by the raw `i64`; out-of-range ids (negatives,
        // which cast to huge `u64`) are dropped either way, so the written
        // output is unchanged. Keeping the sort key equal to the membership key
        // is what lets the single advancing cursor below stay correct.
        let aligned_by_src = self.adj_type.aligned_by_src();
        let key = |e: &Edge| -> u64 {
            if aligned_by_src {
                e.src as u64
            } else {
                e.dst as u64
            }
        };
        self.edges.sort_unstable_by_key(key);

        let vchunk_size = if aligned_by_src {
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

        // The edges are now sorted ascending by the alignment key, and the
        // vertex chunks below are visited with monotonically increasing
        // `[v_start, v_end)` ranges. So a single cursor can walk the sorted Vec
        // once, yielding the contiguous slice for each chunk in O(edges) total
        // instead of re-scanning every edge per vertex chunk.
        //
        // The first chunk starts at key 0 and the cursor begins at the front.
        // Edges whose key lands beyond the final chunk's `v_end`
        // (>= vertex_count) are simply never reached and thus dropped, matching
        // the original per-chunk filter.
        let mut cursor = 0usize;

        for vc in 0..num_vertex_chunks {
            let v_start = vc * vchunk_size;
            let v_end = ((vc + 1) * vchunk_size).min(self.vertex_count);

            // Edges belonging to this vertex chunk: a contiguous run in the
            // sorted Vec starting at `cursor`. Advance `cursor` to the first
            // edge with `key >= v_end`.
            debug_assert!(self.edges[cursor..].iter().all(|e| key(e) >= v_start));
            let chunk_end = cursor + self.edges[cursor..].partition_point(|e| key(e) < v_end);
            let chunk_edges: Vec<&Edge> = self.edges[cursor..chunk_end].iter().collect();
            cursor = chunk_end;

            // Write adjacency list chunks.
            let edge_chunk_size = self.info.chunk_size as usize;
            for (ec, edge_slice) in chunk_edges.chunks(edge_chunk_size).enumerate() {
                let batch = build_adj_list_batch(edge_slice)?;
                let path = self.base_dir.join(self.info.adj_list_chunk_path(
                    &self.adj_type,
                    vc,
                    ec as u64,
                )?);
                io::write_chunk(&path, &[batch], &file_type)?;

                // Write property group chunks.
                for group in self.info.property_groups.iter() {
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
                let offset_batch =
                    build_offset_batch(&chunk_edges, v_start, v_end, vchunk_size, aligned_by_src)?;
                let offset_path = self
                    .base_dir
                    .join(self.info.offset_chunk_path(&self.adj_type, vc)?);
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
    aligned_by_src: bool,
) -> Result<RecordBatch> {
    let count = (v_end - v_start) as usize + 1;
    let mut offsets = vec![0i64; count];
    // Histogram: count edges per vertex slot, then prefix-sum.
    let mut hist = vec![0i64; (v_end - v_start) as usize];
    for edge in chunk_edges {
        // Key the histogram on the same vertex dimension the adj list is aligned
        // by — `src` for *_by_source, `dst` for *_by_dest. Mirrors the `key`
        // closure used in `dump()`; using `src` unconditionally would bucket
        // ordered_by_dest offsets against the wrong dimension.
        let key_id = if aligned_by_src { edge.src } else { edge.dst };
        let slot = (key_id as u64).saturating_sub(v_start) as usize;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    fn offsets_of(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn offset_batch_keys_on_dst_for_ordered_by_dest() {
        // An OrderedByDest chunk is aligned/sorted by `dst`; the offset
        // histogram must bucket on `dst`, not `src`.
        //
        // FAIL-ON-BUG: the old code keyed on `edge.src` unconditionally. Here
        // every `src` (100) is outside [v_start, v_end) = [0, 3), so the buggy
        // path drops all edges and yields all-zero offsets. PASS-ON-FIX: keying
        // on `dst` produces the correct prefix sums.
        let edges = vec![
            Edge::new(100, 0),
            Edge::new(100, 0),
            Edge::new(100, 1),
            Edge::new(100, 2),
        ];
        let refs: Vec<&Edge> = edges.iter().collect();

        // aligned_by_src = false → key on dst.
        let batch = build_offset_batch(&refs, 0, 3, 4, false).unwrap();
        assert_eq!(
            offsets_of(&batch),
            vec![0, 2, 3, 4],
            "ordered_by_dest offsets must be the dst-keyed prefix sum",
        );

        // Sanity: keying on src (aligned_by_src = true) drops every edge here.
        let buggy = build_offset_batch(&refs, 0, 3, 4, true).unwrap();
        assert_eq!(offsets_of(&buggy), vec![0, 0, 0, 0]);
    }

    #[test]
    fn offset_batch_keys_on_src_for_ordered_by_source() {
        // The by-source path is unchanged: bucket on `src`.
        let edges = vec![Edge::new(0, 9), Edge::new(1, 9), Edge::new(1, 9)];
        let refs: Vec<&Edge> = edges.iter().collect();
        let batch = build_offset_batch(&refs, 0, 2, 4, true).unwrap();
        assert_eq!(offsets_of(&batch), vec![0, 1, 3]);
    }
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
