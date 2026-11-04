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

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};

use crate::{
    error::{GraphArError, Result},
    general_params::{DST_INDEX_COL, OFFSET_COL, SRC_INDEX_COL},
    io,
    metadata::EdgeInfo,
    types::AdjListType,
    writer::vertex_writer::{HasProperties, PropertyValue, build_record_batch_for_edges},
};

/// An edge with source/destination and optional properties.
///
/// Properties are a small `Vec`, not a `HashMap`: one allocation per edge and
/// a linear scan over a few names (see `docs/perf.md`).
#[derive(Debug, Clone)]
pub struct Edge {
    pub src: i64,
    pub dst: i64,
    properties: Vec<(String, PropertyValue)>,
}

impl Edge {
    pub fn new(src: i64, dst: i64) -> Self {
        Self {
            src,
            dst,
            properties: Vec::new(),
        }
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

impl HasProperties for Edge {
    #[inline]
    fn get_property(&self, name: &str) -> Option<&PropertyValue> {
        self.get(name)
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

    /// Sort edges by the alignment key and flush as chunked files (adjacency
    /// list, property groups, offsets for ordered lists, `edge_count<N>` and
    /// `vertex_count`). Same output as [`write_edge_table`] on the same edges.
    pub fn dump(&mut self) -> Result<()> {
        let srcs: Vec<i64> = self.edges.iter().map(|e| e.src).collect();
        let dsts: Vec<i64> = self.edges.iter().map(|e| e.dst).collect();
        let mut fields = vec![
            Field::new(SRC_INDEX_COL, ArrowDataType::Int64, false),
            Field::new(DST_INDEX_COL, ArrowDataType::Int64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(srcs)),
            Arc::new(Int64Array::from(dsts)),
        ];
        for group in &self.info.property_groups {
            let batch = build_record_batch_for_edges(&self.edges, group)?;
            fields.extend(batch.schema().fields().iter().map(|f| f.as_ref().clone()));
            columns.extend(batch.columns().iter().cloned());
        }
        let table = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        write_edge_table(
            &self.info,
            &self.base_dir,
            &self.adj_type,
            &table,
            self.vertex_count,
        )?;
        self.edges.clear();
        Ok(())
    }
}

fn i64_column<'a>(table: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    table
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| GraphArError::Other(format!("edge table needs an int64 column `{name}`")))
}

/// Write one adjacency list of an edge type from a table of edges.
///
/// `edges` holds `_graphArSrcIndex` and `_graphArDstIndex` (int64) plus one
/// column per edge property (by name). `vertex_count` is the vertex count of
/// the aligned side (src for `*_by_source`, dst for `*_by_dest`); edges whose
/// aligned endpoint is outside `[0, vertex_count)` are dropped. Edges are
/// sorted by the aligned endpoint (ordered lists: then by the other endpoint),
/// split into vertex-chunk partitions and edge chunks, and written with their
/// property groups, offsets (ordered lists), `edge_count<N>` and
/// `vertex_count`. Returns the number of edges written.
pub fn write_edge_table(
    info: &EdgeInfo,
    base_dir: impl AsRef<Path>,
    adj_type: &AdjListType,
    edges: &RecordBatch,
    vertex_count: u64,
) -> Result<u64> {
    let base = base_dir.as_ref();
    let aligned_by_src = adj_type.aligned_by_src();
    let ordered = adj_type.is_ordered();
    let src = i64_column(edges, SRC_INDEX_COL)?.values();
    let dst = i64_column(edges, DST_INDEX_COL)?.values();
    let (key, other) = if aligned_by_src {
        (src, dst)
    } else {
        (dst, src)
    };

    // Counting sort by the aligned key (O(E + V), the keys are bounded by
    // `vertex_count`), then each vertex's run by the other endpoint for
    // ordered lists; `starts[v]` is where vertex v's edges begin, and it is
    // also the offsets array. Out-of-range keys are dropped.
    let (perm, starts) = sort_edges(key, other, vertex_count, ordered);
    let perm = UInt64Array::from_iter_values(perm.into_iter().map(|i| i as u64));
    let take = |name: &str| -> Result<ArrayRef> {
        let col = edges
            .column_by_name(name)
            .ok_or_else(|| GraphArError::PropertyNotFound(name.to_string()))?;
        Ok(arrow::compute::take(col.as_ref(), &perm, None)?)
    };
    let src_sorted = take(SRC_INDEX_COL)?;
    let dst_sorted = take(DST_INDEX_COL)?;
    let adj_schema = Arc::new(Schema::new(vec![
        Field::new(SRC_INDEX_COL, ArrowDataType::Int64, false),
        Field::new(DST_INDEX_COL, ArrowDataType::Int64, false),
    ]));
    let mut groups = Vec::with_capacity(info.property_groups.len());
    for group in &info.property_groups {
        let schema = group.arrow_schema();
        let sorted = RecordBatch::try_new(
            schema.clone(),
            crate::writer::vertex_writer::project(edges, &schema)?,
        )?;
        let cols = sorted
            .columns()
            .iter()
            .map(|c| Ok(arrow::compute::take(c.as_ref(), &perm, None)?))
            .collect::<Result<Vec<ArrayRef>>>()?;
        groups.push((group, RecordBatch::try_new(schema, cols)?));
    }

    let vchunk_size = if aligned_by_src {
        info.src_chunk_size
    } else {
        info.dst_chunk_size
    };
    let file_type = info
        .get_adj_list_info(adj_type)
        .map(|a| a.file_type.clone())
        .ok_or_else(|| GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string()))?;
    let edge_chunk_size = info.chunk_size as usize;
    let num_vertex_chunks = vertex_count.div_ceil(vchunk_size);

    for vc in 0..num_vertex_chunks {
        let v_start = vc * vchunk_size;
        let v_end = ((vc + 1) * vchunk_size).min(vertex_count);
        let (lo, hi) = (starts[v_start as usize], starts[v_end as usize]);

        let mut ec = 0u64;
        let mut at = lo;
        while at < hi {
            let len = edge_chunk_size.min(hi - at);
            let adj = RecordBatch::try_new(
                adj_schema.clone(),
                vec![src_sorted.slice(at, len), dst_sorted.slice(at, len)],
            )?;
            let path = base.join(info.adj_list_chunk_path(adj_type, vc, ec)?);
            io::write_chunk(&path, &[adj], &file_type)?;
            for (group, batch) in &groups {
                let path = base.join(info.property_chunk_path(adj_type, group, vc, ec));
                io::write_chunk(&path, &[batch.slice(at, len)], &group.file_type)?;
            }
            at += len;
            ec += 1;
        }

        if ordered {
            let offsets: Vec<i64> = starts[v_start as usize..=v_end as usize]
                .iter()
                .map(|&s| (s - lo) as i64)
                .collect();
            let path = base.join(info.offset_chunk_path(adj_type, vc)?);
            io::write_chunk(&path, &[offset_batch_from(offsets)], &file_type)?;
        }
        io::write_count(
            base.join(info.edge_count_path(adj_type, vc)),
            (hi - lo) as u64,
        )?;
    }
    io::write_count(base.join(info.vertex_count_path(adj_type)), vertex_count)?;
    Ok(starts[vertex_count as usize] as u64)
}

/// Counting sort of the edge rows by `key` into a permutation, then (for
/// ordered lists) each vertex's run by `other`.
///
/// Returns `(perm, starts)`: `perm` lists the kept rows (key `< vertex_count`)
/// sorted by `(key, other)` — or by key with insertion order within a vertex
/// when `ordered` is false — and `starts` has `vertex_count + 1` entries,
/// `starts[v]..starts[v + 1]` being vertex v's range in `perm`. O(E + V) for
/// the bucket pass plus O(Σ d_v log d_v) for the per-vertex runs; the edge
/// columns are never moved, `take(perm)` gathers them once.
pub(crate) fn sort_edges(
    key: &[i64],
    other: &[i64],
    vertex_count: u64,
    ordered: bool,
) -> (Vec<usize>, Vec<usize>) {
    let v = vertex_count as usize;
    let mut starts = vec![0usize; v + 1];
    for &k in key {
        if (k as u64) < vertex_count {
            starts[k as usize + 1] += 1;
        }
    }
    for i in 1..=v {
        starts[i] += starts[i - 1];
    }
    let mut perm = vec![0usize; starts[v]];
    let mut next = starts.clone();
    for (i, &k) in key.iter().enumerate() {
        if (k as u64) < vertex_count {
            perm[next[k as usize]] = i;
            next[k as usize] += 1;
        }
    }
    if ordered {
        for w in starts.windows(2) {
            let run = &mut perm[w[0]..w[1]];
            if run.len() > 1 {
                run.sort_unstable_by_key(|&i| (other[i], i));
            }
        }
    }
    (perm, starts)
}

fn offset_batch_from(offsets: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        OFFSET_COL,
        ArrowDataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(offsets))])
        .expect("one non-null int64 column")
}

/// CSR offsets of one vertex chunk `[v_start, v_end)`: `v_end - v_start + 1`
/// prefix sums over the (aligned-side) keys of the chunk's edges. The plain
/// reference; `write_edge_table` cuts offsets from `sort_edges`'s `starts`,
/// and the tests pin the two to each other.
#[cfg(test)]
fn build_offset_batch(keys: &[u64], v_start: u64, v_end: u64) -> Result<RecordBatch> {
    let mut hist = vec![0i64; (v_end - v_start) as usize];
    for &k in keys {
        let slot = k.saturating_sub(v_start) as usize;
        if slot < hist.len() {
            hist[slot] += 1;
        }
    }
    let mut offsets = Vec::with_capacity(hist.len() + 1);
    let mut running = 0i64;
    offsets.push(0);
    for count in hist {
        running += count;
        offsets.push(running);
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        OFFSET_COL,
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
    fn offset_batch_is_prefix_sum_over_aligned_keys() {
        // keys of a chunk [0, 3): two edges on 0, one on 1, one on 2.
        let batch = build_offset_batch(&[0, 0, 1, 2], 0, 3).unwrap();
        assert_eq!(offsets_of(&batch), vec![0, 2, 3, 4]);
        assert_eq!(batch.schema().field(0).name(), OFFSET_COL);
    }

    #[test]
    fn offset_batch_is_relative_to_chunk_start() {
        let batch = build_offset_batch(&[10, 11, 11], 10, 12).unwrap();
        assert_eq!(offsets_of(&batch), vec![0, 1, 3]);
    }

    /// Property test: `sort_edges` agrees with the plain tuple sort
    /// `(key, other, row).sort_unstable()` on the permutation, and its
    /// `starts` equals the histogram offsets, for random inputs with
    /// out-of-range (dropped) keys.
    #[test]
    fn counting_sort_matches_plain_sort() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for round in 0..300 {
            let vertex_count = 1 + rnd() % 50;
            let n = (rnd() % 300) as usize;
            let ordered = rnd() % 2 == 0;
            let key: Vec<i64> = (0..n)
                .map(|_| {
                    if rnd() % 20 == 0 {
                        (rnd() % 100) as i64 - 50
                    } else {
                        (rnd() % vertex_count) as i64
                    }
                })
                .collect();
            let other: Vec<i64> = (0..n).map(|_| (rnd() % 7) as i64).collect();

            let (perm, starts) = sort_edges(&key, &other, vertex_count, ordered);

            let mut plain: Vec<(u64, i64, usize)> = (0..n)
                .filter(|&i| (key[i] as u64) < vertex_count)
                .map(|i| (key[i] as u64, if ordered { other[i] } else { 0 }, i))
                .collect();
            plain.sort_unstable();
            let want: Vec<usize> = plain.iter().map(|p| p.2).collect();
            assert_eq!(perm, want, "round {round} (ordered {ordered}): permutation");

            let kept: Vec<u64> = plain.iter().map(|p| p.0).collect();
            let hist = build_offset_batch(&kept, 0, vertex_count).unwrap();
            let want: Vec<usize> = offsets_of(&hist).into_iter().map(|o| o as usize).collect();
            assert_eq!(starts, want, "round {round}: starts");
        }
    }

    #[test]
    fn add_property_replaces_an_existing_name() {
        let e = Edge::new(1, 2)
            .add_property("w", PropertyValue::Int64(1))
            .add_property("w", PropertyValue::Int64(2))
            .add_property("x", PropertyValue::Bool(true));
        assert!(matches!(e.get("w"), Some(PropertyValue::Int64(2))));
        assert!(matches!(e.get("x"), Some(PropertyValue::Bool(true))));
        assert!(e.get("missing").is_none());
        assert_eq!(e.properties.len(), 2);
    }
}
