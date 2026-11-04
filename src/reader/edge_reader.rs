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
    sync::{Arc, Mutex},
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::DataType;

use crate::{
    error::{GraphArError, Result},
    general_params::{DST_INDEX_COL, OFFSET_COL, SRC_INDEX_COL},
    io,
    metadata::{EdgeInfo, PropertyGroup},
    types::AdjListType,
};

/// Reads edge chunks (adj list + properties + offsets) from disk.
pub struct EdgeReader {
    info: EdgeInfo,
    base_dir: PathBuf,
    /// Total number of source vertices (used to derive vertex chunk count).
    src_vertex_count: u64,
    /// Total number of destination vertices.
    dst_vertex_count: u64,
    /// Decode threads for multi-chunk reads; `None` = the default cap.
    threads: Option<usize>,
    /// Parsed parquet footers of the chunk files touched by point lookups.
    meta_cache: io::parquet::MetadataCache,
    /// Decoded offset arrays per (adj dir, vertex chunk), filled by
    /// [`edge_range`](Self::edge_range) on first use (8 B per vertex).
    offsets: Mutex<OffsetsCache>,
}

/// Decoded offset arrays keyed by (adjacency dir name, vertex chunk).
type OffsetsCache = HashMap<(&'static str, u64), Arc<Vec<i64>>>;

impl EdgeReader {
    pub fn new(
        info: EdgeInfo,
        base_dir: impl AsRef<Path>,
        src_vertex_count: u64,
        dst_vertex_count: u64,
    ) -> Self {
        Self {
            info,
            base_dir: base_dir.as_ref().to_path_buf(),
            src_vertex_count,
            dst_vertex_count,
            threads: None,
            meta_cache: io::parquet::MetadataCache::new(),
            offsets: Mutex::new(HashMap::new()),
        }
    }

    /// Cap the decode threads used by the multi-chunk reads (`1` = serial).
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads.max(1));
        self
    }

    /// Open an edge type, taking the vertex counts from the `vertex_count`
    /// files of its adjacency lists. `adj_type` must be configured and have its
    /// `vertex_count` file; the other side's count comes from any other
    /// adjacency list aligned by that side, or stays 0.
    pub fn open(
        info: EdgeInfo,
        base_dir: impl AsRef<Path>,
        adj_type: &AdjListType,
    ) -> Result<Self> {
        let base = base_dir.as_ref();
        let needed = io::read_count(base.join(info.vertex_count_path(adj_type)))?;
        let mut src = 0;
        let mut dst = 0;
        for a in &info.adj_lists {
            let Ok(t) = a.adj_list_type() else { continue };
            if let Ok(n) = io::read_count(base.join(info.vertex_count_path(&t))) {
                if t.aligned_by_src() {
                    src = n;
                } else {
                    dst = n;
                }
            }
        }
        if adj_type.aligned_by_src() {
            src = needed;
        } else {
            dst = needed;
        }
        Ok(Self::new(info, base, src, dst))
    }

    pub fn edge_info(&self) -> &EdgeInfo {
        &self.info
    }

    fn vertex_chunk_count(&self, adj_type: &AdjListType) -> u64 {
        if adj_type.aligned_by_src() {
            self.src_vertex_count.div_ceil(self.info.src_chunk_size)
        } else {
            self.dst_vertex_count.div_ceil(self.info.dst_chunk_size)
        }
    }

    /// The adjacency chunk files of a vertex chunk, in edge-chunk order.
    fn adj_chunk_files(&self, adj_type: &AdjListType, vertex_chunk: u64) -> Result<Vec<PathBuf>> {
        let info = self.info.get_adj_list_info(adj_type).ok_or_else(|| {
            GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string())
        })?;
        let mut files = Vec::new();
        while let Some(path) = io::resolve_chunk(
            self.base_dir.join(self.info.adj_list_chunk_path(
                adj_type,
                vertex_chunk,
                files.len() as u64,
            )?),
            &info.file_type,
        ) {
            files.push(path);
        }
        Ok(files)
    }

    /// Read all adjacency list chunks for a given vertex chunk. The chunk
    /// files decode in parallel (see [`with_threads`](Self::with_threads)).
    pub fn read_adj_list(
        &self,
        adj_type: &AdjListType,
        vertex_chunk: u64,
    ) -> Result<Vec<RecordBatch>> {
        let info = self.info.get_adj_list_info(adj_type).ok_or_else(|| {
            GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string())
        })?;
        let files = self.adj_chunk_files(adj_type, vertex_chunk)?;
        let per_file = super::par_map(files.len(), self.threads, |i| {
            io::read_chunk(&files[i], &info.file_type)
        })?;
        Ok(per_file.into_iter().flatten().collect())
    }

    /// Every vertex chunk's adjacency list, `(vertex_chunk, batches)` in
    /// order, all chunk files decoding in parallel. The parallel form of
    /// [`iter_vertex_chunks`](Self::iter_vertex_chunks).
    pub fn read_all_adj_lists(
        &self,
        adj_type: &AdjListType,
    ) -> Result<Vec<(u64, Vec<RecordBatch>)>> {
        let info = self.info.get_adj_list_info(adj_type).ok_or_else(|| {
            GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string())
        })?;
        let vchunks = self.vertex_chunk_count(adj_type);
        let mut jobs: Vec<(u64, PathBuf)> = Vec::new();
        for vc in 0..vchunks {
            for path in self.adj_chunk_files(adj_type, vc)? {
                jobs.push((vc, path));
            }
        }
        let decoded = super::par_map(jobs.len(), self.threads, |j| {
            io::read_chunk(&jobs[j].1, &info.file_type)
        })?;
        let mut out: Vec<(u64, Vec<RecordBatch>)> =
            (0..vchunks).map(|vc| (vc, Vec::new())).collect();
        for ((vc, _), batches) in jobs.into_iter().zip(decoded) {
            out[vc as usize].1.extend(batches);
        }
        Ok(out)
    }

    /// The decoded offset array of a vertex chunk (`vertex_chunk_size + 1`
    /// entries, padding included), read once and kept.
    fn offsets_of(&self, adj_type: &AdjListType, vertex_chunk: u64) -> Result<Arc<Vec<i64>>> {
        let key = (adj_type.dir_name(), vertex_chunk);
        if let Some(o) = self.offsets.lock().unwrap().get(&key) {
            return Ok(o.clone());
        }
        let batches = self.read_offset(adj_type, vertex_chunk)?;
        let mut off = Vec::new();
        for b in &batches {
            off.extend_from_slice(i64_col(b, OFFSET_COL)?.values());
        }
        let off = Arc::new(off);
        self.offsets.lock().unwrap().insert(key, off.clone());
        Ok(off)
    }

    /// The edge rows of vertex `v` in an ordered adjacency list:
    /// `(vertex_chunk, begin, end)` where `[begin, end)` indexes the rows of
    /// that vertex chunk's adjacency list (edge chunks concatenated).
    ///
    /// The vertex chunk's offset array is decoded on first touch and kept
    /// (8 B per vertex of that chunk), so this is an array index afterwards:
    /// `slot = v mod vertex_chunk_size`, range `offsets[slot]..offsets[slot + 1]`.
    pub fn edge_range(&self, adj_type: &AdjListType, v: u64) -> Result<(u64, u64, u64)> {
        if !adj_type.is_ordered() {
            return Err(GraphArError::Other(
                "edge_range needs an ordered adjacency list (offsets)".into(),
            ));
        }
        let vchunk_size = if adj_type.aligned_by_src() {
            self.info.src_chunk_size
        } else {
            self.info.dst_chunk_size
        };
        let (vc, slot) = (v / vchunk_size, (v % vchunk_size) as usize);
        let off = self.offsets_of(adj_type, vc)?;
        if slot + 1 >= off.len() {
            return Err(GraphArError::Other(format!(
                "offset chunk {vc} of {} has {} rows, vertex {v} needs slot {slot}",
                adj_type.dir_name(),
                off.len()
            )));
        }
        Ok((vc, off[slot] as u64, off[slot + 1] as u64))
    }

    /// Rows `[begin, end)` of a vertex chunk's adjacency list, projected to
    /// `columns` (`None` = both index columns). The range may span several
    /// edge chunks; each is read through the pushdown path
    /// ([`io::read_chunk_rows`]), O(pages touched).
    pub fn read_adj_list_rows(
        &self,
        adj_type: &AdjListType,
        vertex_chunk: u64,
        begin: u64,
        end: u64,
        columns: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>> {
        let info = self.info.get_adj_list_info(adj_type).ok_or_else(|| {
            GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string())
        })?;
        let mut out = Vec::new();
        let mut row = begin;
        while row < end {
            let ec = row / self.info.chunk_size;
            let in_chunk = row % self.info.chunk_size;
            let take = (end - row).min(self.info.chunk_size - in_chunk);
            let rel = self.info.adj_list_chunk_path(adj_type, vertex_chunk, ec)?;
            let path = io::resolve_chunk(self.base_dir.join(&rel), &info.file_type)
                .ok_or_else(|| GraphArError::ChunkOutOfRange(ec, 0))?;
            out.extend(io::read_chunk_rows_cached(
                &path,
                &info.file_type,
                in_chunk as usize,
                take as usize,
                columns,
                Some(&self.meta_cache),
            )?);
            row += take;
        }
        Ok(out)
    }

    /// Rows `[begin, end)` of a property group of a vertex chunk's edges
    /// (row-aligned with the adjacency list), projected to `columns`.
    pub fn read_property_rows(
        &self,
        adj_type: &AdjListType,
        group: &PropertyGroup,
        vertex_chunk: u64,
        begin: u64,
        end: u64,
        columns: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        let mut row = begin;
        while row < end {
            let ec = row / self.info.chunk_size;
            let in_chunk = row % self.info.chunk_size;
            let take = (end - row).min(self.info.chunk_size - in_chunk);
            let rel = self
                .info
                .property_chunk_path(adj_type, group, vertex_chunk, ec);
            let path = io::resolve_chunk(self.base_dir.join(&rel), &group.file_type)
                .ok_or_else(|| GraphArError::ChunkOutOfRange(ec, 0))?;
            out.extend(io::read_chunk_rows_cached(
                &path,
                &group.file_type,
                in_chunk as usize,
                take as usize,
                columns,
                Some(&self.meta_cache),
            )?);
            row += take;
        }
        Ok(out)
    }

    /// The neighbours of vertex `v`: the destination ids of its edges in an
    /// ordered-by-source list, or the source ids in an ordered-by-dest list.
    /// A point lookup through [`edge_range`](Self::edge_range) and
    /// [`read_adj_list_rows`](Self::read_adj_list_rows): O(pages touched).
    pub fn neighbors(&self, adj_type: &AdjListType, v: u64) -> Result<Vec<i64>> {
        let (vc, begin, end) = self.edge_range(adj_type, v)?;
        let col = if adj_type.aligned_by_src() {
            DST_INDEX_COL
        } else {
            SRC_INDEX_COL
        };
        let rows = self.read_adj_list_rows(adj_type, vc, begin, end, Some(&[col]))?;
        let mut out = Vec::new();
        for b in &rows {
            out.extend_from_slice(i64_col(b, col)?.values());
        }
        Ok(out)
    }

    /// Read the offset chunk for a vertex chunk (ordered adj lists only).
    pub fn read_offset(
        &self,
        adj_type: &AdjListType,
        vertex_chunk: u64,
    ) -> Result<Vec<RecordBatch>> {
        if !adj_type.is_ordered() {
            return Err(GraphArError::Other(
                "offsets only exist for ordered adjacency lists".into(),
            ));
        }
        let info = self.info.get_adj_list_info(adj_type).ok_or_else(|| {
            GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string())
        })?;
        let rel = self.info.offset_chunk_path(adj_type, vertex_chunk)?;
        let path =
            io::resolve_chunk(self.base_dir.join(&rel), &info.file_type).ok_or_else(|| {
                GraphArError::ChunkOutOfRange(vertex_chunk, self.vertex_chunk_count(adj_type))
            })?;
        io::read_chunk(&path, &info.file_type)
    }

    /// Read all edge property chunks for a given vertex chunk.
    pub fn read_property_group(
        &self,
        adj_type: &AdjListType,
        group: &PropertyGroup,
        vertex_chunk: u64,
    ) -> Result<Vec<RecordBatch>> {
        let mut all = Vec::new();
        let mut edge_chunk = 0u64;
        loop {
            let Some(path) = io::resolve_chunk(
                self.base_dir.join(self.info.property_chunk_path(
                    adj_type,
                    group,
                    vertex_chunk,
                    edge_chunk,
                )),
                &group.file_type,
            ) else {
                break;
            };
            let mut batches = io::read_chunk_typed(&path, &group.file_type, &group.arrow_schema())?;
            all.append(&mut batches);
            edge_chunk += 1;
        }
        Ok(all)
    }

    /// Read one adjacency list as CSR: `offsets` (aligned-side vertex count + 1
    /// entries, global) and `indices` (the other endpoint of every edge, in
    /// file order). Ordered lists use their offset chunks; unordered lists are
    /// bucketed by a counting sort. This is the layout GraphBLAS and most graph
    /// kernels take directly.
    pub fn read_csr(&self, adj_type: &AdjListType) -> Result<(Vec<i64>, Vec<i64>)> {
        let aligned_by_src = adj_type.aligned_by_src();
        let (n, chunk) = if aligned_by_src {
            (self.src_vertex_count, self.info.src_chunk_size)
        } else {
            (self.dst_vertex_count, self.info.dst_chunk_size)
        };
        let (key_col, other_col) = if aligned_by_src {
            (SRC_INDEX_COL, DST_INDEX_COL)
        } else {
            (DST_INDEX_COL, SRC_INDEX_COL)
        };
        let mut offsets = Vec::with_capacity(n as usize + 1);
        offsets.push(0i64);
        let mut indices: Vec<i64> = Vec::new();
        for vc in 0..n.div_ceil(chunk) {
            let v_start = vc * chunk;
            let len = chunk.min(n - v_start) as usize;
            let batches = self.read_adj_list(adj_type, vc)?;
            let base = *offsets.last().unwrap_or(&0);
            if adj_type.is_ordered() {
                for b in &batches {
                    indices.extend_from_slice(i64_col(b, other_col)?.values());
                }
                let local: Vec<i64> = match self.read_offset(adj_type, vc) {
                    Ok(off) => off
                        .iter()
                        .map(|b| i64_col(b, OFFSET_COL).map(|a| a.values().to_vec()))
                        .collect::<Result<Vec<_>>>()?
                        .concat(),
                    Err(GraphArError::ChunkOutOfRange(..)) => vec![0; len + 1],
                    Err(e) => return Err(e),
                };
                // The C++ table writer pads the last chunk's offsets to
                // chunk_size + 1 rows; the padding repeats the final offset.
                let mut local = local;
                if local.len() > len + 1 && local[len + 1..].iter().all(|&o| o == local[len]) {
                    local.truncate(len + 1);
                }
                if local.len() != len + 1 {
                    return Err(GraphArError::Other(format!(
                        "offset chunk {vc}: expected {} rows, found {}",
                        len + 1,
                        local.len()
                    )));
                }
                offsets.extend(local[1..].iter().map(|o| base + o));
            } else {
                let mut counts = vec![0i64; len];
                let mut pairs: Vec<(usize, i64)> = Vec::new();
                for b in &batches {
                    let (k, o) = (i64_col(b, key_col)?, i64_col(b, other_col)?);
                    let (k, o) = (k.values(), o.values());
                    for (&k, &o) in k.iter().zip(o) {
                        let slot = (k as u64).wrapping_sub(v_start) as usize;
                        if slot >= len {
                            return Err(GraphArError::Other(format!(
                                "edge key {k} outside vertex chunk {vc}"
                            )));
                        }
                        counts[slot] += 1;
                        pairs.push((slot, o));
                    }
                }
                let mut pos = Vec::with_capacity(len);
                let mut running = 0i64;
                for c in &counts {
                    pos.push(running);
                    running += c;
                    offsets.push(base + running);
                }
                let start = indices.len();
                indices.resize(start + pairs.len(), 0);
                for (slot, o) in pairs {
                    indices[start + pos[slot] as usize] = o;
                    pos[slot] += 1;
                }
            }
            if *offsets.last().unwrap_or(&0) as usize != indices.len() {
                return Err(GraphArError::Other(format!(
                    "vertex chunk {vc}: offsets end at {} but {} edges were read",
                    offsets.last().unwrap_or(&0),
                    indices.len()
                )));
            }
        }
        Ok((offsets, indices))
    }

    /// Iterate over all vertex chunks, yielding adj list + optionally offset batches.
    pub fn iter_vertex_chunks<'a>(
        &'a self,
        adj_type: &'a AdjListType,
    ) -> impl Iterator<Item = Result<(u64, Vec<RecordBatch>)>> + 'a {
        let count = self.vertex_chunk_count(adj_type);
        (0..count).map(move |vc| {
            let batches = self.read_adj_list(adj_type, vc)?;
            Ok((vc, batches))
        })
    }
}

/// An int64 view of an index or offset column; int32 and unsigned columns
/// (written by some producers, e.g. Spark for offsets) are widened.
fn i64_col(b: &RecordBatch, name: &str) -> Result<Int64Array> {
    let col = b
        .column_by_name(name)
        .ok_or_else(|| GraphArError::Other(format!("chunk has no column `{name}`")))?;
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.clone());
    }
    match col.data_type() {
        DataType::Int32
        | DataType::UInt32
        | DataType::Int16
        | DataType::UInt16
        | DataType::UInt64 => {
            let cast = arrow::compute::cast(col, &DataType::Int64)?;
            Ok(cast.as_any().downcast_ref::<Int64Array>().unwrap().clone())
        }
        other => Err(GraphArError::Other(format!(
            "column `{name}` is {other}, not an integer index column"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metadata::{AdjListInfo, Property, PropertyGroup},
        types::{DataType, FileType},
        writer::{Edge, EdgesBuilder, PropertyValue},
    };

    fn col(batches: &[RecordBatch], name: &str) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            out.extend_from_slice(i64_col(b, name).unwrap().values());
        }
        out
    }

    fn xorshift(seed: u64) -> impl FnMut() -> u64 {
        let mut x = seed;
        move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        }
    }

    fn small_info(vchunk: u64, echunk: u64, with_prop: bool) -> EdgeInfo {
        let info =
            EdgeInfo::new("a", "e", "a", echunk, vchunk, vchunk, true).with_adj_list(AdjListInfo {
                ordered: true,
                aligned_by: "src".into(),
                file_type: FileType::Parquet,
                prefix: None,
            });
        if with_prop {
            info.with_property_group(PropertyGroup::new(
                vec![Property::new("w", DataType::Int64)],
                FileType::Parquet,
            ))
        } else {
            info
        }
    }

    /// Property test: for random small graphs (tiny chunks, so ranges cross
    /// edge-chunk borders), `neighbors(v)` through the pushdown path equals
    /// the plain full read + offset slice, for every vertex, and the property
    /// rows line up with the adjacency rows.
    #[test]
    fn neighbors_pushdown_matches_plain_read() {
        let mut rnd = xorshift(0xD1B5_4A32_D192_ED03);
        for round in 0..30 {
            let dir = tempfile::tempdir().unwrap();
            let vertex_count = 1 + rnd() % 40;
            let vchunk = 1 + rnd() % 8;
            let echunk = 1 + rnd() % 6;
            let info = small_info(vchunk, echunk, true);
            let mut b = EdgesBuilder::new(
                info.clone(),
                dir.path(),
                AdjListType::OrderedBySource,
                vertex_count,
            );
            let n = rnd() % 200;
            for i in 0..n {
                let (s, d) = (rnd() % vertex_count, rnd() % vertex_count);
                b.add_edge(
                    Edge::new(s as i64, d as i64).add_property("w", PropertyValue::Int64(i as i64)),
                );
            }
            b.dump().unwrap();

            let r = EdgeReader::new(info.clone(), dir.path(), vertex_count, vertex_count);
            let t = AdjListType::OrderedBySource;
            for v in 0..vertex_count {
                let vc = v / vchunk;
                let off = col(&r.read_offset(&t, vc).unwrap(), OFFSET_COL);
                let slot = (v % vchunk) as usize;
                let (lo, hi) = (off[slot] as usize, off[slot + 1] as usize);
                let adj = r.read_adj_list(&t, vc).unwrap();
                let plain: Vec<i64> = col(&adj, DST_INDEX_COL)[lo..hi].to_vec();
                assert!(
                    col(&adj, SRC_INDEX_COL)[lo..hi]
                        .iter()
                        .all(|&s| s == v as i64),
                    "round {round}: src column"
                );

                let fast = r.neighbors(&t, v).unwrap();
                assert_eq!(fast, plain, "round {round}: neighbors of {v}");

                let (vc2, b2, e2) = r.edge_range(&t, v).unwrap();
                assert_eq!((vc2, b2 as usize, e2 as usize), (vc, lo, hi));
                let group = &info.property_groups[0];
                let w = col(
                    &r.read_property_rows(&t, group, vc, b2, e2, None).unwrap(),
                    "w",
                );
                let w_plain =
                    col(&r.read_property_group(&t, group, vc).unwrap(), "w")[lo..hi].to_vec();
                assert_eq!(w, w_plain, "round {round}: property rows of {v}");
            }
        }
    }

    /// Property test: parallel decode returns exactly what the serial path
    /// returns, chunk by chunk, for random graphs with several edge chunks
    /// per vertex chunk.
    #[test]
    fn parallel_reads_match_serial() {
        let mut rnd = xorshift(0x2545_F491_4F6C_DD1D);
        for round in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let vertex_count = 1 + rnd() % 30;
            let vchunk = 1 + rnd() % 8;
            let echunk = 1 + rnd() % 4;
            let info = small_info(vchunk, echunk, false);
            let mut b = EdgesBuilder::new(
                info.clone(),
                dir.path(),
                AdjListType::OrderedBySource,
                vertex_count,
            );
            for _ in 0..rnd() % 300 {
                b.add_edge(Edge::new(
                    (rnd() % vertex_count) as i64,
                    (rnd() % vertex_count) as i64,
                ));
            }
            b.dump().unwrap();
            let t = AdjListType::OrderedBySource;
            let serial = EdgeReader::new(info.clone(), dir.path(), vertex_count, vertex_count)
                .with_threads(1);
            let par = EdgeReader::new(info.clone(), dir.path(), vertex_count, vertex_count)
                .with_threads(5);
            let want: Vec<(u64, Vec<i64>, Vec<i64>)> = serial
                .iter_vertex_chunks(&t)
                .map(|r| {
                    let (vc, b) = r.unwrap();
                    (vc, col(&b, SRC_INDEX_COL), col(&b, DST_INDEX_COL))
                })
                .collect();
            let got: Vec<(u64, Vec<i64>, Vec<i64>)> = par
                .read_all_adj_lists(&t)
                .unwrap()
                .into_iter()
                .map(|(vc, b)| (vc, col(&b, SRC_INDEX_COL), col(&b, DST_INDEX_COL)))
                .collect();
            assert_eq!(got, want, "round {round}: read_all_adj_lists");
            for vc in 0..vertex_count.div_ceil(vchunk) {
                let a = par.read_adj_list(&t, vc).unwrap();
                let s = serial.read_adj_list(&t, vc).unwrap();
                assert_eq!(
                    col(&a, DST_INDEX_COL),
                    col(&s, DST_INDEX_COL),
                    "round {round}: chunk {vc}"
                );
            }
        }
    }
}
