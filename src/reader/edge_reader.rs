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

use std::path::{Path, PathBuf};

use arrow_array::{Int64Array, RecordBatch};

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
}

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
        }
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

    /// Read all adjacency list chunks for a given vertex chunk.
    pub fn read_adj_list(
        &self,
        adj_type: &AdjListType,
        vertex_chunk: u64,
    ) -> Result<Vec<RecordBatch>> {
        let info = self.info.get_adj_list_info(adj_type).ok_or_else(|| {
            GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string())
        })?;

        let mut all = Vec::new();
        let mut edge_chunk = 0u64;
        loop {
            let Some(path) = io::resolve_chunk(
                self.base_dir.join(self.info.adj_list_chunk_path(
                    adj_type,
                    vertex_chunk,
                    edge_chunk,
                )?),
                &info.file_type,
            ) else {
                break;
            };
            let mut batches = io::read_chunk(&path, &info.file_type)?;
            all.append(&mut batches);
            edge_chunk += 1;
        }
        Ok(all)
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
            let mut batches = io::read_chunk(&path, &group.file_type)?;
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
                    let k = i64_col(b, key_col)?.values();
                    let o = i64_col(b, other_col)?.values();
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

fn i64_col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| GraphArError::Other(format!("chunk has no int64 column `{name}`")))
}
