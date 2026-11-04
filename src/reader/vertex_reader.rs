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

use arrow_array::RecordBatch;

use crate::{
    error::{GraphArError, Result},
    io,
    metadata::{PropertyGroup, VertexInfo},
};

/// Reads vertex chunks from disk for a given vertex type.
pub struct VertexReader {
    info: VertexInfo,
    base_dir: PathBuf,
    vertex_count: u64,
    /// Decode threads for multi-chunk reads; `None` = the default cap.
    threads: Option<usize>,
}

impl VertexReader {
    pub fn new(info: VertexInfo, base_dir: impl AsRef<Path>, vertex_count: u64) -> Self {
        Self {
            info,
            base_dir: base_dir.as_ref().to_path_buf(),
            vertex_count,
            threads: None,
        }
    }

    /// Cap the decode threads used by the multi-chunk reads (`1` = serial).
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads.max(1));
        self
    }

    /// Open a vertex type, taking the vertex count from its `vertex_count` file.
    pub fn open(info: VertexInfo, base_dir: impl AsRef<Path>) -> Result<Self> {
        let base = base_dir.as_ref();
        let vertex_count = io::read_count(base.join(info.vertex_count_path()))?;
        Ok(Self::new(info, base, vertex_count))
    }

    pub fn vertex_count(&self) -> u64 {
        self.vertex_count
    }

    pub fn vertex_info(&self) -> &VertexInfo {
        &self.info
    }

    pub fn chunk_count(&self) -> u64 {
        self.info.chunk_count(self.vertex_count)
    }

    /// Read all chunks for a property group, returning concatenated batches.
    /// Chunks decode in parallel (see [`with_threads`](Self::with_threads)).
    pub fn read_property_group(&self, group: &PropertyGroup) -> Result<Vec<RecordBatch>> {
        let n = self.chunk_count() as usize;
        let per_chunk = super::par_map(n, self.threads, |i| {
            self.read_property_group_chunk(group, i as u64)
        })?;
        Ok(per_chunk.into_iter().flatten().collect())
    }

    /// Read a single chunk for a property group.
    pub fn read_property_group_chunk(
        &self,
        group: &PropertyGroup,
        chunk_index: u64,
    ) -> Result<Vec<RecordBatch>> {
        let rel = self.info.chunk_path(group, chunk_index);
        let Some(full) = io::resolve_chunk(self.base_dir.join(&rel), &group.file_type) else {
            return Err(GraphArError::ChunkOutOfRange(
                chunk_index,
                self.chunk_count(),
            ));
        };
        io::read_chunk_typed(&full, &group.file_type, &group.arrow_schema())
    }

    /// Read every property group and merge them into one row-aligned batch
    /// holding all columns of this vertex type.
    pub fn read_merged(&self) -> Result<RecordBatch> {
        let groups = self.info.property_groups.clone();
        self.read_merged_groups(&groups)
    }

    /// Read the given property groups and merge them into one batch. Every
    /// (group, chunk) file decodes in parallel.
    pub fn read_merged_groups(&self, groups: &[PropertyGroup]) -> Result<RecordBatch> {
        let chunks = (self.chunk_count() as usize).max(1);
        let flat = super::par_map(groups.len() * chunks, self.threads, |j| {
            self.read_property_group_chunk(&groups[j / chunks], (j % chunks) as u64)
        })?;
        let per_group: Vec<Vec<RecordBatch>> = flat
            .chunks(chunks)
            .map(|g| g.iter().flatten().cloned().collect())
            .collect();
        crate::merge::merge_property_groups(&per_group)
    }

    /// Read chunks for the property group containing the named property.
    pub fn read_by_property(&self, property_name: &str) -> Result<Vec<RecordBatch>> {
        let group = self
            .info
            .find_property_group(property_name)
            .ok_or_else(|| GraphArError::PropertyNotFound(property_name.to_string()))?
            .clone();
        self.read_property_group(&group)
    }
}

/// Iterator over vertex chunks for a property group.
pub struct VertexChunkIter<'a> {
    reader: &'a VertexReader,
    group: &'a PropertyGroup,
    current: u64,
    total: u64,
}

impl<'a> VertexChunkIter<'a> {
    pub fn new(reader: &'a VertexReader, group: &'a PropertyGroup) -> Self {
        let total = reader.chunk_count();
        Self {
            reader,
            group,
            current: 0,
            total,
        }
    }
}

impl Iterator for VertexChunkIter<'_> {
    type Item = Result<Vec<RecordBatch>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.total {
            return None;
        }
        let idx = self.current;
        self.current += 1;
        Some(self.reader.read_property_group_chunk(self.group, idx))
    }
}
