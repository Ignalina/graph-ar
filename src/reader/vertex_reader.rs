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
}

impl VertexReader {
    pub fn new(info: VertexInfo, base_dir: impl AsRef<Path>, vertex_count: u64) -> Self {
        Self {
            info,
            base_dir: base_dir.as_ref().to_path_buf(),
            vertex_count,
        }
    }

    pub fn vertex_info(&self) -> &VertexInfo {
        &self.info
    }

    pub fn chunk_count(&self) -> u64 {
        self.info.chunk_count(self.vertex_count)
    }

    /// Read all chunks for a property group, returning concatenated batches.
    pub fn read_property_group(&self, group: &PropertyGroup) -> Result<Vec<RecordBatch>> {
        let mut all = Vec::new();
        for chunk_idx in 0..self.chunk_count() {
            let mut batches = self.read_property_group_chunk(group, chunk_idx)?;
            all.append(&mut batches);
        }
        Ok(all)
    }

    /// Read a single chunk for a property group.
    pub fn read_property_group_chunk(
        &self,
        group: &PropertyGroup,
        chunk_index: u64,
    ) -> Result<Vec<RecordBatch>> {
        let rel = self.info.chunk_path(group, chunk_index);
        let full = self.base_dir.join(&rel);
        if !full.exists() {
            return Err(GraphArError::ChunkOutOfRange(chunk_index, self.chunk_count()));
        }
        io::read_chunk(&full, &group.file_type)
    }

    /// Read every property group and merge them into one row-aligned batch
    /// holding all columns of this vertex type.
    pub fn read_merged(&self) -> Result<RecordBatch> {
        let groups = self.info.property_groups.clone();
        self.read_merged_groups(&groups)
    }

    /// Read the given property groups and merge them into one batch.
    pub fn read_merged_groups(&self, groups: &[PropertyGroup]) -> Result<RecordBatch> {
        let per_group: Result<Vec<_>> = groups
            .iter()
            .map(|g| self.read_property_group(g))
            .collect();
        crate::merge::merge_property_groups(&per_group?)
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
        Self { reader, group, current: 0, total }
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
