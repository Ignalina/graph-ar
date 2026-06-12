use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;

use crate::{
    error::{GraphArError, Result},
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
            let path = self.base_dir.join(self.info.adj_list_chunk_path(
                adj_type,
                vertex_chunk,
                edge_chunk,
            )?);
            if !path.exists() {
                break;
            }
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
        let path = self
            .base_dir
            .join(self.info.offset_chunk_path(adj_type, vertex_chunk)?);
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
            let path = self.base_dir.join(self.info.property_chunk_path(
                adj_type,
                group,
                vertex_chunk,
                edge_chunk,
            ));
            if !path.exists() {
                break;
            }
            let mut batches = io::read_chunk(&path, &group.file_type)?;
            all.append(&mut batches);
            edge_chunk += 1;
        }
        Ok(all)
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
