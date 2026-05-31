use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::{
    error::{GraphArError, Result},
    metadata::property::PropertyGroup,
    types::{AdjListType, FileType},
};

pub const DEFAULT_EDGE_CHUNK_SIZE: u64 = 4_194_304; // 2^22

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdjListInfo {
    pub ordered: bool,
    pub aligned_by: String,
    pub file_type: FileType,
    /// Relative prefix for adjacency list data files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

impl AdjListInfo {
    pub fn adj_list_type(&self) -> Result<AdjListType> {
        AdjListType::try_from((
            if self.ordered { "true" } else { "false" },
            self.aligned_by.as_str(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeInfo {
    pub src_type: String,
    pub edge_type: String,
    pub dst_type: String,
    #[serde(default = "default_edge_chunk_size")]
    pub chunk_size: u64,
    pub src_chunk_size: u64,
    pub dst_chunk_size: u64,
    #[serde(default)]
    pub directed: bool,
    pub prefix: String,
    #[serde(default)]
    pub adj_lists: Vec<AdjListInfo>,
    #[serde(default)]
    pub property_groups: Vec<PropertyGroup>,
    #[serde(default = "super::vertex_info::default_version")]
    pub version: String,
}

fn default_edge_chunk_size() -> u64 {
    DEFAULT_EDGE_CHUNK_SIZE
}

impl EdgeInfo {
    pub fn new(
        src_type: impl Into<String>,
        edge_type: impl Into<String>,
        dst_type: impl Into<String>,
        chunk_size: u64,
        src_chunk_size: u64,
        dst_chunk_size: u64,
        directed: bool,
    ) -> Self {
        let (src, edge, dst) = (src_type.into(), edge_type.into(), dst_type.into());
        let prefix = format!("edge/{}_{}_{}/", src, edge, dst);
        Self {
            src_type: src,
            edge_type: edge,
            dst_type: dst,
            chunk_size,
            src_chunk_size,
            dst_chunk_size,
            directed,
            prefix,
            adj_lists: Vec::new(),
            property_groups: Vec::new(),
            version: super::vertex_info::GAR_VERSION.to_string(),
        }
    }

    pub fn with_adj_list(mut self, info: AdjListInfo) -> Self {
        self.adj_lists.push(info);
        self
    }

    pub fn with_property_group(mut self, group: PropertyGroup) -> Self {
        self.property_groups.push(group);
        self
    }

    /// Number of vertex-aligned partitions for the given vertex count.
    pub fn vertex_chunk_count(&self, adj_type: &AdjListType) -> u64 {
        let vchunk_size = if adj_type.aligned_by_src() {
            self.src_chunk_size
        } else {
            self.dst_chunk_size
        };
        // Caller must supply vertex count; return the chunk size for callers to derive count.
        vchunk_size
    }

    pub fn get_adj_list_info(&self, adj_type: &AdjListType) -> Option<&AdjListInfo> {
        self.adj_lists.iter().find(|a| {
            a.adj_list_type().ok().as_ref() == Some(adj_type)
        })
    }

    /// Path to an adjacency list chunk file.
    /// `vertex_chunk` = partition index (part{n}), `edge_chunk` = chunk within partition.
    pub fn adj_list_chunk_path(
        &self,
        adj_type: &AdjListType,
        vertex_chunk: u64,
        edge_chunk: u64,
    ) -> Result<PathBuf> {
        let info = self
            .get_adj_list_info(adj_type)
            .ok_or_else(|| GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string()))?;
        Ok(PathBuf::from(&self.prefix)
            .join(adj_type.dir_name())
            .join("adj_list")
            .join(format!("part{vertex_chunk}"))
            .join(format!("chunk{}.{}", edge_chunk, info.file_type.extension())))
    }

    /// Path to the offset chunk for a given vertex chunk (ordered adj lists only).
    pub fn offset_chunk_path(
        &self,
        adj_type: &AdjListType,
        vertex_chunk: u64,
    ) -> Result<PathBuf> {
        let info = self
            .get_adj_list_info(adj_type)
            .ok_or_else(|| GraphArError::AdjListTypeNotConfigured(adj_type.dir_name().to_string()))?;
        if !adj_type.is_ordered() {
            return Err(GraphArError::Other(
                "offset files only exist for ordered adjacency lists".into(),
            ));
        }
        Ok(PathBuf::from(&self.prefix)
            .join(adj_type.dir_name())
            .join("offset")
            .join(format!("chunk{}.{}", vertex_chunk, info.file_type.extension())))
    }

    /// Path to an edge property group chunk file.
    pub fn property_chunk_path(
        &self,
        adj_type: &AdjListType,
        group: &PropertyGroup,
        vertex_chunk: u64,
        edge_chunk: u64,
    ) -> PathBuf {
        PathBuf::from(&self.prefix)
            .join(adj_type.dir_name())
            .join(group.dir_name())
            .join(format!("part{vertex_chunk}"))
            .join(format!("chunk{}.{}", edge_chunk, group.file_type.extension()))
    }

    pub fn find_property_group(&self, property_name: &str) -> Option<&PropertyGroup> {
        self.property_groups.iter().find(|g| g.has_property(property_name))
    }

    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&content)?)
    }

    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}
