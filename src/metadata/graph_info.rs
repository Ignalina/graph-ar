use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::{
    error::{GraphArError, Result},
    metadata::{edge_info::EdgeInfo, vertex_info::VertexInfo},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphInfo {
    pub name: String,
    /// Root directory for all data files (relative or absolute). Optional in
    /// the `gar/v1` spec — the upstream testdata `*.graph.yml` files omit it —
    /// so it defaults to empty (data resolved relative to the info file).
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub vertices: Vec<String>,
    #[serde(default)]
    pub edges: Vec<String>,
    #[serde(default = "super::vertex_info::default_version")]
    pub version: String,
}

impl GraphInfo {
    pub fn new(name: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            prefix: prefix.into(),
            vertices: Vec::new(),
            edges: Vec::new(),
            version: super::vertex_info::GAR_VERSION.to_string(),
        }
    }

    pub fn with_vertex(mut self, vertex_info_path: impl Into<String>) -> Self {
        self.vertices.push(vertex_info_path.into());
        self
    }

    pub fn with_edge(mut self, edge_info_path: impl Into<String>) -> Self {
        self.edges.push(edge_info_path.into());
        self
    }

    /// Load graph info from a `.graph.yml` file and resolve all referenced info files.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&content)?)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Resolve and load a VertexInfo by type label.
    pub fn get_vertex_info(&self, vertex_type: &str, base_dir: impl AsRef<Path>) -> Result<VertexInfo> {
        let base = base_dir.as_ref();
        for rel in &self.vertices {
            let full = base.join(rel);
            let vi = VertexInfo::load(&full)?;
            if vi.vertex_type == vertex_type {
                return Ok(vi);
            }
        }
        Err(GraphArError::VertexTypeNotFound(vertex_type.to_string()))
    }

    /// Resolve and load an EdgeInfo by (src, edge, dst) triple.
    pub fn get_edge_info(
        &self,
        src: &str,
        edge: &str,
        dst: &str,
        base_dir: impl AsRef<Path>,
    ) -> Result<EdgeInfo> {
        let base = base_dir.as_ref();
        for rel in &self.edges {
            let full = base.join(rel);
            let ei = EdgeInfo::load(&full)?;
            if ei.src_type == src && ei.edge_type == edge && ei.dst_type == dst {
                return Ok(ei);
            }
        }
        Err(GraphArError::EdgeTypeNotFound {
            src: src.to_string(),
            edge: edge.to_string(),
            dst: dst.to_string(),
        })
    }

    /// Load all vertex infos referenced by this graph.
    pub fn all_vertex_infos(&self, base_dir: impl AsRef<Path>) -> Result<Vec<VertexInfo>> {
        let base = base_dir.as_ref();
        self.vertices.iter().map(|rel| VertexInfo::load(base.join(rel))).collect()
    }

    /// Load all edge infos referenced by this graph.
    pub fn all_edge_infos(&self, base_dir: impl AsRef<Path>) -> Result<Vec<EdgeInfo>> {
        let base = base_dir.as_ref();
        self.edges.iter().map(|rel| EdgeInfo::load(base.join(rel))).collect()
    }
}
