mod tests;

pub mod error;
pub mod io;
pub mod merge;
pub mod metadata;
pub mod reader;
pub mod storage;
pub mod types;
pub mod validate;
pub mod writer;

pub use error::{GraphArError, Result};
pub use merge::merge_property_groups;
pub use metadata::{AdjListInfo, EdgeInfo, GraphInfo, Property, PropertyGroup, VertexInfo};
pub use reader::{EdgeReader, VertexReader};
pub use types::{AdjListType, DataType, FileType};
pub use validate::{
    Severity, ValidateOptions, ValidationIssue, validate_edge_info, validate_graph_dir,
    validate_graph_dir_with, validate_graph_info, validate_vertex_info,
};
pub use writer::{Edge, EdgesBuilder, PropertyValue, Vertex, VerticesBuilder};

/// **Introspection / emit marker** — record one functional-status row for the
/// nornir test matrix. Wraps `nornir_testmatrix::functional_status` behind the
/// `testmatrix` feature (a compiled-out `#[inline]` no-op otherwise, with no
/// nornir dep). The gar/v1 [`storage::ChunkStore`] write path and the
/// [`validate`] graph-info validator call this so `nornir test --features
/// testmatrix` SEES each surface's health.
#[inline]
pub fn functional_status(component: &str, check: &str, ok: bool, detail: &str) {
    #[cfg(feature = "testmatrix")]
    nornir_testmatrix::functional_status(component, check, ok, detail);
    #[cfg(not(feature = "testmatrix"))]
    {
        let _ = (component, check, ok, detail);
    }
}
