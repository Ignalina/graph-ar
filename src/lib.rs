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
