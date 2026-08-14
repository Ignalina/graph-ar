use thiserror::Error;

pub type Result<T> = std::result::Result<T, GraphArError>;

#[derive(Debug, Error)]
pub enum GraphArError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[cfg(feature = "orc")]
    #[error("ORC error: {0}")]
    Orc(#[from] orc_rust::error::OrcError),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Vertex type '{0}' not found in graph")]
    VertexTypeNotFound(String),

    #[error("Edge type '{src}'-'{edge}'-'{dst}' not found in graph")]
    EdgeTypeNotFound {
        src: String,
        edge: String,
        dst: String,
    },

    #[error("Property '{0}' not found in property group")]
    PropertyNotFound(String),

    #[error("Property group not found for properties: {0:?}")]
    PropertyGroupNotFound(Vec<String>),

    #[error("Invalid adjacency list type: ordered={ordered}, aligned_by={aligned_by}")]
    InvalidAdjListType { ordered: String, aligned_by: String },

    #[error("Adjacency list type '{0}' not configured for this edge")]
    AdjListTypeNotConfigured(String),

    #[error("Unsupported file type for operation: {0}")]
    UnsupportedFileType(String),

    #[error("Chunk index {0} out of range (max {1})")]
    ChunkOutOfRange(u64, u64),

    #[error("Invalid graph format version: {0}")]
    InvalidVersion(String),

    #[error("{0}")]
    Other(String),
}
