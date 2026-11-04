//! Reserved column and file names of the `gar/v1` layout. The names match the
//! upstream C++ `GeneralParams`, so chunks written here are read by the C++,
//! Java and Spark libraries and the other way round.

/// Absolute 0-based vertex index, stored in every vertex property chunk.
pub const VERTEX_INDEX_COL: &str = "_graphArVertexIndex";
/// Source vertex index column of an adjacency-list chunk.
pub const SRC_INDEX_COL: &str = "_graphArSrcIndex";
/// Destination vertex index column of an adjacency-list chunk.
pub const DST_INDEX_COL: &str = "_graphArDstIndex";
/// Offset column of an ordered adjacency list's offset chunk.
pub const OFFSET_COL: &str = "_graphArOffset";
/// File holding a vertex type's (or an adjacency list's aligned side's) vertex count.
pub const VERTEX_COUNT_FILE: &str = "vertex_count";
/// Prefix of the per-partition edge count files (`edge_count<N>`).
pub const EDGE_COUNT_FILE_PREFIX: &str = "edge_count";
