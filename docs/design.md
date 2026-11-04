# Design

A graph is a directory of chunks: column-oriented files of a fixed row count,
described by YAML info files. This crate reads and writes those chunks as Arrow
`RecordBatch`es.

## Modules

| module | role |
|---|---|
| `metadata` | `GraphInfo`, `VertexInfo`, `EdgeInfo`, `PropertyGroup`, `Property`, `AdjListInfo`; serde models of the info files and the path rules of the layout |
| `general_params` | the reserved names: `_graphArVertexIndex`, `_graphArSrcIndex`, `_graphArDstIndex`, `_graphArOffset`, `vertex_count`, `edge_count<N>` |
| `format` | `PayloadFormat`: the trait a chunk format implements, the spec's four formats, `format_for`, `register_format` |
| `io` | `read_chunk` / `write_chunk` per `FileType`, `resolve_chunk`, `read_count` / `write_count` |
| `writer` | `VerticesBuilder`, `EdgesBuilder` (row builders) and `write_vertex_table`, `write_edge_table` (Arrow tables) |
| `reader` | `VertexReader`, `EdgeReader` |
| `merge` | row-aligned join of property groups |
| `storage` | `ChunkStore` over `object_store`, with `RetryOptions` and `MultipartOptions` |
| `validate` | structural validation of a graph directory |

## Rules the code keeps

| rule | where |
|---|---|
| Every chunk round-trips the same Arrow schema in every format | `io::read_chunk`, `io::write_chunk` |
| The vertex index is the absolute 0-based row number; edges reference endpoints by it | `writer::vertex_writer::with_vertex_index`, `Edge::new(src, dst)` |
| Chunk files are `chunk<N>` without extension; `chunk<N>.<ext>` (written by older versions) is still read | `VertexInfo::chunk_path`, `EdgeInfo::*_path`, `io::resolve_chunk` |
| Count files are one little-endian int64 | `io::read_count`, `io::write_count` |
| Ordered adjacency lists are sorted by the aligned endpoint, then the other endpoint; offsets are per vertex chunk, `chunk_vertices + 1` entries, relative to the partition | `writer::write_edge_table` |
| Partitions may be sparse (a vertex chunk with no edges has no `part<N>` directory); numbering inside a partition is gapless | `validate` |
| Storage is `object_store`-generic: one API for local, S3, GCS and Azure | `storage::ChunkStore` |

## Chunk formats

`PayloadFormat` turns Arrow batches into a chunk and back: `name` (the
`file_type` value), `extension`, `read_bytes` and `write_bytes`, with
`read_chunk` and `write_chunk` for files defaulting to those two. The spec's
formats are built in. Any other `file_type` value parses as
`FileType::Extension(name)` and resolves through a format registered with
`register_format`; a crate outside this one provides it, and an unregistered
name is an error on read and in `validate`.

```
file_type: <name> ──▶ FileType ──▶ format_for ──▶ built-in, or the registry
                                                     │ Arc<dyn PayloadFormat>
                         every reader and writer ◀───┘ one lookup per chunk
```

A chunk costs one lookup, O(1), plus one virtual call, against decoding the
whole chunk, so the trait adds nothing measurable to a read. Row-window reads
stay Parquet-only pushdown; other formats read the chunk and slice it.

## Write path

`write_vertex_table(info, base, table)` takes one column per property, casts a
column when its type differs from the declared one, slices the table into
`chunk_size` rows, and writes each property group's columns with the vertex
index column, then `vertex_count`. `VerticesBuilder` does the same from rows.

`write_edge_table(info, base, adj_type, edges, vertex_count)` takes
`_graphArSrcIndex`, `_graphArDstIndex` and the property columns, sorts once by
`(aligned endpoint, other endpoint, row)`, applies the permutation with Arrow
`take`, and writes per vertex chunk: adjacency list chunks of `chunk_size`
edges, property group chunks, the offset chunk (ordered lists), and
`edge_count<N>`; then `vertex_count`. `EdgesBuilder` builds the same table from
rows and calls it.

## Read path

`VertexReader::open(info, base)` takes the vertex count from `vertex_count`.
`read_property_group` reads a group's chunks; `read_merged` joins all groups
and keeps one vertex index column.

`EdgeReader::open(info, base, adj_type)` takes the vertex counts from the
adjacency lists' `vertex_count` files. `read_csr(adj_type)` returns global CSR
`offsets` and `indices`: from the offset chunks for ordered lists, by a
counting sort for unordered lists. These arrays go straight into a CSR graph
structure such as a GraphBLAS matrix.

## Gaps

| gap | state |
|---|---|
| `list<T>` in CSV and ORC | CSV has no list form; orc-rust 0.9 has no list encoder, so writing one is an error (reading works). Parquet, JSON and Arrow IPC write them |
| Labels (`labels`, `:LABEL` column) | not modelled |
| `extra_info`, `cardinality` | not modelled |
| Filter pushdown and column projection inside a group | not implemented; row-window reads exist for point lookups (`EdgeReader::neighbors`, `read_adj_list_rows`) |
| High-level collections and iterators (`VerticesCollection`, `EdgesCollection`) | not implemented; readers return Arrow batches |
| Info files on object stores | `GraphInfo::load` reads local files; chunks can be on object stores through `ChunkStore` |
| Edge `vertex_count` of the non-aligned side | read only when an adjacency list aligned by that side exists |
| `time` values in the row builders | `time` is not a GraphAr C++ type; the row builders return an error |
| ORC strings | orc-rust 0.9 writes them directly, without dictionary encoding; chunks are ZSTD-compressed |
