# Guide

## Write a graph from Arrow tables

```rust
use graphar::{
    AdjListInfo, AdjListType, EdgeInfo, FileType, GraphInfo, Property, PropertyGroup,
    VertexInfo, DataType, write_edge_table, write_vertex_table,
};

let vi = VertexInfo::new("person", 1 << 18)
    .with_property_group(PropertyGroup::new(
        vec![Property::new("id", DataType::Int64).primary()],
        FileType::Parquet,
    ));
let ei = EdgeInfo::new("person", "knows", "person", 1 << 22, 1 << 18, 1 << 18, true)
    .with_adj_list(AdjListInfo {
        ordered: true,
        aligned_by: "src".into(),
        file_type: FileType::Parquet,
        prefix: None,
    });

// `people`: a RecordBatch with an `id` column, row i = vertex i.
let n = write_vertex_table(&vi, "out", &people)?;
// `knows`: a RecordBatch with `_graphArSrcIndex` and `_graphArDstIndex` (int64).
write_edge_table(&ei, "out", &AdjListType::OrderedBySource, &knows, n)?;

vi.save("out/person.vertex.yml")?;
ei.save("out/person_knows_person.edge.yml")?;
GraphInfo::new("social", "")
    .with_vertex("person.vertex.yml")
    .with_edge("person_knows_person.edge.yml")
    .save("out/social.graph.yml")?;
```

The row builders `VerticesBuilder` and `EdgesBuilder` write the same files from
`Vertex` / `Edge` values.

## Read it back

```rust
use graphar::{AdjListType, EdgeInfo, EdgeReader, VertexInfo, VertexReader};

let vr = VertexReader::open(VertexInfo::load("out/person.vertex.yml")?, "out")?;
let ids = vr.read_by_property("id")?;        // Vec<RecordBatch>, one group
let all = vr.read_merged()?;                 // one RecordBatch, all groups

let t = AdjListType::OrderedBySource;
let er = EdgeReader::open(EdgeInfo::load("out/person_knows_person.edge.yml")?, "out", &t)?;
let (offsets, indices) = er.read_csr(&t)?;   // CSR: offsets.len() == n + 1
```

## Chunks on an object store

```rust
use graphar::storage::{ChunkStore, MultipartOptions, RetryOptions};
use graphar::FileType;

let store = ChunkStore::from_url("s3://bucket/graphs/social")?;
store.write_chunk("vertex/person/id/chunk0", &batches, &FileType::Parquet).await?;
let back = store.read_chunk("vertex/person/id/chunk0", &FileType::Parquet).await?;
```

`RetryOptions` tunes the `object_store` retry policy (`RetryOptions::none()`
turns it off). Writes above `MultipartOptions::threshold` (16 MiB by default)
use multipart upload.

## Validate

```rust
use graphar::{validate_graph_dir, Severity};

let issues = validate_graph_dir("out/social.graph.yml");
assert!(issues.iter().all(|i| i.severity != Severity::Error));
```

`ValidateOptions::corpus()` also accepts `chunk<N>.<ext>` names and treats a
missing chunk directory as a warning.
