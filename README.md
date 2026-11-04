# graph-ar: native Rust for Apache GraphAr

A pure-Rust reader and writer for the [Apache GraphAr](https://github.com/apache/incubator-graphar)
`gar/v1` file format. It reads and writes the same files as the GraphAr C++,
Java and Spark libraries, with no C++ toolchain: it is built on the Rust
`arrow`, `parquet`, `orc-rust` and `object_store` crates.

This repository is prepared for contribution to Apache GraphAr. The crate name
and its place in the upstream tree are for the GraphAr maintainers to decide;
nothing is published from here.

## What it does

| area | support |
|---|---|
| Info files | `GraphInfo`, `VertexInfo`, `EdgeInfo`, `PropertyGroup`, `Property`, `AdjListInfo`: load and save `*.graph.yml`, `*.vertex.yml`, `*.edge.yml` |
| Chunk formats | Parquet, CSV, ORC (feature `orc`), JSON, plus Arrow IPC (not in the upstream `FileType` set) |
| Layout | `chunk<N>` files, `_graphArVertexIndex` / `_graphArSrcIndex` / `_graphArDstIndex` / `_graphArOffset` columns, `vertex_count` and `edge_count<N>` files |
| Write | row builders (`VerticesBuilder`, `EdgesBuilder`) and table writers (`write_vertex_table`, `write_edge_table`) for all four adjacency list types, offsets for ordered lists |
| Read | `VertexReader` (per group, merged, by property), `EdgeReader` (adjacency list, offsets, property groups, `read_csr`, `neighbors`) |
| Storage | local files; `ChunkStore` over `object_store` (S3, GCS, Azure, HTTP) with retry and multipart options |
| Validation | `validate_graph_dir`: info-file rules and the chunk layout |

Known gaps are listed in [docs/design.md](docs/design.md#gaps).

## Build and test

```shell
git submodule update --init testing   # the upstream test data
cargo test --all-features
```

The tests read the upstream `testing` data (`ldbc_sample` in Parquet, CSV and
ORC, and every `gar/v1` graph for validation). Set `GAR_TEST_DATA` to use a
checkout elsewhere.

## Example

```rust
use graphar::{AdjListType, EdgeInfo, EdgeReader, VertexInfo, VertexReader};

let base = "testing/ldbc_sample/parquet";
let vi = VertexInfo::load(format!("{base}/person.vertex.yml"))?;
let people = VertexReader::open(vi, base)?.read_merged()?;

let ei = EdgeInfo::load(format!("{base}/person_knows_person.edge.yml"))?;
let t = AdjListType::OrderedBySource;
let (offsets, indices) = EdgeReader::open(ei, base, &t)?.read_csr(&t)?;
```

More in [docs/guide.md](docs/guide.md).

## Licence

Apache License 2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE).
