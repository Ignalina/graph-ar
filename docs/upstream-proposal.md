<!--
DRAFT for apache/incubator-graphar. Not posted. The owner reads it first.
Shape follows #828 (native Go SDK proposal) and the enhancement template.
-->

**Title:** [Proposal] Contribute a native pure-Rust GraphAr implementation

### Describe the enhancement requested

Hello GraphAr community,

We propose to contribute our `graphar` crate, a native Rust reader and writer
for the GraphAr `gar/v1` format, to incubator-graphar as its pure-Rust
implementation, maintained here. Ignalina ApS holds the copyright and agrees to
donate it under the Apache License 2.0. I offer to maintain it.

The code is at https://www.gunnar.rs/nordisk/graph-ar (Apache-2.0, ASF
headers). Clone it with `git clone ssh://anonymous@git.gunnar.rs:2222/nordisk/graph-ar`.
A GitHub copy at https://github.com/Ignalina/graph-ar runs its CI workflow
(modelled on `rust.yaml`: license headers, typos, taplo, rustfmt, clippy,
cargo-machete, cargo-deny, nextest with and without all features, doc tests,
docs) on hosted runners; it is green.

#### Motivation

- **No C++ toolchain.** `graphar-rs` (#806) binds the C++ library through cxx,
  so every Rust user builds GraphAr C++ and needs the Arrow C++ development
  packages. A native crate builds with `cargo build` alone. (It is not free of
  C: `zstd-sys`, and `aws-lc-sys` for TLS to object stores, are compiled by the
  `cc` crate, as in most Rust data crates.)
- **Arrow-native.** It reads and writes arrow-rs `RecordBatch`es, so it plugs
  into DataFusion, arrow-flight and other arrow-rs users without a
  copy or an FFI boundary.
- **Object stores.** Chunks go through the `object_store` crate (S3, GCS,
  Azure, HTTP, local) with retry and multipart upload options.
- **Precedent.** #828 (native Go SDK) and the pure-Java modules (#956, #958,
  #960) show the project welcomes native implementations next to the bindings.

#### What exists

About 4,600 lines of Rust plus 1,800 lines of tests, on arrow-rs 59,
`parquet` 59, `orc-rust` 0.9 and `object_store` 0.14.

| area | state |
|---|---|
| Info files | `GraphInfo`, `VertexInfo`, `EdgeInfo`, `PropertyGroup`, `Property`, `AdjListInfo`: load and save; the YAML it writes loads in GraphAr C++ |
| Value types | `bool`, `int32`, `int64`, `float`, `double`, `string`, `date`, `timestamp`, `list<T>` of each scalar, in the table writers and the row builders |
| Chunk formats | The spec's set: Parquet (default, ZSTD like C++), ORC (feature, ZSTD), CSV, JSON, each behind a small `PayloadFormat` trait |
| Layout | `chunk<N>`, `_graphArVertexIndex` / `_graphArSrcIndex` / `_graphArDstIndex` / `_graphArOffset`, `vertex_count`, `edge_count<N>` |
| Writers | table writers (`write_vertex_table`, `write_edge_table`) and row builders, for all four adjacency list types, with offsets |
| Readers | vertex property groups (per chunk, merged, by property), adjacency lists, offsets, edge property groups, `read_csr` (global CSR offsets + indices), and `neighbors` (a row-window read for one vertex); chunks decode in parallel |
| Validation | `validate_graph_dir`: info rules and chunk layout |
| Tests | 93 with all features; they read the `testing` submodule |

Conformance against `testing`: every graph with chunk data reads in full
(`ldbc_sample` in Parquet, CSV, ORC and JSON, including the date, timestamp and
`list<float>` feature variants, `modern_graph`, `neo4j`, `nebula`). A generated
graph with a column of every value type, and `ldbc_sample_with_feature`, give
the same sums in both directions between this crate and the C++ library. Known
gaps, all on our list:

- Writing `list<T>` to ORC: orc-rust 0.9 has no list encoder, so it is an error
  (reading C++-written ORC lists works). CSV has no list form in either
  library.
- orc-rust writes strings without dictionary encoding. With ZSTD the files are
  still as small as those of C++ on our graph.
- Labels (`labels`, `:LABEL`), `cardinality`, `extra_info`: not modelled.
- No filter pushdown or column projection inside a group (row windows exist
  for point lookups) and no high-level `VerticesCollection` /
  `EdgesCollection`.
- Info files are read from the local file system; chunks can be on object
  stores.
- The `java/` sample uses another info schema (`label` instead of `type`) and
  is skipped.

#### Benchmark (summary)

A rustc code graph of 3.4M vertices and 4.6M edges, Parquet with ZSTD on both
sides, default chunk sizes, median of 5, warm page cache, on a shared 32-core
machine that was busy with other jobs (treat differences under about 20 % as
noise). The C++ reader reads the files the native writer wrote.

| operation | native Rust | GraphAr C++ | graphar-rs |
|---|---:|---:|---:|
| write vertices, table API | 0.69 s | 1.43 s | n/a |
| write edges, table API | 0.45 s | 1.49 s | n/a |
| write vertices, row builder | 3.78 s | 8.39 s | 8.68 s |
| write edges, row builder | 2.60 s | 8.05 s | n/a |
| read a property group | 124 ms (1 thread), 60 ms (8) | 312 ms | n/a |
| adjacency list to CSR | 85 ms (1 thread), 78 ms (8) | 109 ms | n/a |
| full scan | 492 ms (1 thread), 247 ms (8) | 815 ms | n/a |
| files on disk (vertices + edges) | 22.5 MB | 60.5 MB | n/a |

Performance work on the crate this week took the row builders and most reads
to about half their earlier time, and cut the files by 63 % (delta-packed
integer columns, which C++ reads as usual); a single-vertex neighbour lookup
went from 59 ms to 26 µs on a synthetic 10M-edge graph.

ORC chunks are now ZSTD-compressed too: on a second machine, with a newer dump
of the same kind of graph (3.1M vertices, 4.2M edges), the native files take
26.1 MB against 28.3 MB for C++.

It does not win everywhere. C++ is faster on ORC adjacency reads (146 ms
against 181 ms on the first machine, 204 ms against 222 ms on the second) and
on CSV vertex writes (0.66 s against 1.39 s). The native scan holds all decoded
batches (504 MB peak) where the C++ loop drops each chunk (195 MB). A clean
release build takes longer than `graphar-rs` (100 s against 40 s) because it
compiles arrow-rs instead of using prebuilt Arrow C++. Numbers, method and
commands: https://www.gunnar.rs/nordisk/graph-ar/blob/bench/BENCH.md

Every reader returned the same counts and sums on the files of every writer,
and on the upstream `ldbc_sample`, in Parquet and ORC, and in CSV apart from
the C++ full scan (item 5 below). While doing this we ran into a few things in
the C++ library; we can open separate issues with reproducers:

1. `AdjListOffsetArrowChunkReader::GetChunk()` returns
   `->column(0)->chunk(0)`, only the first Arrow chunk of the offset column.
   With the default vertex chunk size (2^18) a Parquet offset chunk is read in
   several Arrow chunks, so the offsets come back truncated.
2. `VertexPropertyArrowChunkReader::GetChunk()` with the default
   `GetChunkVersion::AUTO` takes the Parquet-only V2 path when no filter is
   set, so CSV and ORC property chunks fail ("Parquet magic bytes not found");
   `V1` works.
3. `VertexPropertyWriter` / `EdgeChunkWriter` / the builders write Parquet
   uncompressed when no `WriterOptions` are passed, while
   `ParquetOption::compression` defaults to ZSTD.
4. `EdgeChunkWriter::SortAndWriteAdjListTable` and
   `SortAndWritePropertyTable` failed with "Cancelled: Plan was cancelled
   early" in 7 of 60 runs. We do not know the cause; the machine was busy.
5. CSV chunks are read with inferred types, not the declared ones. A column
   that starts with empty values fails ("CSV conversion error to null"), and a
   `string` `creationDate` that looks like a timestamp comes back 2 bytes
   longer.
6. The table writer pads the last vertex chunk's offset chunk to
   `chunk_size + 1` rows; the Spark-written samples have `vertex_count + 1`
   rows there. Readers should accept both; which one does the spec intend?
7. The YAML reader rejects some valid YAML that other writers emit
   (`prefix: ''`, block sequences not indented under their key). We now write
   the upstream style; is the accepted subset documented anywhere?

#### Usage

A rustc driver of ours dumps the code graph of a Rust workspace and its
dependencies (functions, types, fields, MIR blocks; calls, uses, contains,
control flow) as Arrow. We write that as GraphAr chunks. Today the chunks
bulk-load into FalkorDB and answer Cypher over Arrow Flight SQL; we are now
evaluating them as the on-disk graph cache of our code-graph lenses, which
load the ordered-by-source adjacency list straight into a GraphBLAS CSR
matrix.

```
rustc driver ──Arrow IPC──▶ write_vertex_table / write_edge_table ──▶ GraphAr dir
                                                                       │ (Parquet chunks)         
              ┌────────────────────────────────────────────────────────┼──────────────────────────┐
              ▼                                                        ▼                          ▼
   EdgeReader::read_csr ──▶ GraphBLAS CSR matrix       readers ──▶ FalkorDB GRAPH.BULK    any GraphAr reader
   (code-graph lenses)                                 Cypher ◀── Arrow Flight SQL        (C++, Spark, Java)
```

The benchmark's large graph is one such dump: 3.4M vertices and 4.6M edges.
The FalkorDB loader and the Flight SQL server are separate crates
(`graphar-falkordb`, `graphar-flight`). They stay in our repository as users
of this crate; only the core crate moves.

#### Licence and IP

Apache License 2.0. Ignalina ApS is the sole copyright holder and agrees to the donation. As we read the Incubator's IP clearance
rules for code developed outside the ASF, this takes a Software Grant
Agreement from Ignalina ApS, an ICLA from each contributor who will commit,
and an IP clearance record that the Incubator PMC approves. We will file
whatever the mentors ask for. Every file carries the ASF header already;
NOTICE names Ignalina ApS. We also hold the `graphar` crate name on crates.io
and can transfer it to the project.

#### Extension formats

The donated crate carries only the spec's formats. `PayloadFormat` is the seam
for anything else: another crate registers a format under a `file_type` name,
and the readers and writers use it. We run Arrow IPC chunks that way, in a
small crate of our own, for a local graph cache. That crate is not part of this
proposal; if there is interest, Arrow IPC can be a separate proposal later.

#### Relation to graphar-rs

Beside it, not replacing it. `graphar-rs` gives Rust access to the full C++
feature set; the native crate is for users who cannot or do not want to build
C++. Both can share the `testing` data and the same conformance tests, so they
stay interchangeable on disk. Where it lives (for example `rust/` as a second
crate, or a new top-level directory), its name, and any later convergence
(for example `graphar-rs` reusing the native info types) are for the
maintainers to decide.

#### Proposed plan

Small PRs, each under the 300 to 500 lines that CONTRIBUTING.md asks for,
with tests against `testing` in each:

- [ ] Agree here (or in Discussions) on location, crate name and CI job.
- [ ] Skeleton: Cargo package, CI job without C++ (fmt, clippy, taplo,
      cargo-deny, tests), licence check.
- [ ] Types and info: `DataType`, `FileType`, `AdjListType`, `Property`,
      `PropertyGroup`, `VertexInfo`, `EdgeInfo`, `GraphInfo`, YAML load and
      save (two PRs).
- [ ] Chunk I/O: Parquet, CSV, ORC, JSON; reserved column names; count files.
- [ ] Readers: vertex property groups; then adjacency lists, offsets, edge
      properties and CSR.
- [ ] Validation of a graph directory.
- [ ] Writers: vertices; then edges with offsets.
- [ ] Object store backend.
- [ ] After that, from the gap list: labels, pushdown, collections.

#### Questions for the maintainers

1. Is a second, native Rust implementation welcome next to `graphar-rs`?
2. Where should it live, and under which crate name? (`graphar` is free to
   transfer to the project.)
3. Is the IP route above right for this podling, and who files the clearance?
4. Offset chunk length for the last vertex chunk: `vertex_count + 1` or
   `chunk_size + 1`?
5. Should we open the C++ findings above as separate issues first?

Thanks!

### Component(s)

Rust
