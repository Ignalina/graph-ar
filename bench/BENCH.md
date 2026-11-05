<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native Rust GraphAr: bench and conformance

Date: 2026-11-04. Code: https://www.gunnar.rs/nordisk/graph-ar, harnesses in `bench/`. Medians per run: [raw-summary.md](raw-summary.md).
"Native before" is main `cd86afc` (upstream layout, ZSTD, before the performance work). "Native after" is main `d85f31f` (with the performance work: delta-packed integer columns, counting sort, parallel chunk decode).

## Headline

Graph: a rustc code graph dump of three Rust workspaces with all their dependencies, 3,421,126 vertices and 4,566,534 edges. Parquet with ZSTD on both sides, median of 5, page cache warm. Reads run on the files the native writer wrote; the C++ reader reads the same files. C++ was run twice with the same binary, once per native version, so the two C++ columns also show the noise of this host.

| operation | native before | native after | C++ (run 1) | C++ (run 2) | graphar-rs |
|---|---:|---:|---:|---:|---:|
| write vertices, table API | 1.05 s | 685 ms | 1.38 s | 1.43 s | - |
| write edges, table API | 1.04 s | 450 ms | 1.61 s | 1.49 s | - |
| write vertices, row builder | 8.01 s | 3.78 s | 8.96 s | 8.39 s | 8.68 s |
| write edges, row builder | 6.39 s | 2.60 s | 8.78 s | 8.05 s | - |
| read a property group | 159 ms | 116 ms | 370 ms | 343 ms | - |
| adjacency list to CSR | 168 ms | 92 ms | 202 ms | 121 ms | - |
| full scan | 732 ms | 400 ms | 1.04 s | 878 ms | - |

Where the native crate loses or the result needs a caveat:

- ORC size in these tables: 461 + 55 MB against 16 + 15 MB for C++. The native writer did not compress ORC then; that is fixed, see the update below.
- ORC adjacency to CSR: C++ is faster (146 ms against 181 ms).
- CSV vertex table write: C++ is twice as fast (0.66 s against 1.39 s).
- Peak RSS of `read-prop` and `scan`: the native harness holds all decoded batches (183 and 504 MB); the C++ harness drops each chunk (104 and 195 MB). The native reader API returns all batches of a group at once; `read_csr` is the other way round (83 MB against 159 MB).
- Native multi-chunk reads decode up to 8 chunks in parallel by default; the C++ harness loops over the chunks in one thread of its own (Arrow may use its own pool inside). The serial numbers are below.
- Build: a clean `cargo build --release` takes 100 s for the native crate (it compiles arrow-rs and object_store) and 40 s for graphar-rs (GraphAr C++ only, Arrow C++ comes prebuilt from apt). The native crate needs no Arrow C++ packages and no C++ compiler, but it is not the faster build. Both with `-j 8`, on the loaded host.
- The native crate is not C-free: `zstd-sys` and `aws-lc-sys` are compiled by `cc`.
- graphar-rs 0.1.0 only has the vertex row builder, so it appears in one row.

### Serial decode (same files, `BENCH_THREADS`)

Native after (`2d757cb`) against C++ on the native-written Parquet graph, median of 5:

| operation | native, 1 thread | native, 8 threads | C++ |
|---|---:|---:|---:|
| read a property group | 124 ms | 60 ms | 312 ms |
| adjacency list to CSR | 85 ms | 78 ms | 109 ms |
| full scan | 492 ms | 247 ms | 815 ms |

### Point lookup (native only)

`EdgeReader::neighbors` reads one row window instead of a whole chunk. On a synthetic graph of 10M edges and 1M vertices it went from 59 ms to 26 µs per query (docs/perf.md). This is the crate's own stage bench, not compared with C++.

### Peak RSS (code graph, Parquet)

| operation | native before | native after | C++ |
|---|---:|---:|---:|
| write-edges-table (holds the input tables too) | 1046 MB | 1046 MB | 1203 MB |
| write-edges-builder (holds the input tables too) | 4677 MB | 2911 MB | 4673 MB |
| read-prop | 180 MB | 183 MB | 104 MB |
| read-csr | 83 MB | 83 MB | 159 MB |
| scan | 504 MB | 504 MB | 195 MB |

### Bytes on disk (vertices + edges, code graph)

| format | native before | native after | C++ |
|---|---:|---:|---:|
| parquet | 35.8 + 24.5 MB | 15.2 + 7.3 MB | 35.9 + 24.6 MB |
| csv | 555.3 + 140.5 MB | 555.3 + 140.5 MB | 582.7 + 151.1 MB |
| orc | 461.1 + 55.4 MB | 461.1 + 55.4 MB | 16.0 + 14.8 MB |

The C++ writers write Parquet uncompressed unless `WriterOptions` are passed, so the harness passes ZSTD. The "after" Parquet files are 63 % smaller than the C++ ones because the native writer uses DELTA_BINARY_PACKED for the integer columns; C++ reads them with the same answers.

## Update: value types and ORC compression (`cd4e15e`)

Added after the runs above: `list<T>` for every scalar type, `date` and `timestamp` values in the row builders, ZSTD ORC chunks.

**ORC size.** The cause was compression alone: orc-rust compresses only when asked (`with_compression`), and the crate did not ask. The C++ files were ZSTD (the postscript of a chunk says compression kind 5), so the two sizes were not comparable. Stripe and encoding settings were not the cause: the 16 MB against 461 MB is compression, because orc-rust's direct string encoding with ZSTD ends up as small as the C++ one. Both remaining limits are in orc-rust 0.9: no dictionary encoding for strings, and no list encoder, so writing `list<T>` to ORC is an error here (`create_encoder` would panic, the crate refuses first). Reading C++-written ORC lists works.

Second host (see Method), with a newer dump of the same kind of graph: 3,124,200 vertices and 4,172,961 edges. Median of 5, load 28 to 60, native and C++ on the same files:

| operation | Parquet native | Parquet C++ | ORC native | ORC C++ |
|---|---:|---:|---:|---:|
| files on disk, vertices + edges | 14.1 + 6.7 MB | 33.1 + 22.4 MB | 13.9 + 12.2 MB | 14.8 + 13.5 MB |
| write vertices, table API | 821 ms | 1.35 s | 578 ms | 1.08 s |
| write edges, table API | 542 ms | 1.57 s | 602 ms | 1.61 s |
| write vertices, row builder | 4.36 s | 9.61 s | 4.54 s | 9.18 s |
| write edges, row builder | 2.22 s | 7.30 s | 2.21 s | 6.97 s |
| read a property group | 154 ms | 308 ms | 85 ms | 493 ms |
| adjacency list to CSR | 134 ms | 194 ms | 222 ms | 204 ms |
| full scan | 392 ms | 871 ms | 658 ms | 1.29 s |

C++ is still a little ahead on ORC adjacency to CSR (within the noise of this host). Every reader and writer agreed on all of these, and the C++ table writer did not fail in the 10 timed runs here.

**Value types.** `bench/types.sh` writes a generated graph (a column of each type, about one value in seven null, lists of 0 to 3 items, `date`, `timestamp` and `list<double>` edge properties) and `ldbc_sample_with_feature`, then scans with both readers. The sums cover values, not only counts: integers, days and milliseconds by value, floats by their bits, strings by length, lists by length and items.

| dataset | format | native table | native row builder | C++ table |
|---|---|---|---|---|
| all scalars | Parquet, ORC, CSV | same sums | same sums | same sums |
| all scalars | JSON | same sums | same sums | C++ cannot write JSON |
| all scalars and `list<T>` | Parquet | same sums | same sums | same sums |
| all scalars and `list<T>` | JSON | same sums | same sums | C++ cannot write JSON |
| all scalars and `list<T>` | ORC | native write refused | native write refused | same sums |
| `ldbc_sample_with_feature` | Parquet | same sums | same sums | same sums |
| `ldbc_sample_with_feature` | JSON | same sums | same sums | C++ cannot write JSON |
| `ldbc_sample_with_feature` | ORC | native write refused | native write refused | same sums |

Each "same sums" cell was read by both readers. The C++ row builders were not part of this check. C++ names five list types (`list<int32>`, `int64`, `float`, `double`, `string`); this crate also accepts lists of `bool`, `date` and `timestamp`, which C++ will not load.

## Other formats (code graph)

CSV:

| operation | native before | native after | C++ (run 1) | C++ (run 2) | graphar-rs |
|---|---:|---:|---:|---:|---:|
| write vertices, table API | 1.43 s | 1.39 s | 726 ms | 658 ms | - |
| write edges, table API | 1.29 s | 1.02 s | 1.29 s | 1.17 s | - |
| write vertices, row builder | 8.60 s | 4.28 s | 7.69 s | 7.35 s | 7.63 s |
| write edges, row builder | 6.63 s | 2.96 s | 7.55 s | 7.16 s | - |
| read a property group | 363 ms | 84 ms | 514 ms | 514 ms | - |
| adjacency list to CSR | 440 ms | 456 ms | 506 ms | 522 ms | - |
| full scan | 1.87 s | 1.06 s | - | - | - |

ORC:

| operation | native before | native after | C++ (run 1) | C++ (run 2) | graphar-rs |
|---|---:|---:|---:|---:|---:|
| write vertices, table API | 321 ms | 307 ms | 1.05 s | 1.05 s | - |
| write edges, table API | 621 ms | 401 ms | 1.30 s | 1.35 s | - |
| write vertices, row builder | 6.43 s | 3.23 s | 8.02 s | 7.69 s | 8.69 s |
| write edges, row builder | 5.11 s | 2.28 s | 7.66 s | 7.34 s | - |
| read a property group | 192 ms | 194 ms | 406 ms | 402 ms | - |
| adjacency list to CSR | 186 ms | 181 ms | 145 ms | 146 ms | - |
| full scan | 963 ms | 901 ms | 1.09 s | 1.08 s | - |

The C++ CSV full scan is `-` because it fails, see finding 5.

## Upstream sample data

`ldbc_sample` from the `testing` submodule, 903 vertices and 6,626 edges. Rewritten by each writer (Parquet):

| operation | native before | native after | C++ (run 1) | C++ (run 2) | graphar-rs |
|---|---:|---:|---:|---:|---:|
| write vertices, table API | 2.3 ms | 2.5 ms | 4.4 ms | 5.2 ms | - |
| write edges, table API | 5.0 ms | 4.2 ms | 25.3 ms | 13.7 ms | - |
| write vertices, row builder | 4.1 ms | 2.9 ms | 6.7 ms | 5.6 ms | 4.7 ms |
| write edges, row builder | 10.4 ms | 6.2 ms | 16.5 ms | 12.1 ms | - |
| read a property group | 1.1 ms | 0.9 ms | 5.7 ms | 4.7 ms | - |
| adjacency list to CSR | 1.2 ms | 1.3 ms | 14.2 ms | 11.3 ms | - |
| full scan | 3.5 ms | 5.7 ms | 21.2 ms | 14.7 ms | - |

The files as they are in `testing` (written by Spark), read by both:

| format | operation | native | C++ |
|---|---|---:|---:|
| parquet | read-prop | 0.7 ms | 4.6 ms |
| parquet | read-csr | 1.5 ms | 8.9 ms |
| parquet | scan | 3.5 ms | 13.1 ms |
| csv | read-prop | 3.8 ms | 11.6 ms |
| csv | read-csr | 5.2 ms | 18.8 ms |
| csv | scan | 9.1 ms | 43.5 ms |
| orc | read-prop | 0.7 ms | 9.0 ms |
| orc | read-csr | 2.1 ms | 9.8 ms |
| orc | scan | 4.1 ms | 16.9 ms |

These sizes are too small to say anything about throughput: the C++ numbers are mostly fixed setup cost. They are here so the run can be repeated with public data.

## Interoperability

Every reader read the graphs of every table writer, and the Spark-written sample. A read reports counts and sums (rows, CSR offsets and indices, column sums).

| files | Parquet | ORC | CSV |
|---|---|---|---|
| native writer, C++ reader | same answers | same answers | same on `read-prop` and `read-csr`; `scan`: see below |
| C++ writer, native reader | same answers | same answers | same |
| Spark sample, both readers | same answers | same answers | same on `read-prop` and `read-csr`; `scan`: see below |

CSV `scan` differs only in the C++ reader, on files of every writer: on `ldbc_sample` the string sum is 2 bytes per edge longer, on the code graph it fails (findings 5 and 6). The native reader gives the same sums on CSV as on Parquet. Info files written by the native crate load in C++.

## Conformance (`bench/rust`: `graphar-bench conform testing`)

Every `*.graph.yml` in `testing` (commit `8286724`): all vertex types merged, all adjacency lists to CSR, all edge property groups. Rerun at `cd4e15e`.

| graph | parts read | note |
|---|---:|---|
| ldbc_sample/parquet (plain, date, timestamp, with_feature) | 4/4 each | `list<float>` in with_feature |
| ldbc_sample/csv, orc, json | 3/3 each | |
| modern_graph | 4/4 | |
| neo4j/MovieGraph | 14/14 | int32 offsets |
| nebula/basketballplayergraph | 6/6 | int32 offsets |
| ldbc/parquet | 2/7 | the sample holds no `place` chunks |
| ldbc, transformer | 0/54, 0/3 | info files only, no chunks in `testing` |
| java/ldbc_sample (3 formats) | 0/2 each | another info schema (`label`, not `type`), not `gar/v1` |

What the crate does not support yet:

| gap | state |
|---|---|
| Writing `list<T>` to ORC | orc-rust 0.9 has no list encoder; an error here. Reading works |
| `list<T>` in CSV | no list form in CSV, in either library |
| ORC string dictionary | orc-rust 0.9 writes strings directly |
| Labels (`labels`, `:LABEL`) | not modelled, ignored on load |
| `cardinality`, `extra_info` | not modelled, ignored on load |
| `time` | not a GraphAr C++ type; the row builders return an error |
| Filter pushdown, column projection inside a group | not implemented; row windows exist for point lookups |
| High-level collections and iterators | not implemented; readers return Arrow batches |
| Info files on object stores | local only; chunks work through `ChunkStore` |
| Object store path of the readers | `VertexReader` and `EdgeReader` are local; `ChunkStore` reads and writes chunks |
| Object store timing | not measured |

## Findings in the C++ library

1. `AdjListOffsetArrowChunkReader::GetChunk()` returns `->column(0)->chunk(0)`, the first Arrow chunk only (`chunk_reader.cc:826`). At the default vertex chunk size the offsets of a Parquet chunk come back truncated: n was 1,717,177 instead of 3,421,126. The harness seeks forward to get them all.
2. `VertexPropertyArrowChunkReader::GetChunk()` with `AUTO` takes the Parquet-only V2 path when there is no filter, so CSV and ORC fail with "Parquet magic bytes not found". The harness passes `V1`.
3. Without `WriterOptions` the writers write Parquet uncompressed, although `ParquetOption::compression` defaults to ZSTD.
4. The table write path failed with "Cancelled: Plan was cancelled early" in 7 of 60 timed runs, on both datasets and several formats (lines 245 and 247 of the harness: `SortAndWriteAdjListTable`, `SortAndWritePropertyTable`). The cause is not known; that host was loaded. It did not happen in the 10 timed runs on the second host. Medians use the successful runs.
5. CSV chunks are read with inferred types, not the declared ones: `start_line` (null for the first rows of a chunk) fails with "CSV conversion error to null".
6. Same cause, probably: on `ldbc_sample` a `string` `creationDate` that looks like a timestamp comes back 2 bytes longer. Arrow infers a timestamp; we did not inspect the value after GraphAr's cast.
7. The table writer pads the last offset chunk to `chunk_size + 1` rows; Spark writes `vertex_count + 1`. The native reader accepts both.
8. The YAML reader rejects `prefix: ''` and block sequences that are not indented. The native crate now writes the upstream style.
9. GraphAr C++ at `06dddb1` does not build against Arrow 22 (`ReadTable` API) and did not link against the Arrow 26.0.1 apt packages (undefined `arrow::fs::InitializeS3`). We used Arrow 24.0.0.

## Extension formats

The crate carries only the GraphAr spec's formats. Arrow IPC, which earlier runs here also measured, now lives in a crate of our own behind the `PayloadFormat` trait; it is not part of the donation and could be offered upstream later as a separate proposal. `raw-summary.md` still has the Arrow IPC rows of those earlier runs.

## Method

| item | value |
|---|---|
| Host | AMD Threadripper PRO 3975WX, 32 cores, 499 GB RAM, Linux 7.0, XFS on RAID 0 of two Samsung PM1733 NVMe |
| Second host | Cloud VM, 24 vCPUs of an AMD EPYC 7543, 94 GB RAM, Linux 6.8, ext4 on a virtual disk (the update section only) |
| Load | Shared host, other jobs running: load average 20 to 74 during the runs. Treat differences under about 20 % as noise |
| Container | Ubuntu 24.04, g++ 13.3, Rust 1.91.1 |
| Native | graph-ar `cd86afc` and `d85f31f`; arrow-rs and parquet 59.3.2, orc-rust 0.9.0, object_store 0.14.2 |
| C++ | apache/incubator-graphar `06dddb1`, Release, Arrow C++ 24.0.0 |
| graphar-rs | 0.1.0, same commit |
| Runs | One process per run, 5 runs, median. `secs` is timed inside the process around the operation; loading the writers' input is not counted |
| Peak RSS | `/usr/bin/time -f %M`, whole process; for writers it includes the input tables |
| Cache | Warm page cache (files just written); no cache drop, so this is decode cost, not disk |
| Chunk sizes | code graph: vertex 2^18, edge 2^22 (the defaults); `ldbc_sample`: 100 and 1024 |
| Graph | Vertex groups `[id]`, `[kind, name, crate]`, `[start_line]`; edge group `[kind, count]`; `ordered_by_source` |
| Table API, C++ | `VertexPropertyWriter::WriteTable`; edges are partitioned per vertex chunk in the harness (timed), then `SortAndWriteAdjListTable` and `SortAndWritePropertyTable` |
| Row API | `builder::VerticesBuilder` / `EdgesBuilder` against `VerticesBuilder` / `EdgesBuilder`, Arrow to row conversion timed on both sides |
| Sort order | C++ sorts by the aligned endpoint only, native by both endpoints; the sums do not depend on the order inside a row |

The large graph is not public; `ldbc_sample` is what others can repeat at once. Any graph can be used through `prep-gar`.

## Reproduce

See `bench/README.md`. In short:

```shell
git clone --recurse-submodules ssh://anonymous@git.gunnar.rs:2222/nordisk/graph-ar
cd graph-ar/bench
podman build -t graphar-bench -f Containerfile .
# inside the container: build GraphAr C++, gar_bench_cpp, graphar-bench, graphar-rs-bench (README)
LD_LIBRARY_PATH=$PWD/install/lib UPSTREAM_TESTING=$UP/testing RUNS=5 ./run.sh $PWD/work after
python3 summarize.py work/raw/after.jsonl
```

Medians of every case: [raw-summary.md](raw-summary.md). The one-JSON-line-per-run files stay outside the repository.

CI: `.github/workflows/rust.yml` has not run on a hosted runner; there is none for this repository, so its steps were run by hand in containers.
