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

# Bench: native Rust vs GraphAr C++ vs graphar-rs

Three harnesses take the same commands and print the same JSON lines:

| dir | implementation | what it covers |
|---|---|---|
| `rust/` | this crate (`graphar-bench`) | prepare inputs, info files, write (table and row-builder API), read a property, read CSR, full scan |
| `cpp/` | GraphAr C++ library (`gar_bench_cpp`) | write (`VertexPropertyWriter`/`EdgeChunkWriter` and `builder::*`), the same reads with the chunk readers |
| `graphar-rs/` | upstream `graphar-rs` 0.1.0 (`graphar-rs-bench`) | vertex row builder only (it has no edge builder and no readers yet) |

Every reader runs on the graphs written by every table writer, and on the
upstream `testing/ldbc_sample` graph (written by the Spark library), so the
run doubles as an interoperability check: the `check` object of a read
(row counts, CSR sums, column sums) must be the same for every reader and
writer.

## Value types

`types.sh <work-dir> <testing-dir>` checks every value type between the
implementations: a generated graph with a column of each type, some of them
null, and the upstream `ldbc_sample_with_feature` graph. Each writer writes,
each reader scans, and the sums must match.

## Inputs

An input dir holds `vertices.parquet`, `edges.parquet` (with
`_graphArSrcIndex`, `_graphArDstIndex` and the edge properties) and
`schema.json`.

```shell
graphar-bench prep-gar data/ldbc-input testing/ldbc_sample/parquet/ldbc_sample.graph.yml person person knows person
graphar-bench prep-mimir data/mimir-input <dump-dir>...     # rustc graph dumps (Arrow IPC files)
for f in parquet csv orc arrow; do graphar-bench info data/ldbc-input $f data/ldbc-info-$f; done
```

## Build

All builds run in the container of `Containerfile` (Ubuntu 24.04, Arrow C++
24.0.0 from the Apache Arrow apt repository, Rust 1.91.1).

```shell
podman build -t graphar-bench -f Containerfile .
# GraphAr C++ (apache/incubator-graphar checkout at $UP)
cmake -S $UP/cpp -B build/cpp -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$PWD/install
cmake --build build/cpp && cmake --install build/cpp
cmake -S cpp -B build/bench-cpp -G Ninja -DCMAKE_PREFIX_PATH=$PWD/install && cmake --build build/bench-cpp
# this crate
(cd rust && cargo build --release --features orc)
# graphar-rs: upstream is a symlink to the checkout
ln -s $UP graphar-rs/upstream && (cd graphar-rs && GRAPHAR_BUILD_TYPE=Release cargo build --release)
```

## Run

```shell
mkdir -p work/bin && cp <the three binaries> work/bin/ && cp -r data work/
LD_LIBRARY_PATH=$PWD/install/lib UPSTREAM_TESTING=$UP/testing RUNS=5 ./run.sh $PWD/work <label>
python3 summarize.py work/raw/<label>.jsonl
```

`run.sh` writes one JSON line per run with the peak RSS of the process
(`/usr/bin/time -f %M`); `secs` is the time of the operation itself, measured
inside the process (input loading for the writers is not counted).
