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

# graphar performance notes

How the hot paths work, what they cost, and what was measured. Every
number here comes from `benches/graphar_bench.rs` driven by `benches/run.sh`
(one process per stage, so peak RSS and bytes read are per stage), on
**oden** (AMD Threadripper PRO 3975WX, 32 cores, 499 GB RAM, Linux 7.0),
**2026-11-04**, `--release`, Rust 1.97.1 in the `nordisk-cargo-toolchain:ci4`
container. The host carried a load of 25-45 from other jobs during the
runs, so parallel figures are conservative; single-thread figures repeated
within a few percent. The page cache was warm (`read_bytes` 0): the numbers
are decode and memory cost, not disk.

Datasets: a synthetic power-law graph written by the crate's own writers
(`gen` stage; `dst = floor(n · u³)`, dense ids `0..n`, one string property
per edge), at **10M edges / 1M vertices** and **100M edges / 10M vertices**,
GraphAr default chunk sizes (vertex 2¹⁸ = 262 144, edge 2²² = 4 194 304),
Parquet. "Before" is main `12485bf` (ZSTD default, columnar
`write_edge_table`), "after" is this branch (`aad4ced`).

Reproduce: `benches/run.sh out.md --vertices 1000000 --edges 10000000 --dir /some/dir`
(`STAGES="..."` picks stages, `GRAPHAR_BENCH_THREADS=n` caps decode threads).

## Results

### 10M edges, 1M vertices

| stage | before | after | what changed |
|---|---|---|---|
| write-vertices (row API) | 869 ms, 441 MiB | 472 ms, 399 MiB | Vec properties |
| write-vertices-table (columnar) | 272 ms, 50 MiB | 161 ms, 49 MiB | delta encoding (less to compress) |
| write-edges (row API) | 10 357 ms, 7 545 MiB | 4 615 ms, 3 977 MiB | Vec properties, no pseudo-vertex clones, counting sort |
| write-edges-table (columnar) | 2 655 ms, 753 MiB | 2 429 ms, 680 MiB | counting sort, delta encoding |
| chunk files on disk | 49 MB | 28 MB | delta-packed integer columns |
| read-vertices (`read_merged`) | 99 ms | 58 ms serial, 36 ms (8 threads) | delta decode, parallel chunks |
| read-prop (`read_by_property`) | 45 ms | 12 ms | delta decode |
| read-adj (full scan, streaming) | 202 ms | 65 ms | delta decode |
| read-adj-keep (keeps all batches) | 245 ms | 89 ms serial, 62 ms parallel (`read_all_adj_lists`) | delta decode, parallel chunks |
| read-offsets | 14.6 ms | 5.8 ms | delta decode |
| neighbours of v, plain (whole chunk) | 59 ms/query, 12.7 MB read | 24 ms/query, 8.7 MB | smaller files, same API |
| neighbours of v, `neighbors()` | - | 26 µs/query, 52 KB read | pushdown + caches |
| validate | 0.3 ms | 0.3 ms | - |
| vertex id → chunk/slot | 9.9 ns | 8.6 ns | division, untouched |
| edge row → owning vertex (`partition_point`) | 68 ns | 66 ns | untouched (not on a hot path) |

The row-API floor (build 10M `Edge`s with one string property and drop
them, no writing at all) went from 3 326 ms (HashMap per edge) to 2 597 ms
(Vec per edge); the writer's own work above the floor is now ~2.0 s, the
same as the columnar `write_edge_table` on the same data.

### 100M edges, 10M vertices

| stage | before | after |
|---|---|---|
| write-vertices (row API) | 8 390 ms, 4.1 GB | 100M_WV |
| write-edges (row API) | 95 242 ms, 75.4 GB | 100M_WE |
| write-edges-table (columnar) | 32 025 ms, 7.3 GB | 100M_WT |
| chunk files on disk | 833 MB | 100M_SIZE |
| read-vertices | 866 ms | 100M_RVS serial, 100M_RV (8 threads) |
| read-prop | 422 ms | 100M_RP |
| read-adj (streaming) | 1 913 ms | 100M_RA |
| read-adj-keep | - | 100M_RAK serial, 100M_RAP parallel |
| read-offsets | 139 ms | 100M_RO |
| neighbours, plain | 63 ms/query | 100M_NB |
| neighbours, `neighbors()` | - | 100M_NP |

### Lookup path, step by step (10M graph)

| variant | per query | bytes read per query |
|---|---|---|
| plain: whole offset chunk + whole adjacency chunk (before) | 59 ms | 12.7 MB |
| row window + projection (`read_rows`), dictionary-encoded files | 1.5 ms | 2.9 MB |
| same, delta-packed files | 70 µs | 73 KB |
| + cached footers and offset arrays (`neighbors()`) | 26 µs | 52 KB |

(The two middle rows were measured on the earlier, uncompressed main; the
mechanism is the same with ZSTD.)

## Where the time went (perf, row-API write-edges, earlier main)

```
75 %  per-edge HashMap<String, PropertyValue>: malloc, rehash, clone into
      pseudo-vertices, free (glibc _int_free_* consolidating 30M chunks)
 6 %  sort over the edge structs
 4 %  column builders
<2 %  parquet encoding
```

After replacing the HashMap with a Vec the profile was still 40 % `free`,
and 15 % of it came from the final `clear()`: the sort had permuted the
structs, so their strings were freed in random address order and glibc
spent its time merging chunks. Today's writer never moves the edges
(the columns are gathered through a permutation), so that cost is gone
on both the row and the columnar path.

## Algorithms

### 1. Writer: counting-sort permutation (`writer/edge_writer.rs`, `sort_edges`)

Edges must be grouped by their alignment key `k(e)` (`src` or `dst`), and
chunk `c` holds the edges with `k ∈ [c·S, (c+1)·S)`, `S` = vertex chunk
size. Keys are bounded: `0 ≤ k < V`. Ordered lists also sort each vertex's
edges by the other endpoint.

Formula (stable counting sort into a permutation):

```
deg[v]     = |{ e : k(e) = v }|                 pass 1 over E
starts[v]  = Σ_{u<v} deg[u]                     exclusive prefix sum, starts[V] = |kept|
perm[starts[k(e)] + rank(e)] = row(e)           pass 2 over E, rank = insertion order
ordered:   sort perm[starts[v] .. starts[v+1]] by other(e)   per vertex
chunk c    = perm[ starts[c·S] .. starts[min((c+1)·S, V)] ]
offsets_c[i] = starts[c·S + i] − starts[c·S]    i = 0 ..= chunk rows
```

Complexity: O(E + V) for the bucket passes plus O(Σ_v d_v log d_v) for the
per-vertex runs (≪ E log E; most runs are short), versus O(E log E) for
sorting `(key, other, row)` tuples. The columns are gathered once with
`take(perm)`; the offset chunk is a slice of `starts` rebased to 0 (the
histogram the old code recomputed per chunk).

Dataflow:

```
edge table (src, dst, properties), never moved
   │ pass 1: deg[k(e)]++                 │ pass 2: perm[next[k(e)]++] = row
   ▼                                     ▼
deg ──prefix sum──▶ starts ───────────▶ perm (row permutation)
                      │                   │ per-vertex runs sorted by other
                      │ slice per chunk   │ take(perm) → sorted columns → slice per chunk
                      ▼                   ▼
                offset chunk        adj_list chunk, property chunks
```

Worked example: `V = 4`, `S = 2`, rows in insertion order
`(2→9), (0→5), (2→7), (1→3), (7→1)` with `k = src`, ordered by source.

```
deg    = [1, 1, 2, 0]          (src 7 is out of range, dropped)
starts = [0, 1, 2, 4, 4]
perm   = [1, 3, 0, 2]          vertex 0: row 1; vertex 1: row 3; vertex 2: rows 0, 2
per-vertex sort by dst: vertex 2's run [0, 2] has dst 9, 7 → [2, 0]
perm   = [1, 3, 2, 0]
chunk 0 = perm[starts[0]..starts[2]] = [1, 3]  → rows (0→5), (1→3), offsets [0, 1, 2]
chunk 1 = perm[starts[2]..starts[4]] = [2, 0]  → rows (2→7), (2→9), offsets [0, 2, 2]
```

The property test `counting_sort_matches_plain_sort` pins `sort_edges` to
`(key, other, row).sort_unstable()` and `starts` to the histogram offsets.
The written chunks are identical to the old writer's (the `checksum` stage
fingerprints agree: offsets exact, adjacency multiset equal).

### 2. Encoding: delta-packed integer columns (`io/parquet.rs`, `writer_properties`)

Parquet's default is dictionary encoding with a 1 MiB dictionary-page
limit. An id column chunk of a million distinct values fills that page
and then falls back to plain, and *every* read of *any* page of the column
chunk must first read (and, with ZSTD, decompress) the dictionary page.
For a point lookup that is a ~1 MiB preamble per column per file.

`DELTA_BINARY_PACKED` stores blocks of `(first value, min delta, bit-packed
deltas)`. For a sorted column with small deltas (`_graphArSrcIndex`,
`_graphArOffset`) the cost per row is `⌈log₂(max delta − min delta + 1)⌉`
bits; `src` in an ordered adjacency list has deltas of 0 or 1 (~1 bit/row),
`offset` (a monotone prefix sum) a few bits. Random `dst` stays at
~log₂(V) bits. ZSTD then has less to compress, and the decoder has less to
inflate: files 49 MB → 28 MB at 10M edges, scans 2.5-3.7x faster. Still
plain Parquet: every Parquet reader (Arrow C++, Spark, DuckDB) reads it.

Page size trade-off (measured on the earlier, uncompressed main;
`data_page_row_count_limit`):

| rows per page | read-adj scan | neighbours lookup | bytes per lookup |
|---|---|---|---|
| 20 000 (Parquet default, kept) | 58 ms | 27 µs | 52 KB |
| 4 096 | 75 ms | 13 µs | 10.5 KB |
| 1 024 | 101 ms | 20 µs | 3 KB |

Smaller pages halve the lookup but cost 30 % on scans and make the page
index itself the cost at 1 024; GraphAr is scan-first, so the default stays.

### 3. Lookup: row-window pushdown (`io::read_chunk_rows`, `EdgeReader::edge_range / neighbors`)

Edges of vertex `v` in an ordered adjacency list, with vertex chunk size
`S` and edge chunk size `C`:

```
vc    = ⌊v / S⌋                              vertex chunk (a division, no search)
slot  = v mod S
[b, e) = offsets_vc[slot] .. offsets_vc[slot + 1]   rows within the vertex chunk
row r in [b, e) lives in edge chunk ⌊r / C⌋ at row r mod C
```

`read_chunk_rows(path, offset, limit, columns)` hands `offset`/`limit`
and a column projection to the Parquet reader, which uses the row-group
row counts and the offset index (page locations, written by default) to
skip to the page(s) holding `[offset, offset + limit)` and decodes only
the projected column. Cost per lookup: open + footer + one page of the one
column, O(pages touched) instead of O(rows in the chunk).

Dataflow:

```
v ──÷S──▶ vc ──▶ offsets cache (Arc<Vec<i64>>, decoded once per vertex chunk)
   mod S ─▶ slot ─▶ [b, e)
                     │  split at edge-chunk borders (÷C, mod C)
                     ▼
         footer cache ──▶ parquet reader(offset, limit, projection = {_graphArDstIndex})
                                 │  row groups skipped by row count,
                                 │  pages skipped by offset index
                                 ▼
                         one data page → Int64Array → Vec<i64>
```

Worked example: `S = 262 144`, `C = 4 194 304`, `v = 300 000`:
`vc = 1`, `slot = 37 856`; offsets of chunk 1 say `[b, e) = [401 230, 401 241)`;
all rows are in edge chunk 0 (`401 230 / C = 0`), rows `401 230 .. 401 241`
of `adj_list/part1/chunk0`; that file's first row group holds rows
`0 .. 1 048 576`, page 20 of the `_graphArDstIndex` column holds rows
`400 000 .. 420 000`, so the reader reads that page (≈50 KB) and returns
11 values. The property test `neighbors_pushdown_matches_plain_read` pins
this to the full read on random graphs with tiny chunks (ranges crossing
edge-chunk borders).

### 4. Caches (`io::parquet::MetadataCache`, `EdgeReader::offsets_of`)

A parsed footer (schema, row groups, column and offset indexes) is a few
KB per chunk file and immutable, so `EdgeReader` keeps the ones its
lookups touch in a `Mutex<HashMap<PathBuf, Arc<ArrowReaderMetadata>>>`.
The decoded offset array of a vertex chunk is `S + 1` i64s (2 MiB at the
default `S`), kept per `(adjacency dir, vertex chunk)` on first use; after
that `edge_range` is two array reads. Worst case for a graph of `V`
vertices: `8·V` bytes of offsets plus footers. 70 µs → 26 µs per lookup.

### 5. Parallel chunk decode (`reader::par_map`)

Chunk files are independent, so multi-chunk reads fan out over scoped
`std::thread`s: thread `t` of `T` decodes chunk indices `t, t+T, t+2T, …`,
results land in index order. No pool, no rayon, nothing shared but the
inputs. `T = min(available cores, 8, chunks)`; the readers' `with_threads(n)`
overrides it (`1` = serial).

Measured at 100M edges on the earlier main (load 30-45 on the host):

| threads | read-vertices (`read_merged`) | read-adj (keep all batches) |
|---|---|---|
| serial | 350 ms | 765 ms |
| 2 | 240 ms | 635 ms |
| 4 | 209 ms | 554 ms |
| 8 | 198 ms | 728 ms |
| 16 | 207 ms | 637 ms |

Gains flatten at 4-8 threads: decode is memory-bound (each chunk lands in
fresh buffers; `kernel_init_pages` and page faults dominate the profile),
and the host was shared. The streaming `iter_vertex_chunks` (one chunk in
memory at a time) stays the cheapest full scan and keeps ~50 MiB RSS; use
`read_all_adj_lists` when every chunk is wanted in memory anyway. The
property test `parallel_reads_match_serial` pins parallel to serial.

### 6. Tried and not kept

- **mmap** (`read-adj-mmap`): chunk files mapped and handed to the
  in-memory decoder. 100M scan on the uncompressed main: `File` 483 ms,
  whole-file `read` 473 ms, mmap 453 ms; with ZSTD files the `File` path is
  the fastest of the three at 10M (65 / 89 / 75 ms). Not worth an `unsafe`
  and a dependency in the library; the bench stages stay as the record.
- **Smaller data pages**: see the table under 2.

## The static search tree (Ragnar Groot Koerkamp's S+ tree)

Straight answer: **it does not speed up GraphAr's own paths, because
they contain no sorted-array search.** Vertex id → chunk is a division
(`8.6 ns`), edges of a vertex is an array index into the offset array,
and the chunk → file mapping is arithmetic. The only sorted searches one
could write are (a) edge row → owning vertex, a `partition_point` over
the offsets (66 ns at 262k entries) that no reader path needs, and (b)
exact lookup of an *external* key in a sorted key column (LDBC ids → row),
which is an index a graph engine such as knut builds on top of GraphAr,
not part of the format.

For (b) the tree is worth it. `STree64` (znippy-zoomies, Ragnar's layout:
64-byte nodes of 8 i64 keys, branchless SIMD compare, O(log₉ n) node
visits) against `partition_point` and a branchless Eytzinger layout,
single exact lookups of present keys, 10M random queries, oden:

| keys | partition_point | Eytzinger | STree64 | STree64 build |
|---|---|---|---|---|
| 262 144 (one vertex chunk) | 64 ns | 43 ns | 34 ns | 0.9 ms |
| 1 048 576 | 130 ns | 95 ns | 74 ns | 2.0 ms |
| 16 777 216 | 687 ns | 468 ns | 183 ns | 38 ms |

1.8x at cache-resident sizes, 3.8x once the array is RAM-bound (where
binary search pays a cache miss per level and the tree pays one per
node); the batched, software-pipelined lookups skade uses on it go
further, and that is where a vertex-key index would plug in. The
`stree` bench stage reproduces the table.
