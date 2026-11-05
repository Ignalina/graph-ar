// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! graphar perf bench — plain harness, one stage per process so the peak RSS
//! (`VmHWM`) and the byte counters (`/proc/self/io`) belong to that stage
//! alone. Drive it with `benches/run.sh`; see `docs/perf.md` for the tables.
//!
//! Dataset: a synthetic power-law graph (`gen` stage) written through the
//! crate's own writers, so write and read are both measured on the same
//! on-disk layout the library produces. Vertex ids are the dense GraphAr
//! internal ids `0..n`; destinations follow `dst = floor(n * u^3)` (a heavy
//! head, like a social graph's popular accounts).
//!
//! Stages (each prints one `ROW` line: stage, wall ms, peak RSS MiB, rchar
//! MiB, read_bytes MiB, plus a stage-specific count):
//!   gen                  write vertices + edges (row builders) and the infos
//!   write-vertices       VerticesBuilder (row API)
//!   write-vertices-table write_vertex_table (columnar API)
//!   write-edges          EdgesBuilder (row API)
//!   write-edges-table    write_edge_table (columnar API)
//!   write-edges-floor    build the row-API edges and drop them, no writing
//!   read-vertices        read_merged over every vertex chunk
//!   read-prop            read_by_property("name"), one group only
//!   read-adj             streaming full scan of the ordered_by_source list
//!   read-adj-keep        same, keeping every batch (apples to apples with -par)
//!   read-adj-bytes       same, each file read whole and decoded from memory
//!   read-offsets         every offset chunk
//!   neighbors            K random "edges of vertex v" through the plain reads
//!   lookups              in-memory index arithmetic + offset reverse lookups
//!   checksum             order-independent fingerprint of the written edges
//!   validate             validate_graph_dir over the written graph
//!   ldbc                 read every table of an upstream sample (--dir)
//! With the perf branch's API (see docs/perf.md):
//!   neighbors-pushdown   EdgeReader::neighbors (row-window reads + caches)
//!   read-vertices-serial read_merged with one decode thread
//!   read-adj-par         read_all_adj_lists (parallel decode, keeps batches)
//!   read-adj-mmap        full scan from memory-mapped chunk files
//!   stree                sorted-key lookups: partition_point vs Eytzinger vs STree64

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use graphar::{
    AdjListInfo, AdjListType, DST_INDEX_COL, DataType, Edge, EdgeInfo, EdgeReader, EdgesBuilder,
    FileType, GraphInfo, OFFSET_COL, Property, PropertyGroup, PropertyValue, SRC_INDEX_COL, Vertex,
    VertexInfo, VertexReader, VerticesBuilder, validate_graph_dir, write_edge_table,
    write_vertex_table,
};

const VERTEX_CHUNK: u64 = 262_144; // 2^18, the GraphAr default
const EDGE_CHUNK: u64 = 4_194_304; // 2^22, the GraphAr default

struct Args {
    stage: String,
    dir: PathBuf,
    vertices: u64,
    edges: u64,
    queries: u64,
    seed: u64,
}

fn parse_args() -> Args {
    let mut a = Args {
        stage: "all".into(),
        dir: PathBuf::from("/tmp/graphar-bench"),
        vertices: 1_000_000,
        edges: 10_000_000,
        queries: 1000,
        seed: 42,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--stage" => {
                a.stage = argv[i + 1].clone();
                i += 1;
            }
            "--dir" => {
                a.dir = PathBuf::from(&argv[i + 1]);
                i += 1;
            }
            "--vertices" => {
                a.vertices = argv[i + 1].parse().unwrap();
                i += 1;
            }
            "--edges" => {
                a.edges = argv[i + 1].parse().unwrap();
                i += 1;
            }
            "--queries" => {
                a.queries = argv[i + 1].parse().unwrap();
                i += 1;
            }
            "--seed" => {
                a.seed = argv[i + 1].parse().unwrap();
                i += 1;
            }
            "--bench" | "--nocapture" => {}
            other => panic!("unknown arg {other}"),
        }
        i += 1;
    }
    a
}

// ── process counters ────────────────────────────────────────────────────────

fn proc_field(file: &str, key: &str) -> u64 {
    let s = std::fs::read_to_string(file).unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn peak_rss_mib() -> f64 {
    proc_field("/proc/self/status", "VmHWM:") as f64 / 1024.0
}

fn io_counters() -> (u64, u64) {
    (
        proc_field("/proc/self/io", "rchar:"),
        proc_field("/proc/self/io", "read_bytes:"),
    )
}

struct Stage {
    name: String,
    t0: Instant,
    io0: (u64, u64),
}

impl Stage {
    fn start(name: &str) -> Self {
        Self {
            name: name.to_string(),
            t0: Instant::now(),
            io0: io_counters(),
        }
    }

    fn end(self, count: u64, unit: &str) {
        let ms = self.t0.elapsed().as_secs_f64() * 1e3;
        let io1 = io_counters();
        let rchar = (io1.0 - self.io0.0) as f64 / (1024.0 * 1024.0);
        let rbytes = (io1.1 - self.io0.1) as f64 / (1024.0 * 1024.0);
        println!(
            "ROW {}\t{:.1} ms\t{:.0} MiB rss\t{:.1} MiB rchar\t{:.1} MiB read_bytes\t{} {}",
            self.name,
            ms,
            peak_rss_mib(),
            rchar,
            rbytes,
            count,
            unit
        );
    }
}

// ── synthetic graph ─────────────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn person_info() -> VertexInfo {
    VertexInfo::new("person", VERTEX_CHUNK)
        .with_property_group(PropertyGroup::new(
            vec![Property::new("id", DataType::Int64).primary()],
            FileType::Parquet,
        ))
        .with_property_group(PropertyGroup::new(
            vec![
                Property::new("name", DataType::String),
                Property::new("age", DataType::Int32),
            ],
            FileType::Parquet,
        ))
}

fn knows_info() -> EdgeInfo {
    EdgeInfo::new(
        "person",
        "knows",
        "person",
        EDGE_CHUNK,
        VERTEX_CHUNK,
        VERTEX_CHUNK,
        true,
    )
    .with_adj_list(AdjListInfo {
        ordered: true,
        aligned_by: "src".into(),
        file_type: FileType::Parquet,
        prefix: None,
    })
    .with_property_group(PropertyGroup::new(
        vec![Property::new("creationDate", DataType::String)],
        FileType::Parquet,
    ))
}

fn gen_edges(a: &Args) -> Vec<(i64, i64)> {
    let mut rng = Rng(a.seed | 1);
    let n = a.vertices as f64;
    (0..a.edges)
        .map(|_| {
            let src = (rng.next() % a.vertices) as i64;
            let dst = (n * rng.unit().powi(3)) as i64;
            (src, dst.min(a.vertices as i64 - 1))
        })
        .collect()
}

fn write_vertices(a: &Args) -> u64 {
    let st = Stage::start("write-vertices");
    let mut b = VerticesBuilder::new(person_info(), &a.dir);
    for i in 0..a.vertices {
        b.add_vertex(
            Vertex::new()
                .add_property("id", PropertyValue::Int64(i as i64))
                .add_property("name", PropertyValue::String(format!("person{i}")))
                .add_property("age", PropertyValue::Int32((i % 90) as i32)),
        );
    }
    let chunks = b.dump().unwrap();
    st.end(a.vertices, "vertices");
    chunks
}

/// The columnar vertex writer on the same data.
fn write_vertices_table(a: &Args) {
    let st = Stage::start("write-vertices-table");
    let n = a.vertices as usize;
    let ids = Int64Array::from_iter_values(0..a.vertices as i64);
    let names = StringArray::from_iter_values((0..n).map(|i| format!("person{i}")));
    let ages = Int32Array::from_iter_values((0..n).map(|i| (i % 90) as i32));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("name", ArrowDataType::Utf8, true),
        Field::new("age", ArrowDataType::Int32, true),
    ]));
    let table =
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names), Arc::new(ages)]).unwrap();
    let written = write_vertex_table(&person_info(), &a.dir, &table).unwrap();
    st.end(written, "vertices");
}

fn write_edges(a: &Args) {
    let st = Stage::start("write-edges");
    let mut b = EdgesBuilder::new(
        knows_info(),
        &a.dir,
        AdjListType::OrderedBySource,
        a.vertices,
    );
    for (src, dst) in gen_edges(a) {
        b.add_edge(
            Edge::new(src, dst)
                .add_property("creationDate", PropertyValue::String("2026-11-04".into())),
        );
    }
    b.dump().unwrap();
    st.end(a.edges, "edges");
}

/// The columnar edge writer on the same data: one table of
/// `_graphArSrcIndex`, `_graphArDstIndex`, `creationDate`.
fn write_edges_table(a: &Args) {
    let st = Stage::start("write-edges-table");
    let edges = gen_edges(a);
    let src = Int64Array::from_iter_values(edges.iter().map(|e| e.0));
    let dst = Int64Array::from_iter_values(edges.iter().map(|e| e.1));
    let date = StringArray::from_iter_values(edges.iter().map(|_| "2026-11-04"));
    drop(edges);
    let schema = Arc::new(Schema::new(vec![
        Field::new(SRC_INDEX_COL, ArrowDataType::Int64, false),
        Field::new(DST_INDEX_COL, ArrowDataType::Int64, false),
        Field::new("creationDate", ArrowDataType::Utf8, true),
    ]));
    let table =
        RecordBatch::try_new(schema, vec![Arc::new(src), Arc::new(dst), Arc::new(date)]).unwrap();
    let written = write_edge_table(
        &knows_info(),
        &a.dir,
        &AdjListType::OrderedBySource,
        &table,
        a.vertices,
    )
    .unwrap();
    st.end(written, "edges");
}

/// The floor of the row-oriented writer API: build the same `Vec<Edge>` the
/// `write-edges` stage builds, then just drop it. Whatever `write-edges` costs
/// above this is the library's own work (sort, batches, parquet, files).
fn write_edges_floor(a: &Args) {
    let st = Stage::start("write-edges-floor");
    let mut edges = Vec::new();
    for (src, dst) in gen_edges(a) {
        edges.push(
            Edge::new(src, dst)
                .add_property("creationDate", PropertyValue::String("2026-11-04".into())),
        );
    }
    let n = edges.len() as u64;
    drop(edges);
    st.end(n, "edges built+dropped");
}

fn write_infos(a: &Args) {
    person_info().save(a.dir.join("person.vertex.yml")).unwrap();
    knows_info()
        .save(a.dir.join("person_knows_person.edge.yml"))
        .unwrap();
    GraphInfo::new("bench", "./")
        .with_vertex("person.vertex.yml")
        .with_edge("person_knows_person.edge.yml")
        .save(a.dir.join("bench.graph.yml"))
        .unwrap();
}

// ── read stages ─────────────────────────────────────────────────────────────

fn rows(batches: &[RecordBatch]) -> u64 {
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

fn threads() -> Option<usize> {
    std::env::var("GRAPHAR_BENCH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
}

fn read_vertices(a: &Args) {
    let st = Stage::start("read-vertices");
    #[allow(unused_mut)]
    let mut r = VertexReader::new(person_info(), &a.dir, a.vertices);
    // BEGIN new-api
    if let Some(t) = threads() {
        r = r.with_threads(t);
    }
    // END new-api
    let merged = r.read_merged().unwrap();
    st.end(merged.num_rows() as u64, "rows");
}

fn read_prop(a: &Args) {
    let st = Stage::start("read-prop");
    let r = VertexReader::new(person_info(), &a.dir, a.vertices);
    let b = r.read_by_property("name").unwrap();
    st.end(rows(&b), "rows");
}

fn read_adj(a: &Args) {
    let st = Stage::start("read-adj");
    let r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    let mut total = 0;
    for item in r.iter_vertex_chunks(&AdjListType::OrderedBySource) {
        let (_, batches) = item.unwrap();
        total += rows(&batches);
    }
    st.end(total, "edges");
}

/// Serial adjacency scan that keeps every batch (apples to apples with
/// `read-adj-par`, which returns them all).
fn read_adj_keep(a: &Args) {
    let st = Stage::start("read-adj-keep");
    let r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    let all: Vec<_> = r
        .iter_vertex_chunks(&AdjListType::OrderedBySource)
        .map(|x| x.unwrap())
        .collect();
    let total: u64 = all.iter().map(|(_, b)| rows(b)).sum();
    st.end(total, "edges");
}

fn adj_chunk_files(a: &Args) -> Vec<PathBuf> {
    let info = knows_info();
    let chunks = a.vertices.div_ceil(VERTEX_CHUNK);
    let mut files = Vec::new();
    for vc in 0..chunks {
        let mut ec = 0;
        while let Some(path) = graphar::io::resolve_chunk(
            a.dir.join(
                info.adj_list_chunk_path(&AdjListType::OrderedBySource, vc, ec)
                    .unwrap(),
            ),
            &FileType::Parquet,
        ) {
            files.push(path);
            ec += 1;
        }
    }
    files
}

/// Full adjacency scan with each chunk file read whole (`std::fs::read`) and
/// decoded from memory.
fn read_adj_bytes(a: &Args) {
    use graphar::storage::read_chunk_bytes;
    let st = Stage::start("read-adj-bytes");
    let mut total = 0u64;
    for path in adj_chunk_files(a) {
        let bytes = bytes::Bytes::from(std::fs::read(&path).unwrap());
        total += rows(&read_chunk_bytes(bytes, &FileType::Parquet).unwrap());
    }
    st.end(total, "edges");
}

fn read_offsets(a: &Args) {
    let st = Stage::start("read-offsets");
    let r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    let chunks = a.vertices.div_ceil(VERTEX_CHUNK);
    let mut total = 0;
    for vc in 0..chunks {
        total += rows(&r.read_offset(&AdjListType::OrderedBySource, vc).unwrap());
    }
    st.end(total, "offset rows");
}

fn int64(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        out.extend_from_slice(
            b.column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    out
}

/// "Edges of vertex v" through the plain reader: read the offset chunk for
/// v's vertex chunk, read the adjacency chunk(s), slice `[begin, end)`.
fn neighbors_plain(r: &EdgeReader, v: u64) -> Vec<i64> {
    let vc = v / VERTEX_CHUNK;
    let off = int64(
        &r.read_offset(&AdjListType::OrderedBySource, vc).unwrap(),
        OFFSET_COL,
    );
    let i = (v % VERTEX_CHUNK) as usize;
    let (begin, end) = (off[i] as usize, off[i + 1] as usize);
    let adj = int64(
        &r.read_adj_list(&AdjListType::OrderedBySource, vc).unwrap(),
        DST_INDEX_COL,
    );
    adj[begin..end].to_vec()
}

fn neighbors(a: &Args) {
    let st = Stage::start("neighbors");
    let r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    let mut rng = Rng(a.seed ^ 0xBEEF);
    let mut found = 0u64;
    for _ in 0..a.queries {
        let v = rng.next() % a.vertices;
        found += neighbors_plain(&r, v).len() as u64;
    }
    st.end(found, &format!("neighbor rows / {} queries", a.queries));
}

/// Pure in-memory index work, reported in ns/op: vertex id → (chunk, slot),
/// and the reverse lookup "which vertex owns edge row e" over the offsets
/// array via `partition_point` (the plain binary search the static-tree
/// question is about).
fn lookups(a: &Args) {
    let r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    let off = int64(
        &r.read_offset(&AdjListType::OrderedBySource, 0).unwrap(),
        OFFSET_COL,
    );
    let edges_in_chunk = *off.last().unwrap() as u64;
    let mut rng = Rng(a.seed ^ 0xCAFE);
    let n = 10_000_000u64;

    let t = Instant::now();
    let mut acc = 0u64;
    for _ in 0..n {
        let v = rng.next() % a.vertices;
        acc = acc
            .wrapping_add(v / VERTEX_CHUNK)
            .wrapping_add(v % VERTEX_CHUNK);
    }
    let ns = t.elapsed().as_nanos() as f64 / n as f64;
    println!("ROW lookup-id-to-chunk\t{ns:.1} ns/op\t(acc {acc})");

    let queries: Vec<i64> = (0..n)
        .map(|_| (rng.next() % edges_in_chunk) as i64)
        .collect();
    let t = Instant::now();
    let mut acc = 0usize;
    for &e in &queries {
        // first vertex whose end offset is past e = owner of edge row e
        acc = acc.wrapping_add(off.partition_point(|&o| o <= e));
    }
    let ns = t.elapsed().as_nanos() as f64 / n as f64;
    println!(
        "ROW lookup-edge-to-vertex-partition-point\t{ns:.1} ns/op\t(offsets {} rows, acc {acc})",
        off.len()
    );
}

/// Order-independent fingerprint of the written edge data: per vertex chunk,
/// the offsets array (exact) and a commutative hash of the (src, dst) rows, so
/// two writers that order a vertex's edges differently still agree.
fn checksum(a: &Args) {
    let r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    let chunks = a.vertices.div_ceil(VERTEX_CHUNK);
    let mut off_h = 0u64;
    let mut adj_h = 0u64;
    let mut n = 0u64;
    for vc in 0..chunks {
        for o in int64(
            &r.read_offset(&AdjListType::OrderedBySource, vc).unwrap(),
            OFFSET_COL,
        ) {
            off_h = off_h.wrapping_mul(0x100000001b3).wrapping_add(o as u64);
        }
        let b = r.read_adj_list(&AdjListType::OrderedBySource, vc).unwrap();
        let (src, dst) = (int64(&b, SRC_INDEX_COL), int64(&b, DST_INDEX_COL));
        for (s, d) in src.iter().zip(&dst) {
            let x = (*s as u64).wrapping_mul(0x9E3779B97F4A7C15)
                ^ (*d as u64).wrapping_mul(0xC2B2AE3D27D4EB4F);
            adj_h = adj_h.wrapping_add(x.wrapping_mul(x | 1));
            n += 1;
        }
    }
    println!("ROW checksum\toffsets {off_h:016x}\tadj {adj_h:016x}\t{n} edges");
}

fn validate(a: &Args) {
    let st = Stage::start("validate");
    let issues = validate_graph_dir(a.dir.join("bench.graph.yml"));
    for i in issues.iter().take(5) {
        println!("  issue: {i}");
    }
    st.end(issues.len() as u64, "issues");
}

/// Read every table of an upstream sample graph (e.g. testing/ldbc_sample/parquet).
fn ldbc(dir: &Path) {
    let yml = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with(".graph.yml"))
        .expect("no *.graph.yml in dir");
    let st = Stage::start(&format!("ldbc {}", dir.display()));
    let gi = GraphInfo::load(&yml).unwrap();
    let base = dir.join(&gi.prefix);
    let mut total = 0u64;
    for vi in gi.all_vertex_infos(dir).unwrap() {
        let Ok(r) = VertexReader::open(vi, &base) else {
            continue;
        };
        if let Ok(m) = r.read_merged() {
            total += m.num_rows() as u64;
        }
    }
    for ei in gi.all_edge_infos(dir).unwrap() {
        for adj in &ei.adj_lists {
            let Ok(t) = adj.adj_list_type() else { continue };
            let Ok(r) = EdgeReader::open(ei.clone(), &base, &t) else {
                continue;
            };
            for (_, b) in r.iter_vertex_chunks(&t).flatten() {
                total += rows(&b);
            }
        }
    }
    st.end(total, "rows");
}

// BEGIN new-api

/// "Edges of vertex v" through the pushdown path (`EdgeReader::neighbors`).
fn neighbors_pushdown(a: &Args) {
    let st = Stage::start("neighbors-pushdown");
    let r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    let mut rng = Rng(a.seed ^ 0xBEEF);
    let mut found = 0u64;
    for _ in 0..a.queries {
        let v = rng.next() % a.vertices;
        found += r.neighbors(&AdjListType::OrderedBySource, v).unwrap().len() as u64;
    }
    st.end(found, &format!("neighbor rows / {} queries", a.queries));
}

/// Serial vertex read (threads = 1) for the before/after of parallel decode.
fn read_vertices_serial(a: &Args) {
    let st = Stage::start("read-vertices-serial");
    let r = VertexReader::new(person_info(), &a.dir, a.vertices).with_threads(1);
    let merged = r.read_merged().unwrap();
    st.end(merged.num_rows() as u64, "rows");
}

/// Full adjacency scan through the parallel reader.
fn read_adj_par(a: &Args) {
    let st = Stage::start("read-adj-par");
    let mut r = EdgeReader::new(knows_info(), &a.dir, a.vertices, a.vertices);
    if let Some(t) = threads() {
        r = r.with_threads(t);
    }
    let mut total = 0;
    for (_, batches) in r.read_all_adj_lists(&AdjListType::OrderedBySource).unwrap() {
        total += rows(&batches);
    }
    st.end(total, "edges");
}

/// Full adjacency scan with every chunk file memory-mapped and handed to the
/// in-memory decoder (`read_chunk_bytes` over the mmap) instead of `File`.
fn read_adj_mmap(a: &Args) {
    use graphar::storage::read_chunk_bytes;
    let st = Stage::start("read-adj-mmap");
    let mut total = 0u64;
    for path in adj_chunk_files(a) {
        let file = std::fs::File::open(&path).unwrap();
        // SAFETY: the chunk files are immutable while the bench runs.
        let map = unsafe { memmap2::Mmap::map(&file).unwrap() };
        let bytes = bytes::Bytes::from_owner(map);
        total += rows(&read_chunk_bytes(bytes, &FileType::Parquet).unwrap());
    }
    st.end(total, "edges");
}

/// Sorted-key lookups: `partition_point` (binary search), an Eytzinger
/// layout (branchless, cache-friendlier), and Ragnar Groot Koerkamp's static
/// search tree (`STree64`, znippy-zoomies), for exact-match lookups of
/// present keys in a sorted unique `i64` array of `n` keys. Reports ns/op.
fn stree(a: &Args) {
    use znippy_zoomies::stree::STree64;
    let mut rng = Rng(a.seed ^ 0x5EED);
    for n in [262_144usize, 1 << 20, 1 << 24] {
        // sparse ascending keys (LDBC-like ids), unique
        let mut keys: Vec<i64> = Vec::with_capacity(n);
        let mut k = 0i64;
        for _ in 0..n {
            k += 1 + (rng.next() % 1000) as i64;
            keys.push(k);
        }
        let q = 10_000_000usize;
        let queries: Vec<i64> = (0..q).map(|_| keys[(rng.next() as usize) % n]).collect();

        let t = Instant::now();
        let mut acc = 0usize;
        for &x in &queries {
            acc = acc.wrapping_add(keys.partition_point(|&k| k < x));
        }
        let bs = t.elapsed().as_nanos() as f64 / q as f64;

        // Eytzinger: BFS order of the implicit binary tree, branchless descent.
        let mut ey = vec![0i64; n + 1];
        fn fill(keys: &[i64], ey: &mut [i64], i: &mut usize, k: usize) {
            if k <= keys.len() {
                fill(keys, ey, i, 2 * k);
                ey[k] = keys[*i];
                *i += 1;
                fill(keys, ey, i, 2 * k + 1);
            }
        }
        let mut i = 0;
        fill(&keys, &mut ey, &mut i, 1);
        let t = Instant::now();
        let mut acc2 = 0usize;
        for &x in &queries {
            let mut k = 1usize;
            while k <= n {
                k = 2 * k + usize::from(ey[k] < x);
            }
            // lower bound: undo the last right turns
            k >>= k.trailing_ones() + 1;
            acc2 = acc2.wrapping_add(k);
        }
        let eytz_ns = t.elapsed().as_nanos() as f64 / q as f64;

        let tb = Instant::now();
        let tree = STree64::new(&keys);
        let build_ms = tb.elapsed().as_secs_f64() * 1e3;
        let t = Instant::now();
        let mut acc3 = 0usize;
        for &x in &queries {
            acc3 = acc3.wrapping_add(tree.find_exact(x).unwrap_or(0));
        }
        let st = t.elapsed().as_nanos() as f64 / q as f64;
        // the tree returns the index into `keys`; partition_point too
        assert_eq!(acc, acc3, "STree64 index != partition_point index");
        println!(
            "ROW stree n={n}\tpartition_point {bs:.1} ns/op\teytzinger {eytz_ns:.1} ns/op\tSTree64 {st:.1} ns/op (build {build_ms:.1} ms)\t(acc {acc} {acc2})"
        );
    }
}

// END new-api

fn main() {
    let a = parse_args();
    match a.stage.as_str() {
        "gen" => {
            std::fs::create_dir_all(&a.dir).unwrap();
            write_vertices(&a);
            write_edges(&a);
            write_infos(&a);
        }
        "write-vertices" => {
            write_vertices(&a);
        }
        "write-vertices-table" => write_vertices_table(&a),
        "write-edges" => write_edges(&a),
        "write-edges-table" => write_edges_table(&a),
        "write-edges-floor" => write_edges_floor(&a),
        "write-infos" => write_infos(&a),
        "read-vertices" => read_vertices(&a),
        "read-prop" => read_prop(&a),
        "read-adj" => read_adj(&a),
        "read-adj-keep" => read_adj_keep(&a),
        "read-adj-bytes" => read_adj_bytes(&a),
        "read-offsets" => read_offsets(&a),
        "neighbors" => neighbors(&a),
        "lookups" => lookups(&a),
        "checksum" => checksum(&a),
        "validate" => validate(&a),
        "ldbc" => ldbc(&a.dir),
        // BEGIN new-api
        "neighbors-pushdown" => neighbors_pushdown(&a),
        "read-vertices-serial" => read_vertices_serial(&a),
        "read-adj-par" => read_adj_par(&a),
        "read-adj-mmap" => read_adj_mmap(&a),
        "stree" => stree(&a),
        // END new-api
        "all" => {
            std::fs::create_dir_all(&a.dir).unwrap();
            write_vertices(&a);
            write_edges(&a);
            write_infos(&a);
            read_vertices(&a);
            read_prop(&a);
            read_adj(&a);
            read_offsets(&a);
            neighbors(&a);
            lookups(&a);
            validate(&a);
        }
        other => panic!("unknown stage {other}"),
    }
}
