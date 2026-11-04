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

//! Bench harness for the native Rust GraphAr crate. The C++ harness in
//! `../cpp` takes the same commands and prints the same JSON, so both read
//! and write the same files.
//!
//! ```text
//! graphar-bench prep-mimir <input-dir> <dump-dir>...
//! graphar-bench prep-gar   <input-dir> <graph.yml> <vertex-type> <src> <edge> <dst>
//! graphar-bench info       <input-dir> <format> <out-info-dir>
//! graphar-bench write      <table|builder> <input-dir> <info-dir> <out-graph-dir>
//! graphar-bench read-prop  <graph-dir> <property>
//! graphar-bench read-csr   <graph-dir>
//! graphar-bench scan       <graph-dir>
//! ```
//!
//! An input dir holds `vertices.parquet`, `edges.parquet` (with
//! `_graphArSrcIndex`, `_graphArDstIndex` and the edge properties) and
//! `schema.json` (types, property groups, chunk sizes).

use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::compute::concat_batches;
use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt32Array,
    builder::{Int64Builder, StringBuilder},
    cast::AsArray,
};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use graphar::{
    AdjListInfo, AdjListType, DST_INDEX_COL, DataType, Edge, EdgeInfo, EdgeReader, EdgesBuilder,
    FileType, GraphInfo, Property, PropertyGroup, PropertyValue, SRC_INDEX_COL, Vertex,
    VertexInfo, VertexReader, VerticesBuilder, write_edge_table, write_vertex_table,
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use serde_json::{Value, json};

type R<T> = Result<T, Box<dyn std::error::Error>>;

const IMPL: &str = "graphar-native";

fn main() -> R<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let a = |i: usize| -> &str { args.get(i).map(String::as_str).unwrap_or_default() };
    match a(0) {
        "prep-mimir" => prep_mimir(Path::new(a(1)), &args[2..]),
        "prep-gar" => prep_gar(Path::new(a(1)), Path::new(a(2)), a(3), [a(4), a(5), a(6)]),
        "info" => info(Path::new(a(1)), a(2), Path::new(a(3))),
        "write" => write(a(1), Path::new(a(2)), Path::new(a(3)), Path::new(a(4))),
        "read-prop" => read_prop(Path::new(a(1)), a(2)),
        "read-csr" => read_csr(Path::new(a(1))),
        "scan" => scan(Path::new(a(1))),
        other => Err(format!("unknown command `{other}`").into()),
    }
}

fn emit(op: &str, secs: f64, check: Value) {
    println!(
        "{}",
        json!({ "impl": IMPL, "op": op, "secs": secs, "check": check })
    );
}

// ---------------------------------------------------------------- input files

fn read_parquet(path: &Path) -> R<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(1 << 20)
        .build()?;
    let schema = arrow::record_batch::RecordBatchReader::schema(&reader);
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    Ok(concat_batches(&schema, &batches)?)
}

fn write_parquet(path: &Path, batch: &RecordBatch) -> R<()> {
    let mut w = ArrowWriter::try_new(File::create(path)?, batch.schema(), None)?;
    w.write(batch)?;
    w.close()?;
    Ok(())
}

fn type_name(dt: &ArrowDataType) -> &'static str {
    match dt {
        ArrowDataType::Int64 | ArrowDataType::UInt32 => "int64",
        ArrowDataType::Int32 => "int32",
        ArrowDataType::Float64 => "double",
        ArrowDataType::Float32 => "float",
        ArrowDataType::Boolean => "bool",
        _ => "string",
    }
}

fn data_type(name: &str) -> DataType {
    match name {
        "int64" => DataType::Int64,
        "int32" => DataType::Int32,
        "double" => DataType::Double,
        "float" => DataType::Float,
        "bool" => DataType::Bool,
        _ => DataType::String,
    }
}

/// schema.json: vertex/edge type names, property groups (lists of column
/// names; the first column of the first vertex group is the primary key) and
/// chunk sizes.
fn write_schema(
    dir: &Path,
    vertices: &RecordBatch,
    edges: &RecordBatch,
    names: [&str; 4],
    vgroups: &[&[&str]],
    egroups: &[&[&str]],
    chunks: [u64; 2],
) -> R<()> {
    let types = |b: &RecordBatch| -> Value {
        b.schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), json!(type_name(f.data_type()))))
            .collect::<serde_json::Map<_, _>>()
            .into()
    };
    let s = json!({
        "vertex_type": names[0], "src": names[1], "edge_type": names[2], "dst": names[3],
        "vertex_groups": vgroups, "edge_groups": egroups,
        "vertex_types": types(vertices), "edge_types": types(edges),
        "vertex_chunk": chunks[0], "edge_chunk": chunks[1],
        "vertices": vertices.num_rows(), "edges": edges.num_rows(),
    });
    std::fs::write(dir.join("schema.json"), serde_json::to_vec_pretty(&s)?)?;
    Ok(())
}

/// Convert Mímir rustc graph dumps (`*.nodes.arrow`, `*.edges.arrow`, Arrow
/// IPC files) into one vertex table and one edge table. Node ids are
/// monikers; duplicates across dumps (shared dependencies) are merged, edges
/// whose endpoints are not in any dump are dropped, duplicate edges merged.
fn prep_mimir(out: &Path, dumps: &[String]) -> R<()> {
    std::fs::create_dir_all(out)?;
    let mut ids: HashMap<String, i64> = HashMap::new();
    let (mut id_b, mut kind_b, mut name_b, mut crate_b) = (
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut line_b = Int64Builder::new();
    let mut files: Vec<PathBuf> = Vec::new();
    for d in dumps {
        for e in std::fs::read_dir(d)? {
            files.push(e?.path());
        }
    }
    files.sort();
    for f in files.iter().filter(|f| f.to_string_lossy().ends_with(".nodes.arrow")) {
        for batch in arrow::ipc::reader::FileReader::try_new(File::open(f)?, None)? {
            let b = batch?;
            let col = |n: &str| b.column_by_name(n).unwrap().as_string::<i32>().clone();
            let (id, kind, name, krate) = (col("id"), col("kind"), col("name"), col("crate"));
            let line = b.column_by_name("start_line").unwrap();
            let line = line.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..b.num_rows() {
                let key = id.value(i);
                if ids.contains_key(key) {
                    continue;
                }
                ids.insert(key.to_string(), ids.len() as i64);
                id_b.append_value(key);
                kind_b.append_value(kind.value(i));
                name_b.append_value(name.value(i));
                crate_b.append_value(krate.value(i));
                if line.is_null(i) {
                    line_b.append_null();
                } else {
                    line_b.append_value(line.value(i) as i64);
                }
            }
        }
    }
    let vertices = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Utf8, false),
            Field::new("kind", ArrowDataType::Utf8, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("crate", ArrowDataType::Utf8, false),
            Field::new("start_line", ArrowDataType::Int64, true),
        ])),
        vec![
            Arc::new(id_b.finish()),
            Arc::new(kind_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(crate_b.finish()),
            Arc::new(line_b.finish()),
        ],
    )?;

    let mut seen: HashMap<(i64, i64, String), usize> = HashMap::new();
    let (mut src, mut dst): (Vec<i64>, Vec<i64>) = (vec![], vec![]);
    let (mut kinds, mut counts): (Vec<String>, Vec<i64>) = (vec![], vec![]);
    let mut dropped = 0usize;
    for f in files.iter().filter(|f| f.to_string_lossy().ends_with(".edges.arrow")) {
        for batch in arrow::ipc::reader::FileReader::try_new(File::open(f)?, None)? {
            let b = batch?;
            let col = |n: &str| b.column_by_name(n).unwrap().as_string::<i32>().clone();
            let (from, to, kind) = (col("from"), col("to"), col("kind"));
            let count = b.column_by_name("count").unwrap();
            let count = count.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..b.num_rows() {
                let (Some(&s), Some(&d)) = (ids.get(from.value(i)), ids.get(to.value(i))) else {
                    dropped += 1;
                    continue;
                };
                let k = (s, d, kind.value(i).to_string());
                if let Some(&at) = seen.get(&k) {
                    counts[at] = counts[at].max(count.value(i) as i64);
                    continue;
                }
                seen.insert(k, src.len());
                src.push(s);
                dst.push(d);
                kinds.push(kind.value(i).to_string());
                counts.push(count.value(i) as i64);
            }
        }
    }
    let edges = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(SRC_INDEX_COL, ArrowDataType::Int64, false),
            Field::new(DST_INDEX_COL, ArrowDataType::Int64, false),
            Field::new("kind", ArrowDataType::Utf8, false),
            Field::new("count", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(src)),
            Arc::new(Int64Array::from(dst)),
            Arc::new(StringArray::from(kinds)),
            Arc::new(Int64Array::from(counts)),
        ],
    )?;
    write_parquet(&out.join("vertices.parquet"), &vertices)?;
    write_parquet(&out.join("edges.parquet"), &edges)?;
    write_schema(
        out,
        &vertices,
        &edges,
        ["node", "node", "edge", "node"],
        &[&["id"], &["kind", "name", "crate"], &["start_line"]],
        &[&["kind", "count"]],
        [1 << 18, 1 << 22],
    )?;
    eprintln!(
        "mimir: {} vertices, {} edges ({} dangling dropped)",
        vertices.num_rows(),
        edges.num_rows(),
        dropped
    );
    Ok(())
}

/// Make an input dir from an existing GraphAr graph (the upstream sample
/// data): all vertex properties, and the edges of its ordered_by_source list
/// with their properties.
fn prep_gar(out: &Path, graph_yml: &Path, vtype: &str, triple: [&str; 3]) -> R<()> {
    std::fs::create_dir_all(out)?;
    let base = graph_yml.parent().unwrap();
    let gi = GraphInfo::load(graph_yml)?;
    let vi = gi.get_vertex_info(vtype, base)?;
    let ei = gi.get_edge_info(triple[0], triple[1], triple[2], base)?;
    let merged = VertexReader::open(vi.clone(), base)?.read_merged()?;
    let keep: Vec<usize> = (0..merged.num_columns())
        .filter(|&i| merged.schema().field(i).name() != graphar::VERTEX_INDEX_COL)
        .collect();
    let vertices = merged.project(&keep)?;
    let t = AdjListType::OrderedBySource;
    let er = EdgeReader::open(ei.clone(), base, &t)?;
    let (mut adj, mut props) = (Vec::new(), Vec::new());
    for vc in 0..er_vertex_chunks(&er, &t) {
        adj.extend(er.read_adj_list(&t, vc)?);
        for g in &ei.property_groups {
            props.extend(er.read_property_group(&t, g, vc)?);
        }
    }
    let adj = concat_batches(&adj[0].schema(), &adj)?;
    let mut fields: Vec<Field> = adj.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut cols: Vec<ArrayRef> = adj.columns().to_vec();
    if !props.is_empty() {
        let p = concat_batches(&props[0].schema(), &props)?;
        fields.extend(p.schema().fields().iter().map(|f| f.as_ref().clone()));
        cols.extend(p.columns().iter().cloned());
    }
    let edges = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?;
    write_parquet(&out.join("vertices.parquet"), &vertices)?;
    write_parquet(&out.join("edges.parquet"), &edges)?;
    let vgroups: Vec<Vec<String>> = vi
        .property_groups
        .iter()
        .map(|g| g.properties.iter().map(|p| p.name.clone()).collect())
        .collect();
    let egroups: Vec<Vec<String>> = ei
        .property_groups
        .iter()
        .map(|g| g.properties.iter().map(|p| p.name.clone()).collect())
        .collect();
    let vg: Vec<Vec<&str>> = vgroups.iter().map(|g| g.iter().map(String::as_str).collect()).collect();
    let eg: Vec<Vec<&str>> = egroups.iter().map(|g| g.iter().map(String::as_str).collect()).collect();
    let vg: Vec<&[&str]> = vg.iter().map(Vec::as_slice).collect();
    let eg: Vec<&[&str]> = eg.iter().map(Vec::as_slice).collect();
    write_schema(
        out,
        &vertices,
        &edges,
        [vtype, triple[0], triple[1], triple[2]],
        &vg,
        &eg,
        [vi.chunk_size, ei.chunk_size],
    )?;
    eprintln!("gar: {} vertices, {} edges", vertices.num_rows(), edges.num_rows());
    Ok(())
}

fn er_vertex_chunks(er: &EdgeReader, t: &AdjListType) -> u64 {
    er.iter_vertex_chunks(t).count() as u64
}

// ----------------------------------------------------------------- info files

fn load_schema(input: &Path) -> R<Value> {
    Ok(serde_json::from_slice(&std::fs::read(input.join("schema.json"))?)?)
}

fn file_type(fmt: &str) -> R<FileType> {
    Ok(match fmt {
        "parquet" => FileType::Parquet,
        "csv" => FileType::Csv,
        "orc" => FileType::Orc,
        "json" => FileType::Json,
        "arrow" => FileType::ArrowIpc,
        _ => return Err(format!("format {fmt}").into()),
    })
}

/// Write the three info files for one chunk format.
fn info(input: &Path, fmt: &str, out: &Path) -> R<()> {
    let s = load_schema(input)?;
    let ft = file_type(fmt)?;
    let str_of = |v: &Value| v.as_str().unwrap().to_string();
    let vtype = str_of(&s["vertex_type"]);
    let groups = |key: &str, types: &Value, primary: bool| -> Vec<PropertyGroup> {
        s[key]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(gi, g)| {
                let props = g
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(pi, p)| {
                        let name = str_of(p);
                        let prop = Property::new(&name, data_type(types[&name].as_str().unwrap()));
                        if primary && gi == 0 && pi == 0 { prop.primary() } else { prop }
                    })
                    .collect();
                PropertyGroup::new(props, ft.clone())
            })
            .collect()
    };
    let mut vi = VertexInfo::new(&vtype, s["vertex_chunk"].as_u64().unwrap());
    for g in groups("vertex_groups", &s["vertex_types"], true) {
        vi = vi.with_property_group(g);
    }
    let vchunk = s["vertex_chunk"].as_u64().unwrap();
    let mut ei = EdgeInfo::new(
        str_of(&s["src"]),
        str_of(&s["edge_type"]),
        str_of(&s["dst"]),
        s["edge_chunk"].as_u64().unwrap(),
        vchunk,
        vchunk,
        true,
    )
    .with_adj_list(AdjListInfo {
        ordered: true,
        aligned_by: "src".into(),
        file_type: ft.clone(),
        prefix: None,
    });
    for g in groups("edge_groups", &s["edge_types"], false) {
        ei = ei.with_property_group(g);
    }
    std::fs::create_dir_all(out)?;
    let vyml = format!("{vtype}.vertex.yml");
    let eyml = format!("{}_{}_{}.edge.yml", ei.src_type, ei.edge_type, ei.dst_type);
    vi.save(out.join(&vyml))?;
    ei.save(out.join(&eyml))?;
    GraphInfo::new("bench", "")
        .with_vertex(&vyml)
        .with_edge(&eyml)
        .save(out.join("bench.graph.yml"))?;
    Ok(())
}

fn open_graph(dir: &Path) -> R<(VertexInfo, EdgeInfo)> {
    // The first *.graph.yml by name (the upstream sample dirs hold several).
    let mut ymls: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".graph.yml"))
        .collect();
    ymls.sort();
    let yml = ymls.into_iter().next().ok_or("no *.graph.yml")?;
    let gi = GraphInfo::load(&yml)?;
    let vi = gi.all_vertex_infos(dir)?.remove(0);
    let ei = gi.all_edge_infos(dir)?.remove(0);
    Ok((vi, ei))
}

// ---------------------------------------------------------------------- write

fn write(api: &str, input: &Path, info_dir: &Path, out: &Path) -> R<()> {
    std::fs::create_dir_all(out)?;
    for e in std::fs::read_dir(info_dir)? {
        let p = e?.path();
        std::fs::copy(&p, out.join(p.file_name().unwrap()))?;
    }
    let (vi, ei) = open_graph(out)?;
    let vertices = read_parquet(&input.join("vertices.parquet"))?;
    let edges = read_parquet(&input.join("edges.parquet"))?;
    let n = vertices.num_rows() as u64;
    let t = AdjListType::OrderedBySource;

    let start = Instant::now();
    match api {
        "table" => {
            write_vertex_table(&vi, out, &vertices)?;
        }
        "builder" => {
            let mut vb = VerticesBuilder::new(vi.clone(), out);
            let cols: Vec<(String, ArrayRef)> = vertices
                .schema()
                .fields()
                .iter()
                .zip(vertices.columns())
                .map(|(f, c)| (f.name().clone(), c.clone()))
                .collect();
            for i in 0..vertices.num_rows() {
                let mut v = Vertex::new();
                for (name, c) in &cols {
                    if let Some(val) = value_at(c, i) {
                        v = v.add_property(name.clone(), val);
                    }
                }
                vb.add_vertex(v);
            }
            vb.dump()?;
        }
        _ => return Err(format!("api {api}").into()),
    }
    let vsecs = start.elapsed().as_secs_f64();

    let start = Instant::now();
    match api {
        "table" => {
            write_edge_table(&ei, out, &t, &edges, n)?;
        }
        _ => {
            let mut eb = EdgesBuilder::new(ei.clone(), out, t.clone(), n);
            let src = edges.column_by_name(SRC_INDEX_COL).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().clone();
            let dst = edges.column_by_name(DST_INDEX_COL).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().clone();
            let props: Vec<(String, ArrayRef)> = edges
                .schema()
                .fields()
                .iter()
                .zip(edges.columns())
                .filter(|(f, _)| f.name() != SRC_INDEX_COL && f.name() != DST_INDEX_COL)
                .map(|(f, c)| (f.name().clone(), c.clone()))
                .collect();
            for i in 0..edges.num_rows() {
                let mut e = Edge::new(src.value(i), dst.value(i));
                for (name, c) in &props {
                    if let Some(val) = value_at(c, i) {
                        e = e.add_property(name.clone(), val);
                    }
                }
                eb.add_edge(e);
            }
            eb.dump()?;
        }
    }
    let esecs = start.elapsed().as_secs_f64();
    emit(
        &format!("write-vertices-{api}"),
        vsecs,
        json!({ "vertices": n, "bytes": dir_bytes(&out.join(&vi.prefix)) }),
    );
    emit(
        &format!("write-edges-{api}"),
        esecs,
        json!({ "edges": edges.num_rows(), "bytes": dir_bytes(&out.join(&ei.prefix)) }),
    );
    Ok(())
}

fn value_at(c: &ArrayRef, i: usize) -> Option<PropertyValue> {
    if c.is_null(i) {
        return None;
    }
    Some(match c.data_type() {
        ArrowDataType::Int64 => PropertyValue::Int64(c.as_any().downcast_ref::<Int64Array>()?.value(i)),
        ArrowDataType::Utf8 => PropertyValue::String(c.as_string::<i32>().value(i).to_string()),
        ArrowDataType::Utf8View => PropertyValue::String(c.as_string_view().value(i).to_string()),
        _ => return None,
    })
}

fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            total += if p.is_dir() {
                dir_bytes(&p)
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            };
        }
    }
    total
}

// ----------------------------------------------------------------------- read

fn checksum_column(c: &dyn Array) -> u64 {
    match c.data_type() {
        ArrowDataType::Int64 => c
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .fold(0u64, |a, v| a.wrapping_add(v as u64)),
        ArrowDataType::Utf8 => {
            let s = c.as_string::<i32>();
            (0..s.len()).filter(|&i| s.is_valid(i)).map(|i| s.value(i).len() as u64).sum()
        }
        ArrowDataType::LargeUtf8 => {
            let s = c.as_string::<i64>();
            (0..s.len()).filter(|&i| s.is_valid(i)).map(|i| s.value(i).len() as u64).sum()
        }
        _ => c.len() as u64,
    }
}

fn read_prop(dir: &Path, prop: &str) -> R<()> {
    let (vi, _) = open_graph(dir)?;
    let start = Instant::now();
    let vr = VertexReader::open(vi, dir)?;
    let batches = vr.read_by_property(prop)?;
    let secs = start.elapsed().as_secs_f64();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let sum = batches
        .iter()
        .map(|b| checksum_column(b.column_by_name(prop).unwrap().as_ref()))
        .fold(0u64, u64::wrapping_add);
    emit("read-prop", secs, json!({ "rows": rows, "sum": sum, "prop": prop }));
    Ok(())
}

fn read_csr(dir: &Path) -> R<()> {
    let (_, ei) = open_graph(dir)?;
    let t = AdjListType::OrderedBySource;
    let start = Instant::now();
    let er = EdgeReader::open(ei, dir, &t)?;
    let (offsets, indices) = er.read_csr(&t)?;
    let secs = start.elapsed().as_secs_f64();
    emit(
        "read-csr",
        secs,
        json!({
            "n": offsets.len() - 1,
            "m": indices.len(),
            "sum_indices": indices.iter().fold(0u64, |a, &v| a.wrapping_add(v as u64)),
            "sum_offsets": offsets.iter().fold(0u64, |a, &v| a.wrapping_add(v as u64)),
        }),
    );
    Ok(())
}

/// Every vertex property group, the adjacency list and every edge property
/// group, chunk by chunk.
fn scan(dir: &Path) -> R<()> {
    let (vi, ei) = open_graph(dir)?;
    let t = AdjListType::OrderedBySource;
    let start = Instant::now();
    let vr = VertexReader::open(vi.clone(), dir)?;
    let mut vrows = 0usize;
    let mut cells = 0u64;
    let mut sum = 0u64;
    for g in &vi.property_groups {
        for b in vr.read_property_group(g)? {
            vrows += b.num_rows();
            for p in &g.properties {
                let c = b.column_by_name(&p.name).unwrap();
                cells += c.len() as u64;
                sum = sum.wrapping_add(checksum_column(c.as_ref()));
            }
        }
    }
    let er = EdgeReader::open(ei.clone(), dir, &t)?;
    let mut erows = 0usize;
    for vc in 0..er_vertex_chunks(&er, &t) {
        for b in er.read_adj_list(&t, vc)? {
            erows += b.num_rows();
            sum = sum.wrapping_add(checksum_column(b.column_by_name(DST_INDEX_COL).unwrap().as_ref()));
        }
        for g in &ei.property_groups {
            for b in er.read_property_group(&t, g, vc)? {
                for p in &g.properties {
                    let c = b.column_by_name(&p.name).unwrap();
                    cells += c.len() as u64;
                    sum = sum.wrapping_add(checksum_column(c.as_ref()));
                }
            }
        }
    }
    let secs = start.elapsed().as_secs_f64();
    emit(
        "scan",
        secs,
        json!({ "vertex_rows": vrows, "edges": erows, "cells": cells, "sum": sum }),
    );
    Ok(())
}
