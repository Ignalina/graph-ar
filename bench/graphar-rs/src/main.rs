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

//! `graphar-rs-bench write-builder <input-dir> <info-dir> <out-graph-dir>`:
//! the vertices of an input dir through `graphar_rs::builder::VerticesBuilder`.
//! graphar-rs 0.1.0 has no edge builder and no readers yet.

use std::{fs::File, path::Path, time::Instant};

use arrow::array::{Array, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::DataType;
use graphar_rs::{
    builder::{Vertex, VerticesBuilder},
    info::GraphInfo,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

type R<T> = Result<T, Box<dyn std::error::Error>>;

fn main() -> R<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.first().map(String::as_str) != Some("write-builder") || a.len() != 4 {
        return Err("usage: write-builder <input-dir> <info-dir> <out-graph-dir>".into());
    }
    let (input, info, out) = (Path::new(&a[1]), Path::new(&a[2]), Path::new(&a[3]));
    std::fs::create_dir_all(out)?;
    let mut graph_yml = None;
    for e in std::fs::read_dir(info)? {
        let p = e?.path();
        let to = out.join(p.file_name().unwrap());
        std::fs::copy(&p, &to)?;
        if to.to_string_lossy().ends_with(".graph.yml") {
            graph_yml = Some(to);
        }
    }
    let gi = GraphInfo::load(std::fs::canonicalize(graph_yml.ok_or("no graph.yml")?)?)?;
    let vi = gi.vertex_infos().remove(0);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(input.join("vertices.parquet"))?)?
        .with_batch_size(1 << 20)
        .build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    let prefix = format!("{}/", std::fs::canonicalize(out)?.display());

    let start = Instant::now();
    let mut vb = VerticesBuilder::new(&vi, &prefix, 0);
    for b in &batches {
        let names: Vec<String> = b.schema().fields().iter().map(|f| f.name().clone()).collect();
        for i in 0..b.num_rows() {
            let mut v = Vertex::new();
            for (c, name) in b.columns().iter().zip(&names) {
                if c.is_null(i) {
                    continue;
                }
                match c.data_type() {
                    DataType::Int64 => v.add_property_i64(name, c.as_any().downcast_ref::<Int64Array>().unwrap().value(i)),
                    DataType::Utf8 => v.add_property_string(name, c.as_string::<i32>().value(i)),
                    other => return Err(format!("type {other}").into()),
                }
            }
            vb.add_vertex(v)?;
        }
    }
    vb.dump()?;
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{{\"impl\":\"graphar-rs\",\"op\":\"write-vertices-builder\",\"secs\":{secs},\"check\":{{\"vertices\":{n}}}}}"
    );
    Ok(())
}
