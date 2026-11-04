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

//! On-disk layout tests against the Apache GraphAr `gar/v1` layout as the
//! upstream C++, Java and Spark libraries write it: extensionless `chunk<N>`
//! files, the reserved `_graphAr*` column names, and the `vertex_count` /
//! `edge_count<N>` files. The `upstream_*` tests read the upstream `testing`
//! data from `GAR_TEST_DATA`, or from the `testing` submodule, and skip when
//! neither is present.

#[cfg(test)]
mod layout {
    use std::{path::Path, sync::Arc};

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use tempfile::tempdir;

    use crate::{
        DST_INDEX_COL, OFFSET_COL, SRC_INDEX_COL, VERTEX_INDEX_COL, io,
        metadata::{AdjListInfo, EdgeInfo, Property, PropertyGroup, VertexInfo},
        reader::{EdgeReader, VertexReader},
        types::{AdjListType, DataType, FileType},
        validate::{Severity, ValidateOptions, validate_edge_info, validate_vertex_info},
        writer::{
            Edge, EdgesBuilder, PropertyValue, Vertex, VerticesBuilder, write_edge_table,
            write_vertex_table,
        },
    };

    fn person(chunk: u64) -> VertexInfo {
        VertexInfo::new("person", chunk)
            .with_property_group(PropertyGroup::new(
                vec![Property::new("id", DataType::Int64).primary()],
                FileType::Parquet,
            ))
            .with_property_group(PropertyGroup::new(
                vec![Property::new("name", DataType::String)],
                FileType::Parquet,
            ))
    }

    fn knows(vchunk: u64, echunk: u64, ordered: bool) -> EdgeInfo {
        EdgeInfo::new("person", "knows", "person", echunk, vchunk, vchunk, false).with_adj_list(
            AdjListInfo {
                ordered,
                aligned_by: "src".into(),
                file_type: FileType::Parquet,
                prefix: None,
            },
        )
    }

    fn i64s(b: &RecordBatch, col: &str) -> Vec<i64> {
        b.column_by_name(col)
            .unwrap_or_else(|| panic!("column {col} missing in {:?}", b.schema()))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn concat_i64(batches: &[RecordBatch], col: &str) -> Vec<i64> {
        batches.iter().flat_map(|b| i64s(b, col)).collect()
    }

    fn dump_people(base: &Path, n: i64, chunk: u64) -> VertexInfo {
        let vi = person(chunk);
        let mut vb = VerticesBuilder::new(vi.clone(), base);
        for i in 0..n {
            vb.add_vertex(
                Vertex::new()
                    .add_property("id", PropertyValue::Int64(1000 + i))
                    .add_property("name", PropertyValue::String(format!("p{i}"))),
            );
        }
        vb.dump().unwrap();
        vi
    }

    #[test]
    fn info_files_indent_sequences_like_upstream() {
        // The C++ YAML reader needs block sequences indented under their key,
        // the way the upstream info files are written.
        let dir = tempdir().unwrap();
        let p = dir.path().join("person.vertex.yml");
        VertexInfo::new("person", 100)
            .with_property_group(PropertyGroup::new(
                vec![Property::new("id", DataType::Int64).primary()],
                FileType::Parquet,
            ))
            .save(&p)
            .unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        let expected = "type: person
chunk_size: 100
prefix: vertex/person/
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
        is_nullable: false
    file_type: parquet
version: gar/v1
";
        assert_eq!(text, expected);
        assert_eq!(VertexInfo::load(&p).unwrap().property_groups.len(), 1);

        let g = dir.path().join("g.graph.yml");
        crate::metadata::GraphInfo::new("g", "")
            .with_vertex("person.vertex.yml")
            .save(&g)
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&g).unwrap(),
            "name: g\nvertices:\n  - person.vertex.yml\nversion: gar/v1\n"
        );
    }

    #[test]
    fn empty_graph_prefix_is_not_written() {
        // The C++ YAML reader rejects `prefix: ''`; an empty prefix is
        // omitted, which upstream reads as "relative to the info file".
        let dir = tempdir().unwrap();
        let p = dir.path().join("g.graph.yml");
        crate::metadata::GraphInfo::new("g", "")
            .with_vertex("v.vertex.yml")
            .save(&p)
            .unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(!text.contains("prefix"), "{text}");
        assert!(!text.contains("''"), "{text}");
        assert_eq!(crate::metadata::GraphInfo::load(&p).unwrap().prefix, "");
    }

    #[test]
    fn chunk_paths_are_extensionless() {
        let vi = person(10);
        assert_eq!(
            vi.chunk_path(&vi.property_groups[1], 3).to_str().unwrap(),
            "vertex/person/name/chunk3"
        );
        let ei = knows(10, 4, true);
        let t = AdjListType::OrderedBySource;
        assert_eq!(
            ei.adj_list_chunk_path(&t, 2, 1).unwrap().to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1"
        );
        assert_eq!(
            ei.offset_chunk_path(&t, 2).unwrap().to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/offset/chunk2"
        );
    }

    #[test]
    fn count_file_paths_match_upstream() {
        let vi = person(10);
        assert_eq!(
            vi.vertex_count_path().to_str().unwrap(),
            "vertex/person/vertex_count"
        );
        let ei = knows(10, 4, true);
        let t = AdjListType::OrderedBySource;
        assert_eq!(
            ei.vertex_count_path(&t).to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/vertex_count"
        );
        assert_eq!(
            ei.edge_count_path(&t, 7).to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/edge_count7"
        );
    }

    #[test]
    fn vertex_dump_writes_index_column_and_count() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let vi = dump_people(base, 25, 10);

        // Count file: 8 bytes, little-endian int64, as upstream writes it.
        let raw = std::fs::read(base.join("vertex/person/vertex_count")).unwrap();
        assert_eq!(raw, 25i64.to_le_bytes().to_vec());
        assert_eq!(
            io::read_count(base.join(vi.vertex_count_path())).unwrap(),
            25
        );

        // Every group's chunk carries the absolute _graphArVertexIndex.
        let chunk2 =
            io::read_chunk(base.join("vertex/person/name/chunk2"), &FileType::Parquet).unwrap();
        assert_eq!(
            concat_i64(&chunk2, VERTEX_INDEX_COL),
            (20..25).collect::<Vec<_>>()
        );

        // open() takes the count from the file; read_merged keeps one index column.
        let vr = VertexReader::open(vi, base).unwrap();
        assert_eq!(vr.vertex_count(), 25);
        let merged = vr.read_merged().unwrap();
        assert_eq!(merged.num_rows(), 25);
        let names: Vec<_> = merged
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec![VERTEX_INDEX_COL, "id", "name"]);
    }

    #[test]
    fn edge_dump_uses_reserved_column_names_and_counts() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let ei = knows(10, 4, true);
        let t = AdjListType::OrderedBySource;
        let mut eb = EdgesBuilder::new(ei.clone(), base, t.clone(), 25);
        for i in 0..24i64 {
            eb.add_edge(Edge::new(i, (i * 7) % 25));
        }
        eb.dump().unwrap();

        let adj = io::read_chunk(
            base.join("edge/person_knows_person/ordered_by_source/adj_list/part0/chunk0"),
            &FileType::Parquet,
        )
        .unwrap();
        let schema = adj[0].schema();
        assert_eq!(schema.field(0).name(), SRC_INDEX_COL);
        assert_eq!(schema.field(1).name(), DST_INDEX_COL);
        let off = io::read_chunk(
            base.join("edge/person_knows_person/ordered_by_source/offset/chunk0"),
            &FileType::Parquet,
        )
        .unwrap();
        assert_eq!(off[0].schema().field(0).name(), OFFSET_COL);

        let root = base.join("edge/person_knows_person/ordered_by_source");
        assert_eq!(io::read_count(root.join("vertex_count")).unwrap(), 25);
        let per_part: Vec<u64> = (0..3)
            .map(|p| io::read_count(root.join(format!("edge_count{p}"))).unwrap())
            .collect();
        assert_eq!(per_part, vec![10, 10, 4]);

        let er = EdgeReader::open(ei, base, &t).unwrap();
        let (offsets, indices) = er.read_csr(&t).unwrap();
        assert_eq!(offsets.len(), 26);
        assert_eq!(*offsets.last().unwrap(), 24);
        for v in 0..24usize {
            let row = &indices[offsets[v] as usize..offsets[v + 1] as usize];
            assert_eq!(row, &[((v as i64) * 7) % 25]);
        }
    }

    #[test]
    fn padded_offset_chunk_is_read() {
        // The GraphAr C++ table writer (EdgeChunkWriter::SortAndWriteAdjListTable)
        // pads the last vertex chunk's offsets to chunk_size + 1 rows; the
        // extra rows repeat the final offset.
        let dir = tempdir().unwrap();
        let base = dir.path();
        let ei = knows(10, 4, true);
        let t = AdjListType::OrderedBySource;
        let mut eb = EdgesBuilder::new(ei.clone(), base, t.clone(), 25);
        for i in 0..24i64 {
            eb.add_edge(Edge::new(i, (i * 7) % 25));
        }
        eb.dump().unwrap();
        let path = base.join(ei.offset_chunk_path(&t, 2).unwrap());
        let mut off = concat_i64(
            &io::read_chunk(&path, &FileType::Parquet).unwrap(),
            OFFSET_COL,
        );
        assert_eq!(off.len(), 6);
        let last = *off.last().unwrap();
        off.resize(11, last);
        let schema = Arc::new(Schema::new(vec![Field::new(
            OFFSET_COL,
            ArrowDataType::Int64,
            false,
        )]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(off))]).unwrap();
        io::write_chunk(&path, &[b], &FileType::Parquet).unwrap();

        let (offsets, indices) = EdgeReader::open(ei, base, &t)
            .unwrap()
            .read_csr(&t)
            .unwrap();
        assert_eq!(offsets.len(), 26);
        assert_eq!(indices.len(), 24);
    }

    #[test]
    fn legacy_extension_chunks_are_still_read() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let vi = dump_people(base, 5, 10);
        let spec = base.join("vertex/person/id/chunk0");
        std::fs::rename(&spec, base.join("vertex/person/id/chunk0.parquet")).unwrap();
        let vr = VertexReader::open(vi, base).unwrap();
        let ids = concat_i64(&vr.read_by_property("id").unwrap(), "id");
        assert_eq!(ids, vec![1000, 1001, 1002, 1003, 1004]);
    }

    #[test]
    fn dumped_layout_validates_strictly() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let vi = dump_people(base, 25, 10);
        let ei = knows(10, 4, true);
        let mut eb = EdgesBuilder::new(ei.clone(), base, AdjListType::OrderedBySource, 25);
        for i in 0..24i64 {
            eb.add_edge(Edge::new(i, i + 1));
        }
        eb.dump().unwrap();
        let opts = ValidateOptions::default();
        let mut issues = validate_vertex_info(&vi, base, &opts);
        issues.extend(validate_edge_info(&ei, base, &opts));
        let errs: Vec<_> = issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .map(|i| i.to_string())
            .collect();
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn table_writers_match_row_builders() {
        // The table writers take Arrow columns directly; their output must be
        // the same files the row builders write.
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let vi = dump_people(a.path(), 25, 10);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
        ]));
        let table = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    (0..25).map(|i| 1000 + i).collect::<Vec<i64>>(),
                )),
                Arc::new(StringArray::from(
                    (0..25).map(|i| format!("p{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        assert_eq!(write_vertex_table(&vi, b.path(), &table).unwrap(), 25);

        let ei = knows(10, 4, true);
        let t = AdjListType::OrderedBySource;
        let mut eb = EdgesBuilder::new(ei.clone(), a.path(), t.clone(), 25);
        let (mut s, mut d) = (Vec::new(), Vec::new());
        // Deliberately unsorted input.
        for i in (0..24i64).rev() {
            eb.add_edge(Edge::new(i, (i * 7) % 25));
            s.push(i);
            d.push((i * 7) % 25);
        }
        eb.dump().unwrap();
        let eschema = Arc::new(Schema::new(vec![
            Field::new(SRC_INDEX_COL, ArrowDataType::Int64, false),
            Field::new(DST_INDEX_COL, ArrowDataType::Int64, false),
        ]));
        let edges = RecordBatch::try_new(
            eschema,
            vec![Arc::new(Int64Array::from(s)), Arc::new(Int64Array::from(d))],
        )
        .unwrap();
        assert_eq!(write_edge_table(&ei, b.path(), &t, &edges, 25).unwrap(), 24);

        for rel in [
            "vertex/person/vertex_count",
            "vertex/person/id/chunk0",
            "vertex/person/name/chunk2",
            "edge/person_knows_person/ordered_by_source/edge_count2",
            "edge/person_knows_person/ordered_by_source/offset/chunk1",
            "edge/person_knows_person/ordered_by_source/adj_list/part1/chunk0",
        ] {
            let ft = FileType::Parquet;
            if rel.contains("count") {
                assert_eq!(
                    std::fs::read(a.path().join(rel)).unwrap(),
                    std::fs::read(b.path().join(rel)).unwrap(),
                    "{rel}"
                );
            } else {
                let x = io::read_chunk(a.path().join(rel), &ft).unwrap();
                let y = io::read_chunk(b.path().join(rel), &ft).unwrap();
                assert_eq!(x, y, "{rel}");
            }
        }
    }

    // ---- upstream `testing` data ----

    fn testdata() -> Option<std::path::PathBuf> {
        crate::tests::upstream_testing_dir()
    }

    /// Read ldbc_sample in one format: vertex count from the count file, the
    /// merged vertex table, and the ordered_by_source CSR, checked against the
    /// ordered_by_dest CSR of the same edges.
    fn check_ldbc_sample(fmt: &str) {
        let Some(root) = testdata() else {
            eprintln!("upstream testing data not found; skipping");
            return;
        };
        let base = root.join("ldbc_sample").join(fmt);
        let vi = VertexInfo::load(base.join("person.vertex.yml")).unwrap();
        let ei = EdgeInfo::load(base.join("person_knows_person.edge.yml")).unwrap();

        let vr = VertexReader::open(vi, &base).unwrap();
        assert_eq!(vr.vertex_count(), 903);
        let merged = vr.read_merged().unwrap();
        assert_eq!(merged.num_rows(), 903);
        assert!(merged.column_by_name("firstName").is_some());

        let t = AdjListType::OrderedBySource;
        let er = EdgeReader::open(ei.clone(), &base, &t).unwrap();
        let (offsets, indices) = er.read_csr(&t).unwrap();
        assert_eq!(offsets.len(), 904);
        assert_eq!(indices.len(), 6626);
        assert_eq!(*offsets.last().unwrap() as usize, indices.len());

        // Same edge multiset as the ordered_by_dest list (CSC of the same graph).
        let d = AdjListType::OrderedByDest;
        let dr = EdgeReader::open(ei, &base, &d).unwrap();
        let (doff, didx) = dr.read_csr(&d).unwrap();
        let mut pairs = Vec::new();
        for v in 0..doff.len() - 1 {
            for &s in &didx[doff[v] as usize..doff[v + 1] as usize] {
                pairs.push((s, v as i64));
            }
        }
        pairs.sort_unstable();
        let mut csr_pairs = Vec::new();
        for v in 0..903usize {
            for &d in &indices[offsets[v] as usize..offsets[v + 1] as usize] {
                csr_pairs.push((v as i64, d));
            }
        }
        csr_pairs.sort_unstable();
        assert_eq!(pairs, csr_pairs);
    }

    #[test]
    fn upstream_ldbc_sample_parquet() {
        check_ldbc_sample("parquet");
    }

    #[test]
    fn upstream_ldbc_sample_csv() {
        check_ldbc_sample("csv");
    }

    #[test]
    fn upstream_csv_properties_keep_declared_types() {
        // CSV carries no types; a string property that looks like a
        // timestamp must come back as the declared `string`.
        let Some(root) = testdata() else {
            return;
        };
        let base = root.join("ldbc_sample/csv");
        let ei = EdgeInfo::load(base.join("person_knows_person.edge.yml")).unwrap();
        let t = AdjListType::OrderedBySource;
        let er = EdgeReader::open(ei.clone(), &base, &t).unwrap();
        let group = ei.property_groups[0].clone();
        let batches = er.read_property_group(&t, &group, 0).unwrap();
        let field = batches[0].schema().field_with_name("creationDate").unwrap().clone();
        assert_eq!(field.data_type(), &ArrowDataType::Utf8);

        let vi = VertexInfo::load(base.join("person.vertex.yml")).unwrap();
        let merged = VertexReader::open(vi, &base).unwrap().read_merged().unwrap();
        assert_eq!(
            merged.schema().field_with_name("id").unwrap().data_type(),
            &ArrowDataType::Int64
        );
    }

    #[cfg(feature = "orc")]
    #[test]
    fn upstream_ldbc_sample_orc() {
        check_ldbc_sample("orc");
    }

    #[test]
    fn upstream_ldbc_sample_validates_strictly() {
        let Some(root) = testdata() else {
            return;
        };
        for fmt in ["parquet", "csv", "orc"] {
            let issues = crate::validate::validate_graph_dir(
                root.join("ldbc_sample")
                    .join(fmt)
                    .join("ldbc_sample.graph.yml"),
            );
            let errs: Vec<_> = issues
                .iter()
                .filter(|i| i.severity == Severity::Error)
                .map(|i| i.to_string())
                .collect();
            assert!(errs.is_empty(), "{fmt}: {errs:?}");
        }
    }

    #[test]
    fn upstream_offsets_column_is_named() {
        let Some(root) = testdata() else {
            return;
        };
        let p = root
            .join("ldbc_sample/parquet/edge/person_knows_person/ordered_by_source/offset/chunk0");
        let b = io::read_chunk(p, &FileType::Parquet).unwrap();
        assert_eq!(b[0].schema().field(0).name(), OFFSET_COL);
        assert_eq!(b[0].column(0).len(), 101);
    }
}
