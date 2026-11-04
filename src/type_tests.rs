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

//! The `gar/v1` value types: `list<T>`, `date` and `timestamp`, through the
//! info files, the row builders, the table writers and every chunk format.

#[cfg(test)]
mod value_types {
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
        ListArray, RecordBatch, StringArray, TimestampMillisecondArray,
        cast::AsArray,
        types::{Float32Type, Int32Type, Int64Type},
    };
    use arrow_schema::{DataType as ArrowDataType, Field, Schema, TimeUnit};
    use tempfile::tempdir;

    use crate::{
        AdjListInfo, AdjListType, DataType, Edge, EdgeInfo, EdgeReader, EdgesBuilder, FileType,
        GraphInfo, Property, PropertyGroup, PropertyValue, Vertex, VertexInfo, VertexReader,
        VerticesBuilder, io, write_vertex_table,
    };

    const DAY: i32 = 18_000;
    const MS: i64 = 1_555_000_000_123;

    fn list_of(t: DataType) -> DataType {
        DataType::List(Box::new(t))
    }

    #[test]
    fn data_type_names_round_trip() {
        for (name, t) in [
            ("bool", DataType::Bool),
            ("int32", DataType::Int32),
            ("int64", DataType::Int64),
            ("float", DataType::Float),
            ("double", DataType::Double),
            ("string", DataType::String),
            ("date", DataType::Date),
            ("timestamp", DataType::Timestamp),
            ("list<int32>", list_of(DataType::Int32)),
            ("list<int64>", list_of(DataType::Int64)),
            ("list<float>", list_of(DataType::Float)),
            ("list<double>", list_of(DataType::Double)),
            ("list<string>", list_of(DataType::String)),
        ] {
            let parsed: DataType = serde_yaml::from_str(name).unwrap();
            assert_eq!(parsed, t, "{name}");
            let text = serde_yaml::to_string(&t).unwrap();
            assert_eq!(text.trim().trim_matches('\'').trim_matches('"'), name);
        }
    }

    #[test]
    fn bare_list_reads_as_list_of_string_and_bad_names_fail() {
        let t: DataType = serde_yaml::from_str("list").unwrap();
        assert_eq!(t, list_of(DataType::String));
        for bad in ["list<list<int32>>", "list<>", "list<nope>", "nope"] {
            assert!(serde_yaml::from_str::<DataType>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn info_file_keeps_the_angle_brackets_unquoted() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("v.vertex.yml");
        VertexInfo::new("v", 10)
            .with_property_group(PropertyGroup::new(
                vec![Property::new("f", list_of(DataType::Float))],
                FileType::Parquet,
            ))
            .save(&p)
            .unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(text.contains("data_type: list<float>\n"), "{text}");
        let back = VertexInfo::load(&p).unwrap();
        assert_eq!(
            back.property_groups[0].properties[0].data_type,
            list_of(DataType::Float)
        );
    }

    #[test]
    fn arrow_types_match_the_spec() {
        let t = |d: DataType| d.to_arrow("x", true).data_type().clone();
        assert_eq!(t(DataType::Date), ArrowDataType::Date32);
        assert_eq!(
            t(DataType::Timestamp),
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            t(list_of(DataType::Float)),
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float32, true)))
        );
    }

    fn all_properties() -> Vec<Property> {
        vec![
            Property::new("b", DataType::Bool),
            Property::new("i", DataType::Int32),
            Property::new("l", DataType::Int64),
            Property::new("f", DataType::Float),
            Property::new("d", DataType::Double),
            Property::new("s", DataType::String),
            Property::new("day", DataType::Date),
            Property::new("ts", DataType::Timestamp),
        ]
    }

    fn list_properties() -> Vec<Property> {
        vec![
            Property::new("li", list_of(DataType::Int32)),
            Property::new("ll", list_of(DataType::Int64)),
            Property::new("lf", list_of(DataType::Float)),
            Property::new("ld", list_of(DataType::Double)),
            Property::new("ls", list_of(DataType::String)),
        ]
    }

    fn scalar_values(i: i64) -> Vec<(&'static str, PropertyValue)> {
        vec![
            ("b", PropertyValue::Bool(i % 2 == 0)),
            ("i", PropertyValue::Int32(i as i32)),
            ("l", PropertyValue::Int64(i * 1000)),
            ("f", PropertyValue::Float(i as f32 + 0.5)),
            ("d", PropertyValue::Double(i as f64 + 0.25)),
            ("s", PropertyValue::String(format!("s{i}"))),
            ("day", PropertyValue::Date(DAY + i as i32)),
            ("ts", PropertyValue::Timestamp(MS + i)),
        ]
    }

    fn list_values(i: i64) -> Vec<(&'static str, PropertyValue)> {
        let n = (i % 3) as usize; // 0, 1 or 2 items
        let many = |f: &dyn Fn(usize) -> PropertyValue| (0..n).map(f).collect::<Vec<_>>();
        vec![
            (
                "li",
                PropertyValue::List(many(&|k| PropertyValue::Int32(k as i32 + 1))),
            ),
            (
                "ll",
                PropertyValue::List(many(&|k| PropertyValue::Int64(k as i64 + 7))),
            ),
            (
                "lf",
                PropertyValue::List(many(&|k| PropertyValue::Float(k as f32 + 0.5))),
            ),
            (
                "ld",
                PropertyValue::List(many(&|k| PropertyValue::Double(k as f64 + 0.25))),
            ),
            (
                "ls",
                PropertyValue::List(many(&|k| PropertyValue::String(format!("x{k}")))),
            ),
        ]
    }

    fn vertex_info(ft: FileType, lists: bool) -> VertexInfo {
        let mut vi = VertexInfo::new("v", 4)
            .with_property_group(PropertyGroup::new(all_properties(), ft.clone()));
        if lists {
            vi = vi.with_property_group(PropertyGroup::new(list_properties(), ft));
        }
        vi
    }

    fn check_scalars(b: &RecordBatch, n: usize) {
        assert_eq!(b.num_rows(), n);
        let c = |name: &str| b.column_by_name(name).unwrap().clone();
        let i = c("i");
        let day = c("day");
        let ts = c("ts");
        let i = i.as_any().downcast_ref::<Int32Array>().expect("int32");
        let day = day.as_any().downcast_ref::<Date32Array>().expect("date32");
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp[ms]");
        for r in 0..n {
            assert_eq!(i.value(r), r as i32);
            assert_eq!(day.value(r), DAY + r as i32);
            assert_eq!(ts.value(r), MS + r as i64);
        }
        let d = c("d");
        let d = d.as_any().downcast_ref::<Float64Array>().expect("double");
        assert_eq!(d.value(2), 2.25);
        let f = c("f");
        let f = f.as_any().downcast_ref::<Float32Array>().expect("float");
        assert_eq!(f.value(1), 1.5);
        let bo = c("b");
        assert!(
            bo.as_any()
                .downcast_ref::<BooleanArray>()
                .expect("bool")
                .value(0)
        );
    }

    fn check_lists(b: &RecordBatch) {
        let li = b.column_by_name("li").unwrap().as_list::<i32>().clone();
        assert_eq!(li.value(0).len(), 0, "empty list stays empty");
        assert!(!li.is_null(0));
        let row2 = li.value(2);
        assert_eq!(row2.as_primitive::<Int32Type>().values(), &[1, 2]);
        let ll = b.column_by_name("ll").unwrap().as_list::<i32>().clone();
        assert_eq!(ll.value(2).as_primitive::<Int64Type>().values(), &[7, 8]);
        let lf = b.column_by_name("lf").unwrap().as_list::<i32>().clone();
        assert_eq!(lf.value(1).as_primitive::<Float32Type>().values(), &[0.5]);
        let ls = b.column_by_name("ls").unwrap().as_list::<i32>().clone();
        let r = ls.value(2);
        let r = r.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((r.value(0), r.value(1)), ("x0", "x1"));
    }

    fn dump(vi: &VertexInfo, base: &std::path::Path, n: i64, lists: bool) {
        let mut vb = VerticesBuilder::new(vi.clone(), base);
        for i in 0..n {
            let mut v = Vertex::new();
            for (k, val) in scalar_values(i) {
                v = v.add_property(k, val);
            }
            if lists {
                for (k, val) in list_values(i) {
                    v = v.add_property(k, val);
                }
            }
            vb.add_vertex(v);
        }
        vb.dump().unwrap();
    }

    #[test]
    fn row_builder_round_trips_every_type_in_parquet_and_json() {
        for ft in [FileType::Parquet, FileType::Json] {
            let dir = tempdir().unwrap();
            let vi = vertex_info(ft.clone(), true);
            dump(&vi, dir.path(), 10, true);
            let b = VertexReader::open(vi, dir.path())
                .unwrap()
                .read_merged()
                .unwrap();
            check_scalars(&b, 10);
            check_lists(&b);
        }
    }

    #[test]
    fn row_builder_round_trips_scalars_in_csv() {
        let dir = tempdir().unwrap();
        let vi = vertex_info(FileType::Csv, false);
        dump(&vi, dir.path(), 10, false);
        let b = VertexReader::open(vi, dir.path())
            .unwrap()
            .read_merged()
            .unwrap();
        check_scalars(&b, 10);
    }

    #[cfg(feature = "orc")]
    #[test]
    fn row_builder_round_trips_scalars_in_orc() {
        let dir = tempdir().unwrap();
        let vi = vertex_info(FileType::Orc, false);
        dump(&vi, dir.path(), 10, false);
        let b = VertexReader::open(vi, dir.path())
            .unwrap()
            .read_merged()
            .unwrap();
        check_scalars(&b, 10);
    }

    #[test]
    fn a_value_of_the_wrong_type_is_null() {
        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("v", 4).with_property_group(PropertyGroup::new(
            vec![
                Property::new("day", DataType::Date),
                Property::new("lf", list_of(DataType::Float)),
            ],
            FileType::Parquet,
        ));
        let mut vb = VerticesBuilder::new(vi.clone(), dir.path());
        vb.add_vertex(
            Vertex::new()
                .add_property("day", PropertyValue::String("2020-01-01".into()))
                .add_property("lf", PropertyValue::List(vec![PropertyValue::Int32(1)])),
        );
        vb.dump().unwrap();
        let b = VertexReader::open(vi, dir.path())
            .unwrap()
            .read_merged()
            .unwrap();
        assert!(b.column_by_name("day").unwrap().is_null(0));
        let lf = b.column_by_name("lf").unwrap().as_list::<i32>().clone();
        assert!(lf.value(0).is_null(0), "list item of the wrong type");
    }

    #[test]
    fn edge_builder_writes_date_timestamp_and_list_properties() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let group = PropertyGroup::new(
            vec![
                Property::new("day", DataType::Date),
                Property::new("ts", DataType::Timestamp),
                Property::new("w", list_of(DataType::Double)),
            ],
            FileType::Parquet,
        );
        let ei = EdgeInfo::new("v", "e", "v", 8, 4, 4, true)
            .with_adj_list(AdjListInfo {
                ordered: true,
                aligned_by: "src".into(),
                file_type: FileType::Parquet,
                prefix: None,
            })
            .with_property_group(group.clone());
        let t = AdjListType::OrderedBySource;
        let mut eb = EdgesBuilder::new(ei.clone(), base, t.clone(), 3);
        for i in (0..3i64).rev() {
            eb.add_edge(
                Edge::new(i, (i + 1) % 3)
                    .add_property("day", PropertyValue::Date(DAY + i as i32))
                    .add_property("ts", PropertyValue::Timestamp(MS + i))
                    .add_property(
                        "w",
                        PropertyValue::List(vec![PropertyValue::Double(i as f64)]),
                    ),
            );
        }
        eb.dump().unwrap();
        let er = EdgeReader::open(ei, base, &t).unwrap();
        let b = &er.read_property_group(&t, &group, 0).unwrap()[0];
        let day = b.column_by_name("day").unwrap().clone();
        let day = day.as_any().downcast_ref::<Date32Array>().expect("date32");
        assert_eq!(day.values(), &[DAY, DAY + 1, DAY + 2]);
        let ts = b.column_by_name("ts").unwrap().clone();
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp[ms]");
        assert_eq!(ts.value(2), MS + 2);
        assert_eq!(b.column_by_name("w").unwrap().as_list::<i32>().len(), 3);
    }

    #[test]
    fn table_writer_takes_list_columns() {
        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("v", 2).with_property_group(PropertyGroup::new(
            vec![Property::new("lf", list_of(DataType::Float))],
            FileType::Parquet,
        ));
        // A parquet-style `element` field name: cast to the declared `item`.
        let lf: ArrayRef = {
            let item = Arc::new(Field::new("element", ArrowDataType::Float32, true));
            Arc::new(ListArray::new(
                item.clone(),
                arrow::buffer::OffsetBuffer::from_lengths([1, 2, 0]),
                Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
                None,
            ))
        };
        let table = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "lf",
                lf.data_type().clone(),
                true,
            )])),
            vec![lf],
        )
        .unwrap();
        assert_eq!(write_vertex_table(&vi, dir.path(), &table).unwrap(), 3);
        let b = VertexReader::open(vi, dir.path())
            .unwrap()
            .read_merged()
            .unwrap();
        let col = b.column_by_name("lf").unwrap().as_list::<i32>().clone();
        assert_eq!(
            col.value(1).as_primitive::<Float32Type>().values(),
            &[2.0, 3.0]
        );
    }

    #[test]
    fn lists_in_csv_are_an_error_not_a_panic() {
        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("v", 4)
            .with_property_group(PropertyGroup::new(list_properties(), FileType::Csv));
        let mut vb = VerticesBuilder::new(vi, dir.path());
        vb.add_vertex(Vertex::new());
        assert!(vb.dump().is_err());
    }

    #[cfg(feature = "orc")]
    #[test]
    fn lists_in_orc_are_an_error_not_a_panic() {
        // orc-rust 0.9's writer has no list encoder (`unimplemented!`).
        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("v", 4)
            .with_property_group(PropertyGroup::new(list_properties(), FileType::Orc));
        let mut vb = VerticesBuilder::new(vi, dir.path());
        vb.add_vertex(Vertex::new());
        let err = vb.dump().unwrap_err().to_string();
        assert!(err.contains("ORC") && err.contains("list"), "{err}");
    }

    #[test]
    fn upstream_with_feature_reads_in_full() {
        let Some(root) = crate::tests::upstream_testing_dir() else {
            return;
        };
        let base = root.join("ldbc_sample/parquet");
        let gi = GraphInfo::load(base.join("ldbc_sample_with_feature.graph.yml")).unwrap();
        let vi = gi.all_vertex_infos(&base).unwrap().remove(0);
        let ty = &vi.find_property_group("feature").unwrap().properties[0].data_type;
        assert_eq!(ty, &list_of(DataType::Float));
        let b = VertexReader::open(vi, &base)
            .unwrap()
            .read_merged()
            .unwrap();
        assert_eq!(b.num_rows(), 903);
        let f = b
            .column_by_name("feature")
            .unwrap()
            .as_list::<i32>()
            .clone();
        assert!(!f.value(0).is_empty());
        assert_eq!(f.value(0).data_type(), &ArrowDataType::Float32);
    }

    #[test]
    fn upstream_with_feature_survives_a_rewrite() {
        let Some(root) = crate::tests::upstream_testing_dir() else {
            return;
        };
        let base = root.join("ldbc_sample/parquet");
        let gi = GraphInfo::load(base.join("ldbc_sample_with_feature.graph.yml")).unwrap();
        let vi = gi.all_vertex_infos(&base).unwrap().remove(0);
        let b = VertexReader::open(vi.clone(), &base)
            .unwrap()
            .read_merged()
            .unwrap();
        let dir = tempdir().unwrap();
        write_vertex_table(&vi, dir.path(), &b).unwrap();
        let back = VertexReader::open(vi, dir.path())
            .unwrap()
            .read_merged()
            .unwrap();
        let f = |x: &RecordBatch| {
            x.column_by_name("feature")
                .unwrap()
                .as_list::<i32>()
                .clone()
        };
        for r in [0, 450, 902] {
            assert_eq!(
                f(&b).value(r).as_primitive::<Float32Type>().values(),
                f(&back).value(r).as_primitive::<Float32Type>().values()
            );
        }
        let path = dir.path().join("vertex/person/feature/chunk0");
        assert!(!io::read_chunk(path, &FileType::Parquet).unwrap().is_empty());
    }

    #[cfg(feature = "orc")]
    #[test]
    fn orc_chunks_are_compressed() {
        // Repetitive strings: without compression this chunk is about 1 MB.
        let dir = tempdir().unwrap();
        let path = dir.path().join("chunk0");
        let s = StringArray::from_iter_values(
            (0..20_000).map(|i| format!("rust-analyzer cargo some-crate 1.2.3 module/item#{i}")),
        );
        let raw: usize = (0..s.len()).map(|i| s.value(i).len()).sum();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Utf8,
            false,
        )]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(s)]).unwrap();
        io::write_chunk(&path, std::slice::from_ref(&b), &FileType::Orc).unwrap();
        let size = std::fs::metadata(&path).unwrap().len() as usize;
        assert!(size * 5 < raw, "{size} bytes for {raw} raw");
        let back = io::read_chunk(&path, &FileType::Orc).unwrap();
        assert_eq!(back.iter().map(|b| b.num_rows()).sum::<usize>(), 20_000);
    }
}
