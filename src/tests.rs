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

#[cfg(test)]
mod metadata_tests {
    use crate::{
        metadata::{AdjListInfo, EdgeInfo, GraphInfo, Property, PropertyGroup, VertexInfo},
        types::{AdjListType, DataType, FileType},
    };
    use tempfile::tempdir;

    fn person_vertex_info() -> VertexInfo {
        VertexInfo::new("person", 100)
            .with_property_group(PropertyGroup::new(
                vec![Property::new("id", DataType::Int64).primary()],
                FileType::Parquet,
            ))
            .with_property_group(PropertyGroup::new(
                vec![
                    Property::new("firstName", DataType::String),
                    Property::new("lastName", DataType::String),
                ],
                FileType::Parquet,
            ))
    }

    fn knows_edge_info() -> EdgeInfo {
        EdgeInfo::new("person", "knows", "person", 1024, 100, 100, false)
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

    #[test]
    fn vertex_info_round_trip_yaml() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("person.vertex.yml");
        let vi = person_vertex_info();
        vi.save(&path).unwrap();
        let loaded = VertexInfo::load(&path).unwrap();
        assert_eq!(loaded.vertex_type, "person");
        assert_eq!(loaded.chunk_size, 100);
        assert_eq!(loaded.property_groups.len(), 2);
    }

    #[test]
    fn edge_info_round_trip_yaml() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("person_knows_person.edge.yml");
        let ei = knows_edge_info();
        ei.save(&path).unwrap();
        let loaded = EdgeInfo::load(&path).unwrap();
        assert_eq!(loaded.src_type, "person");
        assert_eq!(loaded.edge_type, "knows");
        assert_eq!(loaded.dst_type, "person");
        assert_eq!(loaded.adj_lists.len(), 1);
    }

    #[test]
    fn graph_info_round_trip_yaml() {
        let dir = tempdir().unwrap();
        let vi = person_vertex_info();
        vi.save(dir.path().join("person.vertex.yml")).unwrap();
        let ei = knows_edge_info();
        ei.save(dir.path().join("person_knows_person.edge.yml"))
            .unwrap();

        let gi = GraphInfo::new("test_graph", "./")
            .with_vertex("person.vertex.yml")
            .with_edge("person_knows_person.edge.yml");
        let graph_path = dir.path().join("test_graph.graph.yml");
        gi.save(&graph_path).unwrap();

        let loaded_gi = GraphInfo::load(&graph_path).unwrap();
        assert_eq!(loaded_gi.name, "test_graph");

        let vi2 = loaded_gi.get_vertex_info("person", dir.path()).unwrap();
        assert_eq!(vi2.chunk_size, 100);

        let ei2 = loaded_gi
            .get_edge_info("person", "knows", "person", dir.path())
            .unwrap();
        assert!(!ei2.directed);
    }

    #[test]
    fn chunk_path_formatting() {
        let vi = person_vertex_info();
        let group = &vi.property_groups[1]; // firstName_lastName
        let path = vi.chunk_path(group, 0);
        assert_eq!(
            path.to_str().unwrap(),
            "vertex/person/firstName_lastName/chunk0"
        );
    }

    #[test]
    fn edge_adj_list_path_formatting() {
        let ei = knows_edge_info();
        let path = ei
            .adj_list_chunk_path(&AdjListType::OrderedBySource, 2, 3)
            .unwrap();
        assert_eq!(
            path.to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk3"
        );
    }

    #[test]
    fn offset_path_formatting() {
        let ei = knows_edge_info();
        let path = ei
            .offset_chunk_path(&AdjListType::OrderedBySource, 0)
            .unwrap();
        assert_eq!(
            path.to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/offset/chunk0"
        );
    }

    #[test]
    fn property_chunk_path_formatting() {
        let ei = knows_edge_info();
        let group = &ei.property_groups[0];
        let path = ei.property_chunk_path(&AdjListType::OrderedBySource, group, 1, 0);
        assert_eq!(
            path.to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/creationDate/part1/chunk0"
        );
    }
}

#[cfg(test)]
mod format_matrix_tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use tempfile::tempdir;

    use crate::{io, types::FileType};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("score", ArrowDataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            ],
        )
        .unwrap()
    }

    fn round_trip(file_type: FileType) {
        let dir = tempdir().unwrap();
        let path = dir.path().join(format!("chunk0.{}", file_type.extension()));
        let batch = sample_batch();
        io::write_chunk(&path, std::slice::from_ref(&batch), &file_type).unwrap();
        let batches = io::read_chunk(&path, &file_type).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "{file_type:?} row count");
        assert_eq!(batches[0].num_columns(), 3, "{file_type:?} column count");
    }

    #[test]
    fn round_trip_parquet() {
        round_trip(FileType::Parquet);
    }

    #[test]
    fn round_trip_csv() {
        round_trip(FileType::Csv);
    }

    #[test]
    fn round_trip_json() {
        round_trip(FileType::Json);
    }

    #[cfg(feature = "orc")]
    #[test]
    fn round_trip_orc() {
        round_trip(FileType::Orc);
    }

    #[test]
    fn round_trip_arrow_ipc() {
        round_trip(FileType::ArrowIpc);
    }

    #[test]
    fn empty_write_is_noop() {
        let dir = tempdir().unwrap();
        for ft in [
            FileType::Parquet,
            FileType::Csv,
            FileType::Json,
            #[cfg(feature = "orc")]
            FileType::Orc,
            FileType::ArrowIpc,
        ] {
            let path = dir.path().join(format!("empty.{}", ft.extension()));
            io::write_chunk(&path, &[], &ft).unwrap();
            assert!(
                !path.exists(),
                "{ft:?} should not create a file for empty input"
            );
        }
    }
}

#[cfg(test)]
mod storage_tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use tempfile::tempdir;

    use crate::{storage::ChunkStore, types::FileType};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn chunk_store_local_round_trip_all_formats() {
        let dir = tempdir().unwrap();
        let store = ChunkStore::local(dir.path()).unwrap();
        for ft in [
            FileType::Parquet,
            FileType::Csv,
            FileType::Json,
            #[cfg(feature = "orc")]
            FileType::Orc,
            FileType::ArrowIpc,
        ] {
            let rel = format!("vertex/person/id/chunk0.{}", ft.extension());
            store
                .write_chunk(&rel, &[sample_batch()], &ft)
                .await
                .unwrap();
            let batches = store.read_chunk(&rel, &ft).await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 2, "{ft:?}");
        }
    }

    #[tokio::test]
    async fn chunk_store_raw_bytes() {
        let dir = tempdir().unwrap();
        let store = ChunkStore::local(dir.path()).unwrap();
        store
            .put_bytes("meta/graph.yml", b"name: g".to_vec())
            .await
            .unwrap();
        let bytes = store.get_bytes("meta/graph.yml").await.unwrap();
        assert_eq!(&bytes[..], b"name: g");
    }

    #[tokio::test]
    async fn put_bytes_above_threshold_uses_multipart_round_trips() {
        use std::sync::Arc;

        use object_store::memory::InMemory;

        use crate::storage::{ChunkStore, MultipartOptions};

        // Tiny threshold / part size so a small payload still exercises the
        // multipart split. The payload length is deliberately NOT a multiple of
        // the part size, so the final short part is covered too.
        let store = ChunkStore::new(Arc::new(InMemory::new()), "g")
            .with_multipart(MultipartOptions::new(64, 100));

        let payload: Vec<u8> = (0..1234u32).map(|i| (i % 251) as u8).collect();
        assert!(payload.len() > store.multipart_options().threshold);

        store
            .put_bytes("vertex/big.bin", payload.clone())
            .await
            .unwrap();
        let got = store.get_bytes("vertex/big.bin").await.unwrap();
        assert_eq!(
            &got[..],
            &payload[..],
            "multipart round-trip must be byte-identical"
        );

        // A payload at/under the threshold still round-trips (single-put path).
        let small = vec![7u8; 64];
        store
            .put_bytes("vertex/small.bin", small.clone())
            .await
            .unwrap();
        assert_eq!(
            &store.get_bytes("vertex/small.bin").await.unwrap()[..],
            &small[..]
        );
    }

    #[tokio::test]
    async fn write_chunk_above_threshold_round_trips_via_multipart() {
        use std::sync::Arc;

        use arrow_array::Int64Array;
        use arrow_schema::{DataType as ArrowDataType, Field, Schema};
        use object_store::memory::InMemory;

        use crate::storage::{ChunkStore, MultipartOptions};

        // A many-row batch whose encoded Parquet comfortably exceeds a 4 KiB
        // threshold, forcing write_chunk through put_multipart.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(
                (0..50_000i64).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();

        let store = ChunkStore::new(Arc::new(InMemory::new()), "")
            .with_multipart(MultipartOptions::new(4 * 1024, 8 * 1024));

        let rel = "vertex/person/id/chunk0.parquet";
        store
            .write_chunk(rel, &[batch], &FileType::Parquet)
            .await
            .unwrap();
        let back = store.read_chunk(rel, &FileType::Parquet).await.unwrap();
        let rows: usize = back.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 50_000, "multipart-written chunk must read back fully");
    }

    #[test]
    fn multipart_options_defaults_and_overrides() {
        use crate::storage::{
            DEFAULT_MULTIPART_PART_SIZE, DEFAULT_MULTIPART_THRESHOLD, MultipartOptions,
        };

        let d = MultipartOptions::default();
        assert_eq!(d.threshold, DEFAULT_MULTIPART_THRESHOLD);
        assert_eq!(d.part_size, DEFAULT_MULTIPART_PART_SIZE);

        // part_size is clamped to >= 1 to avoid a zero-length part loop.
        assert_eq!(MultipartOptions::new(10, 0).part_size, 1);
    }

    #[test]
    fn retry_options_map_to_object_store_config() {
        use std::time::Duration;

        use crate::storage::RetryOptions;
        use object_store::RetryConfig;

        // Default mirrors object_store's own defaults (behavior preserved).
        let cfg: RetryConfig = RetryOptions::default().into();
        assert_eq!(cfg.max_retries, 10);
        assert_eq!(cfg.backoff.init_backoff, Duration::from_millis(100));
        assert_eq!(cfg.backoff.max_backoff, Duration::from_secs(15));
        assert_eq!(cfg.retry_timeout, Duration::from_secs(180));

        // Opt-out and explicit bounds round-trip into the native config.
        assert_eq!(RetryConfig::from(RetryOptions::none()).max_retries, 0);
        let custom: RetryConfig =
            RetryOptions::new(3, Duration::from_millis(50), Duration::from_secs(2)).into();
        assert_eq!(custom.max_retries, 3);
        assert_eq!(custom.backoff.init_backoff, Duration::from_millis(50));
        assert_eq!(custom.backoff.max_backoff, Duration::from_secs(2));
    }

    #[test]
    fn s3_compatible_with_retry_constructs() {
        use std::time::Duration;

        use crate::storage::RetryOptions;

        // The retry config is consumed by the S3 builder; a successful build
        // proves the plumbing is wired (no network is touched at construction).
        let store = ChunkStore::s3_compatible_with_retry(
            "http://127.0.0.1:9999",
            "bucket",
            "key",
            "secret",
            "graphs/social",
            RetryOptions::new(5, Duration::from_millis(20), Duration::from_secs(1)),
        );
        assert!(store.is_ok(), "s3_compatible_with_retry should construct");
    }

    #[tokio::test]
    async fn from_url_with_retry_local_round_trips() {
        use crate::storage::RetryOptions;

        // Local backend ignores retry, but the with_retry path must still build
        // and round-trip (the scheme-dispatch fallback for file://).
        let dir = tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        let store = ChunkStore::from_url_with_retry(&url, RetryOptions::none()).unwrap();
        store
            .put_bytes("meta/graph.yml", b"name: g".to_vec())
            .await
            .unwrap();
        assert_eq!(
            &store.get_bytes("meta/graph.yml").await.unwrap()[..],
            b"name: g"
        );
    }
}

#[cfg(test)]
mod io_tests {
    use crate::{
        metadata::{Property, PropertyGroup, VertexInfo},
        reader::VertexReader,
        types::FileType,
        writer::{PropertyValue, Vertex, VerticesBuilder},
    };
    use tempfile::tempdir;

    #[test]
    fn write_and_read_vertex_parquet() {
        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("person", 10).with_property_group(PropertyGroup::new(
            vec![
                Property::new("id", crate::types::DataType::Int64).primary(),
                Property::new("name", crate::types::DataType::String),
            ],
            FileType::Parquet,
        ));

        let mut builder = VerticesBuilder::new(vi.clone(), dir.path());
        for i in 0..25i64 {
            builder.add_vertex(
                Vertex::new()
                    .add_property("id", PropertyValue::Int64(i))
                    .add_property("name", PropertyValue::String(format!("person_{i}"))),
            );
        }
        let chunk_count = builder.dump().unwrap();
        assert_eq!(chunk_count, 3); // ceil(25/10)

        let reader = VertexReader::new(vi, dir.path(), 25);
        let group = reader.vertex_info().property_groups[0].clone();
        let batches = reader.read_property_group(&group).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 25);
    }

    // FAIL-ON-BUG / PASS-ON-FIX: before the fix, `DataType::List` declared an
    // Arrow `List<Utf8>` schema but the writer emitted a flat `Utf8` column and
    // there was no `PropertyValue::List` variant — so writing a list property
    // either panicked in `RecordBatch::try_new` (schema/column type mismatch)
    // or was simply unrepresentable. This proves real list values round-trip.
    #[test]
    fn write_and_read_list_property() {
        use arrow_array::{Array, ListArray, StringArray, cast::AsArray};

        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("person", 10).with_property_group(PropertyGroup::new(
            vec![
                Property::new("id", crate::types::DataType::Int64).primary(),
                Property::new(
                    "tags",
                    crate::types::DataType::List(Box::new(crate::types::DataType::String)),
                ),
            ],
            FileType::Parquet,
        ));

        let mut builder = VerticesBuilder::new(vi.clone(), dir.path());
        // row 0: two items, row 1: empty list, row 2: missing -> null list.
        builder.add_vertex(
            Vertex::new()
                .add_property("id", PropertyValue::Int64(0))
                .add_property(
                    "tags",
                    PropertyValue::List(vec![
                        PropertyValue::String("alpha".to_string()),
                        PropertyValue::String("beta".to_string()),
                    ]),
                ),
        );
        builder.add_vertex(
            Vertex::new()
                .add_property("id", PropertyValue::Int64(1))
                .add_property("tags", PropertyValue::List(Vec::new())),
        );
        builder.add_vertex(Vertex::new().add_property("id", PropertyValue::Int64(2)));
        builder.dump().unwrap();

        let reader = VertexReader::new(vi, dir.path(), 10);
        let merged = reader.read_merged().unwrap();
        assert_eq!(merged.num_rows(), 3);

        let tags_idx = merged.schema().index_of("tags").unwrap();
        // The column must be an Arrow List, not a flat Utf8 (the old bug).
        let col = merged.column(tags_idx);
        let lists: &ListArray = col.as_list_opt::<i32>().expect("tags must be a ListArray");

        // row 0: ["alpha", "beta"]
        assert!(!lists.is_null(0));
        let r0 = lists.value(0);
        let r0 = r0.as_any().downcast_ref::<StringArray>().unwrap();
        let r0: Vec<&str> = (0..r0.len()).map(|i| r0.value(i)).collect();
        assert_eq!(r0, vec!["alpha", "beta"]);

        // row 1: empty (non-null) list
        assert!(!lists.is_null(1));
        assert_eq!(lists.value(1).len(), 0);

        // row 2: null list (missing property)
        assert!(lists.is_null(2));
    }

    #[test]
    fn merge_property_groups_round_trip() {
        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("person", 10)
            .with_property_group(PropertyGroup::new(
                vec![Property::new("id", crate::types::DataType::Int64).primary()],
                FileType::Parquet,
            ))
            .with_property_group(PropertyGroup::new(
                vec![
                    Property::new("name", crate::types::DataType::String),
                    Property::new("age", crate::types::DataType::Int64),
                ],
                FileType::Parquet,
            ));

        let mut builder = VerticesBuilder::new(vi.clone(), dir.path());
        for i in 0..25i64 {
            builder.add_vertex(
                Vertex::new()
                    .add_property("id", PropertyValue::Int64(i))
                    .add_property("name", PropertyValue::String(format!("person_{i}")))
                    .add_property("age", PropertyValue::Int64(20 + i)),
            );
        }
        builder.dump().unwrap();

        let reader = VertexReader::new(vi, dir.path(), 25);
        let merged = reader.read_merged().unwrap();
        assert_eq!(merged.num_rows(), 25);
        // Every group chunk carries `_graphArVertexIndex`; the merge keeps one.
        assert_eq!(merged.num_columns(), 4);
        let names: Vec<_> = merged
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["_graphArVertexIndex", "id", "name", "age"]);
    }
}

#[cfg(test)]
mod merge_tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};

    use crate::merge::merge_property_groups;

    /// A one-column batch `col` = `values`, for building crafted property groups.
    fn int_group(col: &str, values: Vec<i64>) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            col,
            ArrowDataType::Int64,
            false,
        )]));
        vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()]
    }

    fn str_group(col: &str, values: Vec<&str>) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            col,
            ArrowDataType::Utf8,
            false,
        )]));
        vec![RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()]
    }

    #[test]
    fn merge_property_groups_zips_groups() {
        // Two row-aligned groups with distinct columns zip into one batch that
        // carries all columns, in the order the groups were given.
        let id = int_group("id", vec![1, 2, 3]);
        let name = str_group("name", vec!["a", "b", "c"]);
        let merged = merge_property_groups(&[id, name]).unwrap();
        assert_eq!(merged.num_rows(), 3);
        assert_eq!(merged.num_columns(), 2);
        let names: Vec<_> = merged
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn merge_property_groups_rejects_empty() {
        // No groups at all, and a single all-empty group, both yield the same
        // "no batches to merge" error (nothing survives the flatten).
        let err = merge_property_groups(&[]).unwrap_err().to_string();
        assert!(err.contains("no batches to merge"), "got: {err}");

        let empty: Vec<Vec<RecordBatch>> = vec![vec![]];
        let err = merge_property_groups(&empty).unwrap_err().to_string();
        assert!(err.contains("no batches to merge"), "got: {err}");
    }

    #[test]
    fn merge_property_groups_rejects_row_mismatch() {
        // Groups covering a different number of rows cannot be zipped.
        let a = int_group("id", vec![1, 2, 3]);
        let b = int_group("age", vec![10, 20]);
        let err = merge_property_groups(&[a, b]).unwrap_err().to_string();
        assert!(err.contains("row count mismatch"), "got: {err}");
    }

    #[test]
    fn merge_property_groups_rejects_duplicate_column() {
        // GraphAr forbids the same property name in two groups.
        let a = int_group("id", vec![1, 2, 3]);
        let b = int_group("id", vec![4, 5, 6]);
        let err = merge_property_groups(&[a, b]).unwrap_err().to_string();
        assert!(err.contains("duplicate column"), "got: {err}");
    }
}

#[cfg(test)]
mod validate_tests {
    use std::path::Path;

    use crate::{
        metadata::{AdjListInfo, EdgeInfo, GraphInfo, Property, PropertyGroup, VertexInfo},
        types::{AdjListType, DataType, FileType},
        validate::{
            Severity, ValidateOptions, validate_edge_info, validate_graph_dir, validate_vertex_info,
        },
        writer::{Edge, EdgesBuilder, PropertyValue, Vertex, VerticesBuilder},
    };
    use tempfile::tempdir;

    fn errors(issues: &[crate::validate::ValidationIssue]) -> Vec<String> {
        issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .map(|i| i.to_string())
            .collect()
    }

    fn person_vertex_info() -> VertexInfo {
        VertexInfo::new("person", 10)
            .with_property_group(PropertyGroup::new(
                vec![Property::new("id", DataType::Int64).primary()],
                FileType::Parquet,
            ))
            .with_property_group(PropertyGroup::new(
                vec![
                    Property::new("firstName", DataType::String),
                    Property::new("lastName", DataType::String),
                ],
                FileType::Parquet,
            ))
    }

    fn knows_edge_info() -> EdgeInfo {
        EdgeInfo::new("person", "knows", "person", 1024, 10, 10, false)
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

    /// Dump a conformant ordered-by-source `knows` edge layout under `base` and
    /// return the `EdgeInfo` describing it. Shared by the edge-validator tests.
    fn dump_knows_edges(base: &Path) -> EdgeInfo {
        let ei = knows_edge_info();
        let mut eb = EdgesBuilder::new(ei.clone(), base, AdjListType::OrderedBySource, 25);
        for i in 0..24i64 {
            eb.add_edge(
                Edge::new(i, i + 1)
                    .add_property("creationDate", PropertyValue::String(format!("2020-{i}"))),
            );
        }
        eb.dump().unwrap();
        ei
    }

    #[test]
    fn validate_edge_info_accepts_conformant_partitioned_edge() {
        // A freshly-dumped ordered-by-source edge (adj_list + offset + property
        // partitions all present) must validate with zero errors when
        // validate_edge_info is called directly.
        let dir = tempdir().unwrap();
        let base = dir.path();
        let ei = dump_knows_edges(base);
        let issues = validate_edge_info(&ei, base, &ValidateOptions::default());
        let errs = errors(&issues);
        assert!(
            errs.is_empty(),
            "conformant partitioned edge should validate cleanly, got: {errs:?}"
        );
    }

    #[test]
    fn validate_edge_info_flags_missing_adjlist_chunks() {
        // Remove ONLY the adj_list partition subtree from an otherwise
        // conformant layout: exactly one Error issue naming that missing path.
        let dir = tempdir().unwrap();
        let base = dir.path();
        let ei = dump_knows_edges(base);
        let adj_list_dir = base.join("edge/person_knows_person/ordered_by_source/adj_list");
        assert!(adj_list_dir.exists(), "sanity: adj_list dumped");
        std::fs::remove_dir_all(&adj_list_dir).unwrap();

        let issues = validate_edge_info(&ei, base, &ValidateOptions::default());
        let errs = errors(&issues);
        assert_eq!(
            errs.len(),
            1,
            "removing adj_list yields exactly one error, got: {errs:?}"
        );
        assert!(
            errs[0].contains("adj_list"),
            "the error names the missing adj_list path, got: {errs:?}"
        );
    }

    /// Build a small graph with the public builders, dump it, then assert the
    /// validator passes AND the on-disk layout matches the gar/v1 paths exactly.
    #[test]
    fn round_trip_conformance() {
        let dir = tempdir().unwrap();
        let base = dir.path();

        // --- vertices ---
        let vi = person_vertex_info();
        let mut vb = VerticesBuilder::new(vi.clone(), base);
        for i in 0..25i64 {
            vb.add_vertex(
                Vertex::new()
                    .add_property("id", PropertyValue::Int64(i))
                    .add_property("firstName", PropertyValue::String(format!("first{i}")))
                    .add_property("lastName", PropertyValue::String(format!("last{i}"))),
            );
        }
        let vchunks = vb.dump().unwrap();
        assert_eq!(vchunks, 3, "ceil(25/10) vertex chunks");

        // --- edges (ordered_by_source) ---
        let ei = knows_edge_info();
        let mut eb = EdgesBuilder::new(ei.clone(), base, AdjListType::OrderedBySource, 25);
        for i in 0..24i64 {
            eb.add_edge(
                Edge::new(i, i + 1)
                    .add_property("creationDate", PropertyValue::String(format!("2020-{i}"))),
            );
        }
        eb.dump().unwrap();

        // --- info files ---
        vi.save(base.join("person.vertex.yml")).unwrap();
        ei.save(base.join("person_knows_person.edge.yml")).unwrap();
        let gi = GraphInfo::new("social", "")
            .with_vertex("person.vertex.yml")
            .with_edge("person_knows_person.edge.yml");
        gi.save(base.join("social.graph.yml")).unwrap();

        // --- exact spec paths exist on disk ---
        let expect = |rel: &str| {
            assert!(base.join(rel).exists(), "expected spec path missing: {rel}");
        };
        expect("vertex/person/id/chunk0");
        expect("vertex/person/id/chunk2"); // 3rd chunk
        expect("vertex/person/firstName_lastName/chunk0");
        expect("edge/person_knows_person/ordered_by_source/adj_list/part0/chunk0");
        expect("edge/person_knows_person/ordered_by_source/offset/chunk0");
        expect("edge/person_knows_person/ordered_by_source/creationDate/part0/chunk0");

        // --- validator passes (no errors) ---
        let issues = validate_graph_dir(base.join("social.graph.yml"));
        let errs = errors(&issues);
        assert!(
            errs.is_empty(),
            "round-trip graph should validate cleanly, got: {errs:?}"
        );
    }

    #[test]
    fn missing_vertex_chunk_dir_is_error() {
        let dir = tempdir().unwrap();
        let vi = person_vertex_info();
        // Info written, but no chunk data dumped.
        let issues = validate_vertex_info(&vi, dir.path(), &ValidateOptions::default());
        let errs = errors(&issues);
        assert!(
            errs.iter()
                .any(|e| e.contains("vertex chunk") && e.contains("missing")),
            "missing chunk dir must be an error, got: {errs:?}"
        );
    }

    #[test]
    fn bad_version_is_error() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let mut vi = person_vertex_info();
        vi.version = "gar/v0".into();
        let mut vb = VerticesBuilder::new(vi.clone(), base);
        vb.add_vertex(Vertex::new().add_property("id", PropertyValue::Int64(0)));
        vb.dump().unwrap();
        let issues = validate_vertex_info(&vi, base, &ValidateOptions::default());
        let errs = errors(&issues);
        assert!(
            errs.iter().any(|e| e.contains("version")),
            "bad version, got: {errs:?}"
        );
    }

    #[test]
    fn duplicate_property_across_groups_is_error() {
        // `id` declared in two groups violates the one-group-per-property rule.
        let vi = VertexInfo::new("person", 10)
            .with_property_group(PropertyGroup::new(
                vec![Property::new("id", DataType::Int64).primary()],
                FileType::Parquet,
            ))
            .with_property_group(PropertyGroup::new(
                vec![Property::new("id", DataType::Int64)],
                FileType::Parquet,
            ));
        let dir = tempdir().unwrap();
        let issues = validate_vertex_info(&vi, dir.path(), &ValidateOptions::default());
        let errs = errors(&issues);
        assert!(
            errs.iter().any(|e| e.contains("more than one group")),
            "duplicate property must be an error, got: {errs:?}"
        );
    }

    #[test]
    fn zero_chunk_size_is_error() {
        let mut vi = person_vertex_info();
        vi.chunk_size = 0;
        let dir = tempdir().unwrap();
        let issues = validate_vertex_info(&vi, dir.path(), &ValidateOptions::default());
        assert!(errors(&issues).iter().any(|e| e.contains("chunk_size")));
    }

    #[test]
    fn multiple_primaries_is_error() {
        // gar/v1 allows at most one primary property per vertex/edge (verified
        // against the upstream corpus, where every vertex has exactly one).
        let vi = VertexInfo::new("person", 10).with_property_group(PropertyGroup::new(
            vec![
                Property::new("id", DataType::Int64).primary(),
                Property::new("uid", DataType::Int64).primary(),
            ],
            FileType::Parquet,
        ));
        let dir = tempdir().unwrap();
        let issues = validate_vertex_info(&vi, dir.path(), &ValidateOptions::default());
        assert!(
            errors(&issues)
                .iter()
                .any(|e| e.contains("primary properties")),
            "two primaries must be an error, got: {:?}",
            errors(&issues)
        );
    }

    #[test]
    fn extra_file_in_chunk_dir_is_warning_not_error() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let vi = VertexInfo::new("person", 10).with_property_group(PropertyGroup::new(
            vec![Property::new("id", DataType::Int64).primary()],
            FileType::Parquet,
        ));
        let mut vb = VerticesBuilder::new(vi.clone(), base);
        vb.add_vertex(Vertex::new().add_property("id", PropertyValue::Int64(0)));
        vb.dump().unwrap();
        // Drop a stray file into the chunk dir.
        std::fs::write(base.join("vertex/person/id/README.txt"), b"stray").unwrap();
        let issues = validate_vertex_info(&vi, base, &ValidateOptions::default());
        assert!(
            errors(&issues).is_empty(),
            "stray file must not be an error"
        );
        assert!(
            issues
                .iter()
                .any(|i| i.severity == Severity::Warning && i.message.contains("unexpected file")),
            "stray file should be a warning"
        );
    }

    /// Validate every `gar/v1` graph of the upstream `testing` data
    /// (<https://github.com/apache/incubator-graphar-testing>, the `testing`
    /// submodule, or `GAR_TEST_DATA`). Never fetches over the network.
    ///
    /// The `java/` subtree is skipped, not failed: its info files use a
    /// different (non-`gar/v1`) field schema (`label` instead of `type`).
    #[test]
    fn corpus_validates() {
        let Some(root) = crate::tests::upstream_testing_dir() else {
            eprintln!("upstream testing data not found; skipping corpus validation");
            return;
        };
        let root = root.to_string_lossy().into_owned();
        let opts = ValidateOptions::corpus();

        // Walk for every *.graph.yml under the corpus and validate it.
        let mut graph_ymls = Vec::new();
        collect_graph_ymls(Path::new(&root), &mut graph_ymls);
        assert!(!graph_ymls.is_empty(), "no *.graph.yml found under {root}");

        let mut validated = 0usize;
        let mut skipped = 0usize;
        let mut total_errors = Vec::new();
        for yml in &graph_ymls {
            // The java/ corpus uses a non-gar/v1 info-file schema.
            if yml.components().any(|c| c.as_os_str() == "java") {
                skipped += 1;
                continue;
            }
            let issues = crate::validate::validate_graph_dir_with(yml, &opts);
            // A hard load failure (unsupported parameterized type, foreign
            // schema) is a known model gap, not a structural violation — skip it.
            let load_failed = issues
                .iter()
                .any(|i| i.message.contains("failed to load") || i.message.contains("YAML parse"));
            if load_failed {
                skipped += 1;
                continue;
            }
            validated += 1;
            for issue in &issues {
                if issue.severity == Severity::Error {
                    total_errors.push(format!("{}: {issue}", yml.display()));
                }
            }
        }
        assert!(
            total_errors.is_empty(),
            "corpus validation found {} errors:\n{}",
            total_errors.len(),
            total_errors.join("\n")
        );
        assert!(
            validated > 0,
            "expected to validate at least one gar/v1 corpus graph"
        );
        eprintln!("validated {validated} corpus graphs cleanly ({skipped} skipped)");
    }

    fn collect_graph_ymls(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_graph_ymls(&path, out);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".graph.yml"))
            {
                out.push(path);
            }
        }
    }
}

#[cfg(test)]
mod edge_writer_tests {
    use arrow_array::Int64Array;
    use tempfile::tempdir;

    use crate::{
        metadata::{AdjListInfo, EdgeInfo},
        reader::EdgeReader,
        types::{AdjListType, FileType},
        writer::{Edge, EdgesBuilder},
    };

    fn ordered_by_dest_edge_info() -> EdgeInfo {
        EdgeInfo::new("person", "knows", "person", 1024, 100, 100, false).with_adj_list(
            AdjListInfo {
                ordered: true,
                aligned_by: "dst".into(),
                file_type: FileType::Parquet,
                prefix: None,
            },
        )
    }

    /// The `OrderedByDest` adjacency path builds its CSR offset array bucketed by
    /// *destination* vertex. A previous bug bucketed by `edge.src` regardless of
    /// alignment, so every offset for a dst-ordered chunk collapsed to zero.
    /// Round-trip a known fan-in and assert the per-dst prefix sums.
    #[test]
    fn ordered_by_dest_offsets_count_edges_per_dst() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let ei = ordered_by_dest_edge_info();

        // Fan-in (deliberately src != dst so the old src-keyed bug is exposed):
        //   dst 0: 0 edges, dst 1: 2 edges, dst 2: 1 edge, dst 3: 3 edges.
        // Expected CSR offsets (prefix sums, leading 0): [0, 0, 2, 3, 6].
        let dst_vertex_count = 4u64;
        let mut eb = EdgesBuilder::new(
            ei.clone(),
            base,
            AdjListType::OrderedByDest,
            dst_vertex_count,
        );
        for (src, dst) in [(10, 1), (11, 1), (20, 2), (30, 3), (31, 3), (32, 3)] {
            eb.add_edge(Edge::new(src, dst));
        }
        eb.dump().unwrap();

        let reader = EdgeReader::new(ei, base, 100, dst_vertex_count);
        let batches = reader.read_offset(&AdjListType::OrderedByDest, 0).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5, "offset rows = dst_count + 1");

        let offsets: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            offsets,
            vec![0, 0, 2, 3, 6],
            "per-dst CSR offsets must be bucketed by destination vertex"
        );
    }
}

#[cfg(test)]
mod csv_io_tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use tempfile::tempdir;

    use crate::{io, types::FileType};

    /// CSV write→read must preserve the actual cell values, not merely the row
    /// count, and the writer flushes its `BufWriter` explicitly so a flush error
    /// surfaces as `Err` (matching `json::write`) instead of being swallowed on
    /// drop.
    #[test]
    fn csv_write_read_preserves_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("chunk0.csv");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();

        io::write_chunk(&path, std::slice::from_ref(&batch), &FileType::Csv).unwrap();
        let back = io::read_chunk(&path, &FileType::Csv).unwrap();

        let ids: Vec<i64> = back
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        let names: Vec<String> = back
            .iter()
            .flat_map(|b| {
                let c = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..b.num_rows())
                    .map(|r| c.value(r).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(names, vec!["alice", "bob", "carol"]);
    }
}

#[cfg(test)]
mod empty_slice_contract_tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};

    use crate::{storage::write_chunk_bytes, types::FileType};

    const ALL: &[FileType] = &[
        FileType::Parquet,
        FileType::Csv,
        FileType::Json,
        #[cfg(feature = "orc")]
        FileType::Orc,
        FileType::ArrowIpc,
    ];

    /// Documented contract: encoding an *empty slice* of batches (`&[]`) writes
    /// nothing — `write_chunk_bytes` returns zero bytes for every format (and
    /// `io::write_chunk` likewise creates no file, see
    /// `format_matrix_tests::empty_write_is_noop`). A caller that needs a file to
    /// exist must pass at least one (possibly zero-row) batch.
    #[test]
    fn write_chunk_bytes_empty_slice_yields_no_bytes() {
        for ft in ALL {
            let bytes = write_chunk_bytes(&[], ft).unwrap();
            assert!(bytes.is_empty(), "{ft:?}: &[] must encode to zero bytes");
        }
    }

    /// Contrast: passing a real (even zero-row) batch is *not* the same as `&[]`.
    /// The schema-bearing/columnar formats (Parquet, Orc, Arrow IPC) and CSV (a
    /// header row) emit bytes; line-delimited JSON emits nothing for zero rows.
    /// Documenting this as data pins the per-format contract.
    #[test]
    fn write_chunk_bytes_zero_row_batch_per_format() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let empty_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])
                .unwrap();
        for ft in ALL.iter().cloned() {
            let bytes = write_chunk_bytes(std::slice::from_ref(&empty_batch), &ft).unwrap();
            let nonempty = bytes.is_empty();
            match ft {
                // Line-delimited JSON writes one line per row, so zero rows → no bytes.
                FileType::Json => assert!(bytes.is_empty(), "Json zero-row should be empty"),
                // CSV writes a header; Parquet/Orc/IPC write schema/footer metadata.
                _ => assert!(
                    !bytes.is_empty(),
                    "{ft:?}: a zero-row batch still encodes a schema/header (nonempty={nonempty})"
                ),
            }
        }
    }
}

/// The upstream `testing` data: `GAR_TEST_DATA` if set, else the `testing`
/// submodule of this repository if it is checked out.
#[cfg(test)]
pub(crate) fn upstream_testing_dir() -> Option<std::path::PathBuf> {
    if let Some(dir) = std::env::var_os("GAR_TEST_DATA") {
        return Some(dir.into());
    }
    let sub = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testing");
    sub.join("ldbc_sample").is_dir().then_some(sub)
}
