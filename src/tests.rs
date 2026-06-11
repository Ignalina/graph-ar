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
        ei.save(dir.path().join("person_knows_person.edge.yml")).unwrap();

        let gi = GraphInfo::new("test_graph", "./")
            .with_vertex("person.vertex.yml")
            .with_edge("person_knows_person.edge.yml");
        let graph_path = dir.path().join("test_graph.graph.yml");
        gi.save(&graph_path).unwrap();

        let loaded_gi = GraphInfo::load(&graph_path).unwrap();
        assert_eq!(loaded_gi.name, "test_graph");

        let vi2 = loaded_gi.get_vertex_info("person", dir.path()).unwrap();
        assert_eq!(vi2.chunk_size, 100);

        let ei2 = loaded_gi.get_edge_info("person", "knows", "person", dir.path()).unwrap();
        assert!(!ei2.directed);
    }

    #[test]
    fn chunk_path_formatting() {
        let vi = person_vertex_info();
        let group = &vi.property_groups[1]; // firstName_lastName
        let path = vi.chunk_path(group, 0);
        assert_eq!(path.to_str().unwrap(), "vertex/person/firstName_lastName/chunk0.parquet");
    }

    #[test]
    fn edge_adj_list_path_formatting() {
        let ei = knows_edge_info();
        let path = ei.adj_list_chunk_path(&AdjListType::OrderedBySource, 2, 3).unwrap();
        assert_eq!(
            path.to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk3.parquet"
        );
    }

    #[test]
    fn offset_path_formatting() {
        let ei = knows_edge_info();
        let path = ei.offset_chunk_path(&AdjListType::OrderedBySource, 0).unwrap();
        assert_eq!(
            path.to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/offset/chunk0.parquet"
        );
    }

    #[test]
    fn property_chunk_path_formatting() {
        let ei = knows_edge_info();
        let group = &ei.property_groups[0];
        let path = ei.property_chunk_path(&AdjListType::OrderedBySource, group, 1, 0);
        assert_eq!(
            path.to_str().unwrap(),
            "edge/person_knows_person/ordered_by_source/creationDate/part1/chunk0.parquet"
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
        let path = dir
            .path()
            .join(format!("chunk0.{}", file_type.extension()));
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
            FileType::Orc,
            FileType::ArrowIpc,
        ] {
            let path = dir.path().join(format!("empty.{}", ft.extension()));
            io::write_chunk(&path, &[], &ft).unwrap();
            assert!(!path.exists(), "{ft:?} should not create a file for empty input");
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
            FileType::Orc,
            FileType::ArrowIpc,
        ] {
            let rel = format!("vertex/person/id/chunk0.{}", ft.extension());
            store.write_chunk(&rel, &[sample_batch()], &ft).await.unwrap();
            let batches = store.read_chunk(&rel, &ft).await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 2, "{ft:?}");
        }
    }

    #[tokio::test]
    async fn chunk_store_raw_bytes() {
        let dir = tempdir().unwrap();
        let store = ChunkStore::local(dir.path()).unwrap();
        store.put_bytes("meta/graph.yml", b"name: g".to_vec()).await.unwrap();
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

        store.put_bytes("vertex/big.bin", payload.clone()).await.unwrap();
        let got = store.get_bytes("vertex/big.bin").await.unwrap();
        assert_eq!(&got[..], &payload[..], "multipart round-trip must be byte-identical");

        // A payload at/under the threshold still round-trips (single-put path).
        let small = vec![7u8; 64];
        store.put_bytes("vertex/small.bin", small.clone()).await.unwrap();
        assert_eq!(&store.get_bytes("vertex/small.bin").await.unwrap()[..], &small[..]);
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
        let schema = Arc::new(Schema::new(vec![Field::new("id", ArrowDataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from((0..50_000i64).collect::<Vec<_>>()))],
        )
        .unwrap();

        let store = ChunkStore::new(Arc::new(InMemory::new()), "")
            .with_multipart(MultipartOptions::new(4 * 1024, 8 * 1024));

        let rel = "vertex/person/id/chunk0.parquet";
        store.write_chunk(rel, &[batch], &FileType::Parquet).await.unwrap();
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
        store.put_bytes("meta/graph.yml", b"name: g".to_vec()).await.unwrap();
        assert_eq!(&store.get_bytes("meta/graph.yml").await.unwrap()[..], b"name: g");
    }
}

#[cfg(test)]
mod io_tests {
    use crate::{
        metadata::{Property, PropertyGroup, VertexInfo},
        types::FileType,
        writer::{PropertyValue, Vertex, VerticesBuilder},
        reader::VertexReader,
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
        assert_eq!(merged.num_columns(), 3);
        let names: Vec<_> = merged
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["id", "name", "age"]);
    }
}
