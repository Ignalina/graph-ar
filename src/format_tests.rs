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

//! The [`crate::PayloadFormat`] extension point: the spec's chunk formats are
//! built in, anything else is registered by name.

#[cfg(test)]
mod payload_format {
    use std::{path::Path, sync::Arc};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::{
        AdjListInfo, AdjListType, DST_INDEX_COL, DataType, EdgeInfo, EdgeReader, FileType,
        PayloadFormat, Property, PropertyGroup, Result, SRC_INDEX_COL, Severity, VertexInfo,
        VertexReader, format_for, io, register_format,
        storage::{ChunkStore, read_chunk_bytes, write_chunk_bytes},
        validate::{ValidateOptions, validate_vertex_info},
        write_edge_table, write_vertex_table,
    };

    /// A format outside the spec: Parquet behind a four-byte marker.
    struct Marked;

    const MARK: &[u8] = b"MARK";

    impl PayloadFormat for Marked {
        fn name(&self) -> &str {
            "marked"
        }
        fn extension(&self) -> &str {
            "mrk"
        }
        fn write_bytes(&self, batches: &[RecordBatch]) -> Result<Vec<u8>> {
            let mut out = MARK.to_vec();
            out.extend(write_chunk_bytes(batches, &FileType::Parquet)?);
            Ok(out)
        }
        fn read_bytes(&self, bytes: Bytes, _declared: Option<&Schema>) -> Result<Vec<RecordBatch>> {
            assert_eq!(&bytes[..4], MARK);
            read_chunk_bytes(bytes.slice(4..), &FileType::Parquet)
        }
    }

    fn marked() -> FileType {
        register_format(Arc::new(Marked))
    }

    fn table(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..n))]).unwrap()
    }

    fn vertex_info(ft: FileType) -> VertexInfo {
        VertexInfo::new("v", 4).with_property_group(PropertyGroup::new(
            vec![Property::new("id", DataType::Int64).primary()],
            ft,
        ))
    }

    #[test]
    fn spec_formats_are_built_in() {
        for (ft, name, ext) in [
            (FileType::Parquet, "parquet", "parquet"),
            (FileType::Csv, "csv", "csv"),
            (FileType::Json, "json", "json"),
            (FileType::Orc, "orc", "orc"),
        ] {
            let f = format_for(&ft).unwrap();
            assert_eq!((f.name(), f.extension()), (name, ext));
            assert_eq!(ft.extension(), ext);
            assert_eq!(serde_yaml::to_string(&ft).unwrap().trim(), name);
            assert_eq!(serde_yaml::from_str::<FileType>(name).unwrap(), ft);
        }
        assert_eq!(FileType::default(), FileType::Parquet);
    }

    #[test]
    fn a_registered_format_writes_and_reads_a_whole_graph() {
        let ft = marked();
        assert_eq!(ft, FileType::Extension("marked".into()));
        assert_eq!(ft.extension(), "mrk");
        let dir = tempdir().unwrap();
        let base = dir.path();

        let vi = vertex_info(ft.clone());
        write_vertex_table(&vi, base, &table(10)).unwrap();
        let raw = std::fs::read(base.join("vertex/v/id/chunk0")).unwrap();
        assert_eq!(&raw[..4], MARK, "the chunk went through the format");
        let b = VertexReader::open(vi, base).unwrap().read_merged().unwrap();
        assert_eq!(b.num_rows(), 10);

        let ei = EdgeInfo::new("v", "e", "v", 8, 4, 4, true).with_adj_list(AdjListInfo {
            ordered: true,
            aligned_by: "src".into(),
            file_type: ft,
            prefix: None,
        });
        let schema = Arc::new(Schema::new(vec![
            Field::new(SRC_INDEX_COL, ArrowDataType::Int64, false),
            Field::new(DST_INDEX_COL, ArrowDataType::Int64, false),
        ]));
        let edges = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 9])),
                Arc::new(Int64Array::from(vec![1, 2, 0])),
            ],
        )
        .unwrap();
        let t = AdjListType::OrderedBySource;
        write_edge_table(&ei, base, &t, &edges, 10).unwrap();
        let (offsets, indices) = EdgeReader::open(ei, base, &t)
            .unwrap()
            .read_csr(&t)
            .unwrap();
        assert_eq!(offsets.len(), 11);
        assert_eq!(indices, vec![1, 2, 0]);
    }

    #[test]
    fn info_files_name_a_registered_format() {
        let ft = marked();
        let dir = tempdir().unwrap();
        let p = dir.path().join("v.vertex.yml");
        vertex_info(ft.clone()).save(&p).unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(text.contains("file_type: marked\n"), "{text}");
        assert_eq!(
            VertexInfo::load(&p).unwrap().property_groups[0].file_type,
            ft
        );
    }

    #[tokio::test]
    async fn chunk_store_uses_a_registered_format() {
        let ft = marked();
        let dir = tempdir().unwrap();
        let store = ChunkStore::local(dir.path()).unwrap();
        store
            .write_chunk("c/chunk0", &[table(3)], &ft)
            .await
            .unwrap();
        let back = store.read_chunk("c/chunk0", &ft).await.unwrap();
        assert_eq!(back[0].num_rows(), 3);
    }

    #[test]
    fn an_unregistered_format_is_a_clear_error() {
        let ft: FileType = serde_yaml::from_str("nobody_registered_this").unwrap();
        let err = io::read_chunk(Path::new("/nonexistent"), &ft)
            .unwrap_err()
            .to_string();
        assert!(err.contains("nobody_registered_this"), "{err}");
        assert!(err.contains("not registered"), "{err}");
        assert!(format_for(&ft).is_err());
    }

    #[test]
    fn the_validator_flags_an_unregistered_format() {
        let dir = tempdir().unwrap();
        let vi = vertex_info(FileType::Extension("nobody_registered_this".into()));
        let issues = validate_vertex_info(&vi, dir.path(), &ValidateOptions::default());
        assert!(
            issues
                .iter()
                .any(|i| i.severity == Severity::Error && i.message.contains("not registered")),
            "{issues:?}"
        );
    }

    #[test]
    fn a_format_cannot_take_a_spec_name() {
        struct Fake;
        impl PayloadFormat for Fake {
            fn name(&self) -> &str {
                "parquet"
            }
            fn extension(&self) -> &str {
                "parquet"
            }
            fn write_bytes(&self, _: &[RecordBatch]) -> Result<Vec<u8>> {
                Ok(Vec::new())
            }
            fn read_bytes(&self, _: Bytes, _: Option<&Schema>) -> Result<Vec<RecordBatch>> {
                Ok(Vec::new())
            }
        }
        // The spec format stays what FileType::Parquet resolves to.
        assert_eq!(register_format(Arc::new(Fake)), FileType::Parquet);
        let dir = tempdir().unwrap();
        let path = dir.path().join("chunk0");
        io::write_chunk(&path, &[table(2)], &FileType::Parquet).unwrap();
        assert_eq!(
            io::read_chunk(&path, &FileType::Parquet).unwrap()[0].num_rows(),
            2
        );
    }
}

#[cfg(test)]
mod read_path {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use tempfile::tempdir;

    use crate::{
        AdjListInfo, AdjListType, DST_INDEX_COL, EdgeInfo, EdgeReader, FileType, SRC_INDEX_COL, io,
        write_edge_table,
    };

    fn edges(m: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SRC_INDEX_COL, ArrowDataType::Int64, false),
            Field::new(DST_INDEX_COL, ArrowDataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values((0..m).map(|e| e % 100))),
                Arc::new(Int64Array::from_iter_values((0..m).map(|e| (e * 7) % 100))),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_parquet_chunk_reads_as_one_batch_not_one_per_1024_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("chunk0");
        io::write_chunk(&path, &[edges(50_000)], &FileType::Parquet).unwrap();
        let batches = io::read_chunk(&path, &FileType::Parquet).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 50_000);
    }

    #[test]
    fn read_chunk_columns_decodes_only_the_named_columns() {
        let dir = tempdir().unwrap();
        for ft in [FileType::Parquet, FileType::Csv] {
            let path = dir.path().join(format!("chunk.{}", ft.extension()));
            io::write_chunk(&path, &[edges(10)], &ft).unwrap();
            let b = io::read_chunk_columns(&path, &ft, &[DST_INDEX_COL]).unwrap();
            assert_eq!(b[0].num_columns(), 1);
            assert_eq!(b[0].schema().field(0).name(), DST_INDEX_COL);
        }
    }

    #[test]
    fn read_property_returns_only_that_column() {
        use crate::{
            DataType, Property, PropertyGroup, VertexInfo, VertexReader, write_vertex_table,
        };
        let dir = tempdir().unwrap();
        let vi = VertexInfo::new("v", 4).with_property_group(PropertyGroup::new(
            vec![
                Property::new(SRC_INDEX_COL, DataType::Int64),
                Property::new(DST_INDEX_COL, DataType::Int64),
            ],
            FileType::Parquet,
        ));
        write_vertex_table(&vi, dir.path(), &edges(10)).unwrap();
        let b = VertexReader::open(vi, dir.path())
            .unwrap()
            .read_property(DST_INDEX_COL)
            .unwrap();
        assert_eq!(b.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
        assert!(b.iter().all(|b| b.num_columns() == 1));
    }

    #[test]
    fn read_csr_is_unchanged_by_the_projection() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        let ei = EdgeInfo::new("v", "e", "v", 64, 32, 32, true).with_adj_list(AdjListInfo {
            ordered: true,
            aligned_by: "src".into(),
            file_type: FileType::Parquet,
            prefix: None,
        });
        let t = AdjListType::OrderedBySource;
        write_edge_table(&ei, base, &t, &edges(1000), 100).unwrap();
        let (offsets, indices) = EdgeReader::open(ei, base, &t)
            .unwrap()
            .read_csr(&t)
            .unwrap();
        assert_eq!(offsets.len(), 101);
        assert_eq!(indices.len(), 1000);
        // Row v holds the edges e with e % 100 == v; dst is 7v mod 100.
        for v in 0..100usize {
            let row = &indices[offsets[v] as usize..offsets[v + 1] as usize];
            assert_eq!(row.len(), 10);
            assert!(row.iter().all(|&d| d == (v as i64 * 7) % 100), "row {v}");
        }
    }
}
