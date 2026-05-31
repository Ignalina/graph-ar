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
}
