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

//! Native Rust reader and writer for the Apache GraphAr `gar/v1` file format.
//!
//! Info files are modelled in [`metadata`]; chunks are read and written as
//! Arrow `RecordBatch`es by [`reader`] and [`writer`], in Parquet, CSV, ORC
//! (feature `orc`), JSON or Arrow IPC, on local disk or through
//! [`storage::ChunkStore`] on any `object_store` backend. The on-disk layout
//! is the one the GraphAr C++, Java and Spark libraries use.

mod format_tests;
mod spec_tests;
mod tests;
mod type_tests;

pub mod error;
pub mod general_params;
pub mod io;
pub mod merge;
pub mod metadata;
pub mod reader;
pub mod storage;
pub mod types;
pub mod validate;
pub mod writer;

pub use error::{GraphArError, Result};
pub use general_params::{DST_INDEX_COL, OFFSET_COL, SRC_INDEX_COL, VERTEX_INDEX_COL};
pub use merge::merge_property_groups;
pub use metadata::{AdjListInfo, EdgeInfo, GraphInfo, Property, PropertyGroup, VertexInfo};
pub use reader::{EdgeReader, VertexReader};
pub use types::{AdjListType, DataType, FileType};
pub use validate::{
    Severity, ValidateOptions, ValidationIssue, validate_edge_info, validate_graph_dir,
    validate_graph_dir_with, validate_graph_info, validate_vertex_info,
};
pub use writer::{
    Edge, EdgesBuilder, PropertyValue, Vertex, VerticesBuilder, write_edge_table,
    write_vertex_table,
};
