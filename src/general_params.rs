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

//! Reserved column and file names of the `gar/v1` layout. The names match the
//! upstream C++ `GeneralParams`, so chunks written here are read by the C++,
//! Java and Spark libraries and the other way round.

/// Absolute 0-based vertex index, stored in every vertex property chunk.
pub const VERTEX_INDEX_COL: &str = "_graphArVertexIndex";
/// Source vertex index column of an adjacency-list chunk.
pub const SRC_INDEX_COL: &str = "_graphArSrcIndex";
/// Destination vertex index column of an adjacency-list chunk.
pub const DST_INDEX_COL: &str = "_graphArDstIndex";
/// Offset column of an ordered adjacency list's offset chunk.
pub const OFFSET_COL: &str = "_graphArOffset";
/// File holding a vertex type's (or an adjacency list's aligned side's) vertex count.
pub const VERTEX_COUNT_FILE: &str = "vertex_count";
/// Prefix of the per-partition edge count files (`edge_count<N>`).
pub const EDGE_COUNT_FILE_PREFIX: &str = "edge_count";
