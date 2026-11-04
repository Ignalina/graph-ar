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

//! Merging row-aligned property-group batches into a single `RecordBatch`.
//!
//! GraphAr stores each property group in its own chunk files, but all groups
//! of a vertex (or edge) type are row-aligned: row `i` of every group refers
//! to the same vertex/edge. Merging is therefore a horizontal column zip.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};

use crate::error::{GraphArError, Result};

/// Merge per-group batch streams into a single batch containing the columns
/// of every group, in the order the groups were given.
///
/// Each element of `groups` is the full batch list for one property group.
/// All groups must cover the same number of rows. Column names must be
/// unique across groups (GraphAr forbids duplicate property names).
pub fn merge_property_groups(groups: &[Vec<RecordBatch>]) -> Result<RecordBatch> {
    let mut flattened = Vec::new();
    for batches in groups {
        if batches.is_empty() {
            continue;
        }
        let schema = batches[0].schema();
        flattened.push(concat_batches(&schema, batches)?);
    }
    if flattened.is_empty() {
        return Err(GraphArError::Other(
            "merge_property_groups: no batches to merge".into(),
        ));
    }

    let row_count = flattened[0].num_rows();
    let mut fields: Vec<Field> = Vec::new();
    let mut columns = Vec::new();
    for batch in &flattened {
        if batch.num_rows() != row_count {
            return Err(GraphArError::Other(format!(
                "merge_property_groups: row count mismatch ({} vs {})",
                batch.num_rows(),
                row_count
            )));
        }
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            let seen = fields.iter().any(|f: &Field| f.name() == field.name());
            // Every group chunk carries the same `_graphArVertexIndex` column;
            // keep the first one.
            if seen && field.name() == crate::general_params::VERTEX_INDEX_COL {
                continue;
            }
            if seen {
                return Err(GraphArError::Other(format!(
                    "merge_property_groups: duplicate column '{}'",
                    field.name()
                )));
            }
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
        }
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}
