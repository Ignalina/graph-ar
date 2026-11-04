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

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::{
    error::{GraphArError, Result},
    metadata::property::PropertyGroup,
};

pub const DEFAULT_VERTEX_CHUNK_SIZE: u64 = 262_144; // 2^18
pub const GAR_VERSION: &str = "gar/v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VertexInfo {
    /// Vertex type label.
    #[serde(rename = "type")]
    pub vertex_type: String,
    #[serde(default = "default_vertex_chunk_size")]
    pub chunk_size: u64,
    /// Relative path prefix for data files.
    pub prefix: String,
    #[serde(default)]
    pub property_groups: Vec<PropertyGroup>,
    #[serde(default = "default_version")]
    pub version: String,
}

fn default_vertex_chunk_size() -> u64 {
    DEFAULT_VERTEX_CHUNK_SIZE
}

pub fn default_version() -> String {
    GAR_VERSION.to_string()
}

impl VertexInfo {
    pub fn new(vertex_type: impl Into<String>, chunk_size: u64) -> Self {
        let vertex_type = vertex_type.into();
        let prefix = format!("vertex/{}/", vertex_type);
        Self {
            vertex_type,
            chunk_size,
            prefix,
            property_groups: Vec::new(),
            version: GAR_VERSION.to_string(),
        }
    }

    pub fn with_property_group(mut self, group: PropertyGroup) -> Self {
        self.property_groups.push(group);
        self
    }

    /// Number of chunks needed for `vertex_count` vertices.
    pub fn chunk_count(&self, vertex_count: u64) -> u64 {
        vertex_count.div_ceil(self.chunk_size)
    }

    /// Path to a specific vertex property chunk file (no leading slash). Chunk
    /// files carry no extension, as in the upstream layout.
    pub fn chunk_path(&self, group: &PropertyGroup, chunk_index: u64) -> PathBuf {
        PathBuf::from(&self.prefix)
            .join(group.dir_name())
            .join(format!("chunk{chunk_index}"))
    }

    /// Path of the `vertex_count` file (8-byte little-endian int64).
    pub fn vertex_count_path(&self) -> PathBuf {
        PathBuf::from(&self.prefix).join(crate::general_params::VERTEX_COUNT_FILE)
    }

    pub fn find_property_group(&self, property_name: &str) -> Option<&PropertyGroup> {
        self.property_groups
            .iter()
            .find(|g| g.has_property(property_name))
    }

    pub fn primary_key_group(&self) -> Option<&PropertyGroup> {
        self.property_groups
            .iter()
            .find(|g| g.primary_key().is_some())
    }

    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&content)?)
    }

    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.vertex_type.is_empty() {
            return Err(GraphArError::Other("vertex type must not be empty".into()));
        }
        if self.chunk_size == 0 {
            return Err(GraphArError::Other("chunk_size must be > 0".into()));
        }
        if self.version != GAR_VERSION {
            return Err(GraphArError::InvalidVersion(self.version.clone()));
        }
        Ok(())
    }
}
