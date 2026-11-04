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

use arrow_schema::{DataType as ArrowDataType, Field, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::error::{GraphArError, Result};

/// A property's value type, as named in the info files: `bool`, `int32`,
/// `int64`, `float`, `double`, `string`, `date`, `timestamp`, `time` and
/// `list<T>`. A bare `list` (written by older versions of this crate) reads as
/// `list<string>`; lists do not nest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Int32,
    Int64,
    Float,
    Double,
    String,
    /// Days since the UNIX epoch (Arrow `Date32`).
    Date,
    /// Milliseconds since the UNIX epoch (Arrow `Timestamp(ms)`).
    Timestamp,
    Time,
    /// A variable-length list of a scalar type.
    List(Box<DataType>),
}

impl DataType {
    /// The name used in info files.
    pub fn name(&self) -> String {
        match self {
            DataType::Bool => "bool".into(),
            DataType::Int32 => "int32".into(),
            DataType::Int64 => "int64".into(),
            DataType::Float => "float".into(),
            DataType::Double => "double".into(),
            DataType::String => "string".into(),
            DataType::Date => "date".into(),
            DataType::Timestamp => "timestamp".into(),
            DataType::Time => "time".into(),
            DataType::List(item) => format!("list<{}>", item.name()),
        }
    }

    /// Parse a name from an info file.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "bool" => DataType::Bool,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "float" => DataType::Float,
            "double" => DataType::Double,
            "string" => DataType::String,
            "date" => DataType::Date,
            "timestamp" => DataType::Timestamp,
            "time" => DataType::Time,
            "list" => DataType::List(Box::new(DataType::String)),
            other => {
                let item = other.strip_prefix("list<")?.strip_suffix('>')?;
                match DataType::parse(item)? {
                    DataType::List(_) => return None,
                    scalar => DataType::List(Box::new(scalar)),
                }
            }
        })
    }

    fn arrow_type(&self) -> ArrowDataType {
        match self {
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float => ArrowDataType::Float32,
            DataType::Double => ArrowDataType::Float64,
            DataType::String => ArrowDataType::Utf8,
            DataType::Date => ArrowDataType::Date32,
            DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Time => ArrowDataType::Time64(TimeUnit::Millisecond),
            // The same `item` field the Arrow `ListBuilder`s of the row
            // builders produce.
            DataType::List(item) => ArrowDataType::List(std::sync::Arc::new(Field::new(
                "item",
                item.arrow_type(),
                true,
            ))),
        }
    }

    pub fn to_arrow(&self, field_name: &str, nullable: bool) -> Field {
        Field::new(field_name, self.arrow_type(), nullable)
    }
}

impl Serialize for DataType {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.name())
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        DataType::parse(&name)
            .ok_or_else(|| serde::de::Error::custom(format!("unknown data type `{name}`")))
    }
}

/// A chunk format, as named in the info files. The spec's formats are the
/// plain variants; any other name is an [`FileType::Extension`], served by a
/// [`crate::PayloadFormat`] registered under that name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FileType {
    #[default]
    Parquet,
    Orc,
    Csv,
    Json,
    /// A format outside the spec; see [`crate::register_format`].
    Extension(String),
}

impl FileType {
    /// The `file_type` value in the info files.
    pub fn name(&self) -> &str {
        match self {
            FileType::Parquet => "parquet",
            FileType::Orc => "orc",
            FileType::Csv => "csv",
            FileType::Json => "json",
            FileType::Extension(name) => name,
        }
    }

    /// The format with this `file_type` value.
    pub fn from_name(name: &str) -> Self {
        match name {
            "parquet" => FileType::Parquet,
            "orc" => FileType::Orc,
            "csv" => FileType::Csv,
            "json" => FileType::Json,
            other => FileType::Extension(other.to_string()),
        }
    }

    /// The format's file extension. An unregistered
    /// [`FileType::Extension`] uses its name.
    pub fn extension(&self) -> String {
        crate::format::format_for(self)
            .map(|f| f.extension().to_string())
            .unwrap_or_else(|_| self.name().to_string())
    }
}

impl Serialize for FileType {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for FileType {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Ok(FileType::from_name(&String::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdjListType {
    OrderedBySource,
    OrderedByDest,
    UnorderedBySource,
    UnorderedByDest,
}

impl AdjListType {
    pub fn dir_name(&self) -> &'static str {
        match self {
            AdjListType::OrderedBySource => "ordered_by_source",
            AdjListType::OrderedByDest => "ordered_by_dest",
            AdjListType::UnorderedBySource => "unordered_by_source",
            AdjListType::UnorderedByDest => "unordered_by_dest",
        }
    }

    pub fn is_ordered(&self) -> bool {
        matches!(
            self,
            AdjListType::OrderedBySource | AdjListType::OrderedByDest
        )
    }

    pub fn aligned_by_src(&self) -> bool {
        matches!(
            self,
            AdjListType::OrderedBySource | AdjListType::UnorderedBySource
        )
    }
}

impl TryFrom<(&str, &str)> for AdjListType {
    type Error = GraphArError;

    fn try_from((ordered, aligned_by): (&str, &str)) -> Result<Self> {
        match (ordered, aligned_by) {
            ("true", "src") => Ok(AdjListType::OrderedBySource),
            ("true", "dst") => Ok(AdjListType::OrderedByDest),
            ("false", "src") => Ok(AdjListType::UnorderedBySource),
            ("false", "dst") => Ok(AdjListType::UnorderedByDest),
            _ => Err(GraphArError::InvalidAdjListType {
                ordered: ordered.to_string(),
                aligned_by: aligned_by.to_string(),
            }),
        }
    }
}
