use arrow_schema::{DataType as ArrowDataType, Field, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::error::{GraphArError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Bool,
    Int32,
    Int64,
    Float,
    Double,
    String,
    Date,
    Timestamp,
    Time,
    List,
}

impl DataType {
    pub fn to_arrow(&self, field_name: &str, nullable: bool) -> Field {
        let dt = match self {
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float => ArrowDataType::Float32,
            DataType::Double => ArrowDataType::Float64,
            DataType::String => ArrowDataType::Utf8,
            DataType::Date => ArrowDataType::Date32,
            DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Time => ArrowDataType::Time64(TimeUnit::Millisecond),
            DataType::List => ArrowDataType::List(std::sync::Arc::new(Field::new(
                "item",
                ArrowDataType::Utf8,
                true,
            ))),
        };
        Field::new(field_name, dt, nullable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    #[default]
    Parquet,
    Orc,
    Csv,
    Json,
    ArrowIpc,
}

impl FileType {
    pub fn extension(&self) -> &'static str {
        match self {
            FileType::Parquet => "parquet",
            FileType::Orc => "orc",
            FileType::Csv => "csv",
            FileType::Json => "json",
            FileType::ArrowIpc => "arrow",
        }
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
