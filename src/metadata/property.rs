use arrow_schema::Schema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::types::{DataType, FileType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Property {
    pub name: String,
    pub data_type: DataType,
    #[serde(default)]
    pub is_primary: bool,
    #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
    pub is_nullable: bool,
}

fn default_nullable() -> bool {
    true
}

fn is_true(v: &bool) -> bool {
    *v
}

impl Property {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            is_primary: false,
            is_nullable: true,
        }
    }

    pub fn primary(mut self) -> Self {
        self.is_primary = true;
        self.is_nullable = false;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyGroup {
    pub properties: Vec<Property>,
    pub file_type: FileType,
    /// Relative path prefix; if None, derived from property names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

impl PropertyGroup {
    pub fn new(properties: Vec<Property>, file_type: FileType) -> Self {
        Self { properties, file_type, prefix: None }
    }

    /// Directory name derived from property names joined by `_`.
    pub fn dir_name(&self) -> String {
        self.prefix.clone().unwrap_or_else(|| {
            self.properties.iter().map(|p| p.name.as_str()).collect::<Vec<_>>().join("_")
        })
    }

    pub fn arrow_schema(&self) -> Arc<Schema> {
        let fields: Vec<_> = self
            .properties
            .iter()
            .map(|p| p.data_type.to_arrow(&p.name, p.is_nullable))
            .collect();
        Arc::new(Schema::new(fields))
    }

    pub fn has_property(&self, name: &str) -> bool {
        self.properties.iter().any(|p| p.name == name)
    }

    pub fn primary_key(&self) -> Option<&Property> {
        self.properties.iter().find(|p| p.is_primary)
    }
}
