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

//! YAML output in the style of the upstream info files: block sequences
//! indented under their key, plain scalars where possible, double quotes
//! otherwise, and empty sequences left out. The GraphAr C++ YAML reader
//! accepts this form; it rejects flush sequences and single quotes.

use serde::Serialize;
use serde_yaml::Value;

use crate::error::Result;

/// Serialize `value` as an info file.
pub(crate) fn to_string<T: Serialize>(value: &T) -> Result<String> {
    let v = serde_yaml::to_value(value)?;
    let mut out = String::new();
    match &v {
        Value::Mapping(m) => mapping(&mut out, m, 0),
        other => {
            out.push_str(&scalar(other));
            out.push('\n');
        }
    }
    Ok(out)
}

fn pad(out: &mut String, n: usize) {
    out.extend(std::iter::repeat_n(' ', n));
}

/// Write a mapping's entries at `indent`; the first line's indentation is
/// already written when `first_inline` callers (sequence items) need it.
fn mapping(out: &mut String, m: &serde_yaml::Mapping, indent: usize) {
    let mut first = true;
    for (k, v) in m {
        if matches!(v, Value::Sequence(s) if s.is_empty()) || v.is_null() {
            continue;
        }
        if !first || !out.ends_with("- ") {
            pad(out, indent);
        }
        first = false;
        out.push_str(&scalar(k));
        out.push(':');
        entry(out, v, indent);
    }
}

fn entry(out: &mut String, v: &Value, indent: usize) {
    match v {
        Value::Mapping(m) if !m.is_empty() => {
            out.push('\n');
            mapping(out, m, indent + 2);
        }
        Value::Sequence(s) if !s.is_empty() => {
            out.push('\n');
            for item in s {
                pad(out, indent + 2);
                out.push_str("- ");
                match item {
                    Value::Mapping(m) if !m.is_empty() => mapping(out, m, indent + 4),
                    other => {
                        out.push_str(&scalar(other));
                        out.push('\n');
                    }
                }
            }
        }
        other => {
            out.push(' ');
            out.push_str(&scalar(other));
            out.push('\n');
        }
    }
}

fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => {
            let plain = serde_yaml::to_string(s).unwrap_or_default();
            let plain = plain.trim_end_matches('\n');
            if plain == s {
                s.clone()
            } else {
                serde_json::to_string(s).unwrap_or_default()
            }
        }
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => "null".into(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}
