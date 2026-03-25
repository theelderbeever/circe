use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};
use chrono::DateTime;
use scylla::{
    frame::response::result::ColumnSpec,
    statement::prepared::PreparedStatement,
    value::{CqlDate, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue},
};
use serde_json::{Map, Value};

/// Read an NDJSON file and return each line parsed as a JSON object.
pub fn parse_params_from_file(path: &Path) -> Result<Vec<Map<String, Value>>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read params file: {}", path.display()))?;

    content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .map(|(i, line)| {
            serde_json::from_str(line)
                .with_context(|| format!("Failed to parse line {} as JSON object", i + 1))
        })
        .collect()
}

/// Convert a single JSON object to a `HashMap<String, CqlValue>` using the prepared statement's
/// variable bind marker metadata.
pub fn convert_param_set(
    params: Map<String, Value>,
    prepared: &PreparedStatement,
) -> Result<HashMap<String, CqlValue>> {
    let col_specs = prepared.get_variable_col_specs();

    params
        .into_iter()
        .map(|(name, value)| {
            let (_, spec) = col_specs
                .get_by_name(&name)
                .with_context(|| format!("Unknown bind marker '{name}'"))?;
            let cql = json_to_cql(value, spec)
                .with_context(|| format!("Failed to convert value for bind marker '{name}'"))?;
            Ok((name, cql))
        })
        .collect()
}

/// Convert all JSON parameter sets to `Vec<HashMap<String, CqlValue>>`.
pub fn convert_param_sets(
    param_sets: Vec<Map<String, Value>>,
    prepared: &PreparedStatement,
) -> Result<Vec<HashMap<String, CqlValue>>> {
    param_sets
        .into_iter()
        .enumerate()
        .map(|(i, raw)| {
            convert_param_set(raw, prepared)
                .with_context(|| format!("Failed to convert parameter set {}", i + 1))
        })
        .collect()
}

fn json_to_cql(value: Value, col_spec: &ColumnSpec) -> Result<CqlValue> {
    use scylla::frame::response::result::{ColumnType, NativeType};

    match col_spec.typ() {
        ColumnType::Native(native) => match native {
            NativeType::Boolean => {
                let b = value
                    .as_bool()
                    .with_context(|| format!("Expected boolean, got {value}"))?;
                Ok(CqlValue::Boolean(b))
            }

            NativeType::Int => {
                let n = as_i64(&value, col_spec.name())?;
                Ok(CqlValue::Int(n as i32))
            }
            NativeType::BigInt | NativeType::Counter => {
                let n = as_i64(&value, col_spec.name())?;
                Ok(CqlValue::BigInt(n))
            }
            NativeType::SmallInt => {
                let n = as_i64(&value, col_spec.name())?;
                Ok(CqlValue::SmallInt(n as i16))
            }
            NativeType::TinyInt => {
                let n = as_i64(&value, col_spec.name())?;
                Ok(CqlValue::TinyInt(n as i8))
            }

            NativeType::Float => {
                let f = as_f64(&value, col_spec.name())?;
                Ok(CqlValue::Float(f as f32))
            }
            NativeType::Double => {
                let f = as_f64(&value, col_spec.name())?;
                Ok(CqlValue::Double(f))
            }

            NativeType::Text => Ok(CqlValue::Text(as_str(value, col_spec.name())?)),
            NativeType::Ascii => Ok(CqlValue::Ascii(as_str(value, col_spec.name())?)),

            NativeType::Uuid => {
                let s = as_str(value, col_spec.name())?;
                let v = s
                    .parse::<uuid::Uuid>()
                    .with_context(|| format!("Expected UUID string for '{}'", col_spec.name()))?;
                Ok(CqlValue::Uuid(v))
            }
            NativeType::Timeuuid => {
                let s = as_str(value, col_spec.name())?;
                let v = s.parse::<uuid::Uuid>().with_context(|| {
                    format!("Expected Timeuuid string for '{}'", col_spec.name())
                })?;
                Ok(CqlValue::Timeuuid(CqlTimeuuid::from(v)))
            }

            NativeType::Inet => {
                let s = as_str(value, col_spec.name())?;
                let v = s.parse::<std::net::IpAddr>().with_context(|| {
                    format!("Expected IP address string for '{}'", col_spec.name())
                })?;
                Ok(CqlValue::Inet(v))
            }

            NativeType::Timestamp => {
                let s = as_str(value, col_spec.name())?;
                let dt = DateTime::parse_from_rfc3339(&s).with_context(|| {
                    format!(
                        "Expected RFC3339 timestamp for '{}' (e.g. 2024-01-15T10:30:00Z)",
                        col_spec.name()
                    )
                })?;
                Ok(CqlValue::Timestamp(CqlTimestamp(dt.timestamp_millis())))
            }
            NativeType::Date => {
                let n = as_i64(&value, col_spec.name())?;
                Ok(CqlValue::Date(CqlDate(n as u32)))
            }
            NativeType::Time => {
                let n = as_i64(&value, col_spec.name())?;
                Ok(CqlValue::Time(CqlTime(n)))
            }

            NativeType::Blob => {
                let s = as_str(value, col_spec.name())?;
                let bytes = hex::decode(&s).with_context(|| {
                    format!("Expected hex-encoded blob for '{}'", col_spec.name())
                })?;
                Ok(CqlValue::Blob(bytes))
            }

            other => anyhow::bail!(
                "Unsupported native type {other:?} for '{}'",
                col_spec.name()
            ),
        },
        other => anyhow::bail!(
            "Unsupported column type {other:?} for '{}'",
            col_spec.name()
        ),
    }
}

fn as_i64(value: &Value, name: &str) -> Result<i64> {
    value
        .as_i64()
        .with_context(|| format!("Expected integer for '{name}', got {value}"))
}

fn as_f64(value: &Value, name: &str) -> Result<f64> {
    value
        .as_f64()
        .with_context(|| format!("Expected number for '{name}', got {value}"))
}

fn as_str(value: Value, name: &str) -> Result<String> {
    match value {
        Value::String(s) => Ok(s),
        other => anyhow::bail!("Expected string for '{name}', got {other}"),
    }
}
