use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::DateTime;
use scylla::frame::response::result::ColumnSpec;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlDate, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue};

/// Parse a comma-separated string into individual parameter values.
pub fn parse_param_set(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Convert clap's Vec<String> (multiple --params flags) into Vec<Vec<String>>.
pub fn parse_params_from_args(params: Option<Vec<String>>) -> Vec<Vec<String>> {
    match params {
        Some(param_strings) => param_strings
            .into_iter()
            .map(|s| parse_param_set(&s))
            .collect(),
        None => vec![],
    }
}

/// Read parameter sets from a file, one set per line (CSV format).
pub fn parse_params_from_file(path: &Path) -> Result<Vec<Vec<String>>> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read params file: {}", path.display()))?;

    let param_sets = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(parse_param_set)
        .collect();

    Ok(param_sets)
}

/// Infer CqlValue from a string based on the column specification.
pub fn infer_cql_value(s: &str, col_spec: &ColumnSpec) -> Result<CqlValue> {
    use scylla::frame::response::result::{ColumnType, NativeType};

    let col_type = col_spec.typ();
    match col_type {
        ColumnType::Native(native) => {
            match native {
                NativeType::Int => {
                    let v = s.parse::<i32>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as Int for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Int(v))
                }
                NativeType::BigInt | NativeType::Counter => {
                    let v = s.parse::<i64>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as BigInt for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::BigInt(v))
                }
                NativeType::SmallInt => {
                    let v = s.parse::<i16>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as SmallInt for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::SmallInt(v))
                }
                NativeType::TinyInt => {
                    let v = s.parse::<i8>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as TinyInt for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::TinyInt(v))
                }
                NativeType::Float => {
                    let v = s.parse::<f32>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as Float for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Float(v))
                }
                NativeType::Double => {
                    let v = s.parse::<f64>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as Double for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Double(v))
                }
                NativeType::Boolean => {
                    let v = s.parse::<bool>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as Boolean for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Boolean(v))
                }
                NativeType::Text => Ok(CqlValue::Text(s.to_owned())),
                NativeType::Ascii => Ok(CqlValue::Ascii(s.to_owned())),
                NativeType::Uuid => {
                    let v = s.parse::<uuid::Uuid>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as UUID for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Uuid(v))
                }
                NativeType::Timeuuid => {
                    let v = s.parse::<uuid::Uuid>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as Timeuuid for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Timeuuid(CqlTimeuuid::from(v)))
                }
                NativeType::Inet => {
                    let v = s.parse::<std::net::IpAddr>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as IpAddr for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Inet(v))
                }
                NativeType::Timestamp => {
                    // Parse RFC3339 timestamp using chrono
                    let dt = DateTime::parse_from_rfc3339(s).with_context(|| {
                    format!(
                        "Failed to parse '{}' as RFC3339 timestamp for column '{}'. Expected format: 2024-01-15T10:30:00Z",
                        s,
                        col_spec.name()
                    )
                })?;
                    let millis = dt.timestamp_millis();
                    Ok(CqlValue::Timestamp(CqlTimestamp(millis)))
                }
                NativeType::Date => {
                    // Parse as u32 days since epoch
                    let days = s.parse::<u32>().with_context(|| {
                        format!(
                            "Failed to parse '{}' as Date (days since epoch) for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Date(CqlDate(days)))
                }
                NativeType::Time => {
                    // Parse as i64 nanoseconds since midnight
                    let nanos = s.parse::<i64>().with_context(|| {
                    format!("Failed to parse '{}' as Time (nanoseconds since midnight) for column '{}'", s, col_spec.name())
                })?;
                    Ok(CqlValue::Time(CqlTime(nanos)))
                }
                NativeType::Blob => {
                    // Hex decode
                    let bytes = hex::decode(s).with_context(|| {
                        format!(
                            "Failed to parse '{}' as hex-encoded Blob for column '{}'",
                            s,
                            col_spec.name()
                        )
                    })?;
                    Ok(CqlValue::Blob(bytes))
                }
                other => anyhow::bail!(
                    "Unsupported native type {:?} for column '{}'. Cannot convert from string parameter.",
                    other,
                    col_spec.name()
                ),
            }
        }
        other => anyhow::bail!(
            "Unsupported column type {:?} for column '{}'. Cannot convert from string parameter.",
            other,
            col_spec.name()
        ),
    }
}

/// Convert a single parameter set (Vec<String>) to Vec<CqlValue> using prepared statement metadata.
pub fn convert_param_set(
    raw_params: Vec<String>,
    prepared: &PreparedStatement,
) -> Result<Vec<CqlValue>> {
    let col_specs = prepared.get_variable_col_specs();

    if raw_params.len() != col_specs.len() {
        anyhow::bail!(
            "Parameter count mismatch: query expects {} parameters, but {} were provided",
            col_specs.len(),
            raw_params.len()
        );
    }

    raw_params
        .into_iter()
        .zip(col_specs.iter())
        .map(|(s, col_spec)| infer_cql_value(&s, col_spec))
        .collect()
}

/// Convert all parameter sets to Vec<Vec<CqlValue>>.
pub fn convert_all_param_sets(
    raw_param_sets: Vec<Vec<String>>,
    prepared: &PreparedStatement,
) -> Result<Vec<Vec<CqlValue>>> {
    raw_param_sets
        .into_iter()
        .enumerate()
        .map(|(i, raw_set)| {
            convert_param_set(raw_set, prepared)
                .with_context(|| format!("Failed to convert parameter set {}", i + 1))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_parse_param_set() {
        assert_eq!(parse_param_set("foo"), vec!["foo"]);
        assert_eq!(parse_param_set("a,b,c"), vec!["a", "b", "c"]);
        assert_eq!(parse_param_set(" a , b , c "), vec!["a", "b", "c"]);
        assert_eq!(parse_param_set(""), Vec::<String>::new());
    }

    #[test]
    fn test_parse_params_from_args() {
        assert_eq!(
            parse_params_from_args(Some(vec!["a,b,c".to_string()])),
            vec![vec!["a", "b", "c"]]
        );
        assert_eq!(
            parse_params_from_args(Some(vec!["a,b".to_string(), "c,d".to_string()])),
            vec![vec!["a", "b"], vec!["c", "d"]]
        );
        assert_eq!(parse_params_from_args(None), Vec::<Vec<String>>::new());
    }

    #[test]
    fn test_parse_params_from_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "a,b,c").unwrap();
        writeln!(file).unwrap(); // empty line
        writeln!(file, "# comment").unwrap();
        writeln!(file, "d,e").unwrap();
        file.flush().unwrap();

        let result = parse_params_from_file(file.path()).unwrap();
        assert_eq!(result, vec![vec!["a", "b", "c"], vec!["d", "e"]]);
    }
}
