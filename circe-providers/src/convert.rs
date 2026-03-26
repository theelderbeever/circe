use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
            Int8Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder, MapBuilder,
            StringBuilder, Time64NanosecondBuilder, TimestampMillisecondBuilder,
        },
        datatypes::{DataType, Field, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    error::{DataFusionError, Result},
};
use scylla::{
    frame::response::result::{CollectionType, ColumnType, NativeType},
    value::{CqlValue, Row},
};

/// Converts a Scylla CQL column type to an Arrow DataType.
pub fn to_arrow(coltype: &ColumnType) -> Result<DataType> {
    match coltype {
        ColumnType::Native(native) => match native {
            NativeType::Int => Ok(DataType::Int32),
            NativeType::BigInt | NativeType::Counter => Ok(DataType::Int64),
            NativeType::SmallInt => Ok(DataType::Int16),
            NativeType::TinyInt => Ok(DataType::Int8),
            NativeType::Float => Ok(DataType::Float32),
            NativeType::Double => Ok(DataType::Float64),
            NativeType::Boolean => Ok(DataType::Boolean),
            NativeType::Text | NativeType::Ascii => Ok(DataType::Utf8),
            NativeType::Blob => Ok(DataType::Binary),
            NativeType::Timestamp => Ok(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into()),
            )),
            NativeType::Date => Ok(DataType::Date32),
            NativeType::Time => Ok(DataType::Time64(TimeUnit::Nanosecond)),
            NativeType::Uuid
            | NativeType::Timeuuid
            | NativeType::Inet
            | NativeType::Varint
            | NativeType::Decimal
            | NativeType::Duration => Ok(DataType::Utf8),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported native Scylla type: {native:?}"
            ))),
        },
        ColumnType::Collection { frozen: _, typ } => match typ {
            CollectionType::List(inner) | CollectionType::Set(inner) => {
                let inner_type = to_arrow(inner)?;
                Ok(DataType::List(Arc::new(Field::new(
                    "item", inner_type, true,
                ))))
            }
            CollectionType::Map(key_type, value_type) => {
                let key = to_arrow(key_type)?;
                let value = to_arrow(value_type)?;
                Ok(DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", key, false),
                                Field::new("value", value, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ))
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported collection type: {typ:?}"
            ))),
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported Scylla type: {:?}",
            coltype
        ))),
    }
}

/// Converts a slice of Scylla rows into an Arrow RecordBatch.
pub fn rows_to_record_batch(rows: &[Row], schema: &SchemaRef) -> Result<RecordBatch> {
    let num_cols = schema.fields().len();
    let num_rows = rows.len();

    let columns: Vec<ArrayRef> = (0..num_cols)
        .map(|col_idx| {
            let field = schema.field(col_idx);
            build_array(rows, col_idx, field.data_type(), num_rows)
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_array(
    rows: &[Row],
    col_idx: usize,
    data_type: &DataType,
    capacity: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int8 => {
            let mut builder = Int8Builder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::TinyInt(v))) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::SmallInt(v))) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Int(v))) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::BigInt(v))) => builder.append_value(*v),
                    Some(Some(CqlValue::Counter(c))) => builder.append_value(c.0),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Float(v))) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Double(v))) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Boolean(v))) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(capacity, capacity * 32);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Text(s) | CqlValue::Ascii(s))) => {
                        builder.append_value(s.as_str())
                    }
                    Some(Some(val)) => match cql_value_to_string(val) {
                        Some(s) => builder.append_value(s.as_str()),
                        None => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(capacity, capacity * 64);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Blob(v))) => builder.append_value(v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let mut builder = TimestampMillisecondBuilder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Timestamp(ts))) => builder.append_value(ts.0),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish().with_timezone("UTC")))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Date(d))) => {
                        // CqlDate is days since -5877641-06-23 (2^31 days before epoch)
                        // Arrow Date32 is days since unix epoch
                        let days_since_epoch = d.0 as i32 - (1 << 31);
                        builder.append_value(days_since_epoch);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let mut builder = Time64NanosecondBuilder::with_capacity(capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::Time(t))) => builder.append_value(t.0),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::List(inner_field) => build_list_array(rows, col_idx, inner_field, capacity),
        DataType::Map(entries_field, _) => build_map_array(rows, col_idx, entries_field, capacity),
        other => Err(DataFusionError::NotImplemented(format!(
            "Arrow type not yet supported in conversion: {other:?}"
        ))),
    }
}

fn build_list_array(
    rows: &[Row],
    col_idx: usize,
    inner_field: &Arc<Field>,
    capacity: usize,
) -> Result<ArrayRef> {
    match inner_field.data_type() {
        DataType::Utf8 => {
            let mut builder = ListBuilder::with_capacity(StringBuilder::new(), capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::List(items) | CqlValue::Set(items))) => {
                        for item in items {
                            match cql_value_to_string(item) {
                                Some(s) => builder.values().append_value(s),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = ListBuilder::with_capacity(Int32Builder::new(), capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::List(items) | CqlValue::Set(items))) => {
                        for item in items {
                            match item {
                                CqlValue::Int(v) => builder.values().append_value(*v),
                                _ => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = ListBuilder::with_capacity(Int64Builder::new(), capacity);
            for row in rows {
                match row.columns.get(col_idx) {
                    Some(Some(CqlValue::List(items) | CqlValue::Set(items))) => {
                        for item in items {
                            match item {
                                CqlValue::BigInt(v) => builder.values().append_value(*v),
                                _ => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "List<{other:?}> not yet supported in conversion"
        ))),
    }
}

fn build_map_array(
    rows: &[Row],
    col_idx: usize,
    entries_field: &Arc<Field>,
    _capacity: usize,
) -> Result<ArrayRef> {
    // Map entries are Struct{key, value}
    let DataType::Struct(struct_fields) = entries_field.data_type() else {
        return Err(DataFusionError::Internal(
            "Map entries field must be a Struct".into(),
        ));
    };
    let key_type = struct_fields[0].data_type();
    let value_type = struct_fields[1].data_type();

    // For now support the common case: Map<Utf8, Utf8>
    if matches!(key_type, DataType::Utf8) && matches!(value_type, DataType::Utf8) {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in rows {
            match row.columns.get(col_idx) {
                Some(Some(CqlValue::Map(entries))) => {
                    for (k, v) in entries {
                        match cql_value_to_string(k) {
                            Some(s) => builder.keys().append_value(s),
                            None => builder.keys().append_null(),
                        }
                        match cql_value_to_string(v) {
                            Some(s) => builder.values().append_value(s),
                            None => builder.values().append_null(),
                        }
                    }
                    builder
                        .append(true)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                }
                _ => {
                    builder
                        .append(false)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    } else {
        Err(DataFusionError::NotImplemented(format!(
            "Map<{key_type:?}, {value_type:?}> not yet supported in conversion"
        )))
    }
}

fn cql_value_to_string(val: &CqlValue) -> Option<String> {
    match val {
        CqlValue::Text(s) | CqlValue::Ascii(s) => Some(s.clone()),
        CqlValue::Uuid(u) => Some(u.to_string()),
        CqlValue::Timeuuid(t) => Some(t.to_string()),
        CqlValue::Inet(ip) => Some(ip.to_string()),
        CqlValue::Varint(v) => Some(format!("{v:?}")),
        CqlValue::Decimal(d) => Some(format!("{d:?}")),
        CqlValue::Duration(d) => Some(format!("{}mo{}d{}ns", d.months, d.days, d.nanoseconds)),
        CqlValue::Int(v) => Some(v.to_string()),
        CqlValue::BigInt(v) => Some(v.to_string()),
        CqlValue::SmallInt(v) => Some(v.to_string()),
        CqlValue::TinyInt(v) => Some(v.to_string()),
        CqlValue::Float(v) => Some(v.to_string()),
        CqlValue::Double(v) => Some(v.to_string()),
        CqlValue::Boolean(v) => Some(v.to_string()),
        _ => None,
    }
}
