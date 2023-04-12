use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result};

/// Convert arrow schema from CeresDB to format used in InfluxDB
pub fn ceresdb_schema_to_influxdb(arrow_schema: Arc<Schema>) -> Result<Arc<Schema>> {
    let md = arrow_schema.metadata();
    let time_idx = md
        .get("schema::timestamp_index")
        .ok_or(DataFusionError::External(
            "no timestamp index".to_string().into(),
        ))?;
    let time_idx: usize = time_idx.parse().map_err(|e| {
        DataFusionError::External(format!("timestamp index value isn't usize, err:{e:?}").into())
    })?;

    let fields = arrow_schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let is_tag = if let Some(v) = f.metadata().get("field::is_tag") {
                v == "true"
            } else {
                false
            };

            let data_type = f.data_type();
            let influxql_col_type = if i == time_idx {
                "iox::column_type::timestamp"
            } else if is_tag {
                "iox::column_type::tag"
            } else {
                match data_type {
                    DataType::Float64 | DataType::Float32 | DataType::Float16 => {
                        "iox::column_type::field::float"
                    }
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                        "iox::column_type::field::integer"
                    }
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                        "iox::column_type::field::uinteger"
                    }
                    DataType::Utf8 | DataType::LargeUtf8 => "iox::column_type::field::string",
                    DataType::Boolean => "iox::column_type::field::boolean",
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "influxql don't supported this datatype, type:{data_type}"
                        )));
                    }
                }
            };

            // tsid column is treated as field, so it's nullable
            let nullable = if "tsid" == f.name() {
                true
            } else {
                f.is_nullable()
            };
            let md = HashMap::from([(
                "iox::column::type".to_string(),
                influxql_col_type.to_string(),
            )]);
            Ok(Field::new(f.name(), data_type.clone(), nullable).with_metadata(md))
        })
        .collect::<Result<_>>()?;

    Ok(Arc::new(Schema::new_with_metadata(fields, md.clone())))
}
