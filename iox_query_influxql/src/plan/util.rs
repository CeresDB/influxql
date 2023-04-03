use datafusion::common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion::logical_expr::Operator;
use influxdb_influxql_parser::expression::BinaryOperator;
use influxdb_influxql_parser::string::Regex;
use query_functions::clean_non_meta_escapes;
use schema::Schema;
use std::sync::Arc;

pub(in crate::plan) fn binary_operator_to_df_operator(op: BinaryOperator) -> Operator {
    match op {
        BinaryOperator::Add => Operator::Plus,
        BinaryOperator::Sub => Operator::Minus,
        BinaryOperator::Mul => Operator::Multiply,
        BinaryOperator::Div => Operator::Divide,
        BinaryOperator::Mod => Operator::Modulo,
        BinaryOperator::BitwiseAnd => Operator::BitwiseAnd,
        BinaryOperator::BitwiseOr => Operator::BitwiseOr,
        BinaryOperator::BitwiseXor => Operator::BitwiseXor,
    }
}

/// Return the IOx schema for the specified DataFusion schema.
pub(in crate::plan) fn schema_from_df(schema: &DFSchema) -> Result<Schema> {
    let s: Arc<arrow::datatypes::Schema> = Arc::new(schema.into());
    ceresdb_schema_to_influxdb(s).and_then(|s| {
        s.try_into().map_err(|err| {
            DataFusionError::Internal(format!(
                "unable to convert DataFusion schema to IOx schema: {err}"
            ))
        })
    })
}

/// Container for both the DataFusion and equivalent IOx schema.
pub(in crate::plan) struct Schemas {
    pub(in crate::plan) df_schema: DFSchemaRef,
    pub(in crate::plan) iox_schema: Schema,
}

impl Schemas {
    pub(in crate::plan) fn new(df_schema: &DFSchemaRef) -> Result<Self> {
        Ok(Self {
            df_schema: Arc::clone(df_schema),
            iox_schema: schema_from_df(df_schema)?,
        })
    }
}

/// Sanitize an InfluxQL regular expression and create a compiled [`regex::Regex`].
pub(crate) fn parse_regex(re: &Regex) -> Result<regex::Regex> {
    let pattern = clean_non_meta_escapes(re.as_str());
    regex::Regex::new(&pattern).map_err(|e| {
        DataFusionError::External(format!("invalid regular expression '{re}': {e}").into())
    })
}

/// Convert arrow schema from CeresDB to format used in InfluxDB
pub fn ceresdb_schema_to_influxdb(
    arrow_schema: Arc<arrow::datatypes::Schema>,
) -> Result<Arc<arrow::datatypes::Schema>> {
    use arrow::datatypes::DataType;

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

            let data_type = if i == time_idx {
                // this is required by iox, time is ms unit in ceresdb
                schema::TIME_DATA_TYPE()
            } else {
                data_type.clone()
            };
            // tsid column is treated as field, so it's nullable
            let nullable = if 0 == i { true } else { f.is_nullable() };
            let md = [(
                "iox::column::type".to_string(),
                influxql_col_type.to_string(),
            )]
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
            Ok(arrow::datatypes::Field::new(f.name(), data_type, nullable).with_metadata(md))
        })
        .collect::<Result<_>>()?;

    Ok(Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        md.clone(),
    )))
}
