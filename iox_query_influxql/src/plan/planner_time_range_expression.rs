use crate::plan::timestamp::parse_timestamp;
use crate::plan::util::binary_operator_to_df_operator;
use arrow::temporal_conversions::MILLISECONDS_IN_DAY;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{binary_expr, lit, now, BinaryExpr, Expr as DFExpr, Operator};
use influxdb_influxql_parser::expression::BinaryOperator;
use influxdb_influxql_parser::{expression::Expr, literal::Literal};

type ExprResult = Result<DFExpr>;

/// Transform an InfluxQL expression, to a DataFusion logical [`Expr`][DFExpr],
/// applying rules specific to time-range expressions. When possible, literal values are folded.
///
/// ## NOTEs
///
/// The rules applied to this transformation are determined from
/// the Go InfluxQL parser and treated as the source of truth in the
/// absence of an official specification. Most of the implementation
/// is sourced from the [`getTimeRange`][] and [`Reduce`][] functions.
///
/// A [time-range][] expression is determined when either the left or right
/// hand side of a [`ConditionalExpression`][influxdb_influxql_parser::expression::ConditionalExpression]
/// has a single node that refers to a `time` field. Whilst most of InfluxQL
/// performs comparisons of fields using case-sensitive matches, this is a
/// case-insensitive match, per the [`conditionExpr`][conditionExpr] function.
///
/// Binary expressions, where the left and right hand sides are strings, are
/// treated as a string concatenation operation. All other expressions are
/// treated as arithmetic expressions.
///
/// Literal values interpreted as follows:
///
/// * single-quoted strings are interpreted as timestamps when either the left or right
///   hand side of the binary expression is numeric.
/// * integer and float values as nanosecond offsets from the Unix epoch.
///   * The Go implementation may interpret a number as a timestamp or duration,
///     depending on context, however, in reality both are just offsets from the Unix epoch.
///
/// [time range]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#absolute-time
/// [`getTimeRange`]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5788-L5791
/// [`Reduce`]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4850-L4852
/// [conditionExpr]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L5751-L5756
/// [`TZ`]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-time-zone-clause
pub(in crate::plan) fn time_range_to_df_expr(expr: &Expr, tz: Option<chrono_tz::Tz>) -> ExprResult {
    let df_expr = reduce_expr(expr, tz)?;

    // Attempt to coerce the final expression into a timestamp
    Ok(match df_expr {
        // timestamp literals require no transformation and Call has
        // already been validated as a now() function call.
        DFExpr::Literal(ScalarValue::TimestampNanosecond(..))
        | DFExpr::ScalarFunction { .. }
        | DFExpr::BinaryExpr { .. } => df_expr,
        DFExpr::Literal(ScalarValue::Utf8(Some(s))) => {
            parse_timestamp_df_expr(&s, tz).map_err(map_expr_err(expr))?
        }
        DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(d))) => {
            DFExpr::Literal(ScalarValue::TimestampNanosecond(Some(d as i64), None))
        }
        DFExpr::Literal(ScalarValue::Float64(Some(v))) => {
            DFExpr::Literal(ScalarValue::TimestampNanosecond(Some(v as i64), None))
        }
        DFExpr::Literal(ScalarValue::Int64(Some(v))) => {
            DFExpr::Literal(ScalarValue::TimestampNanosecond(Some(v), None))
        }
        _ => {
            return Err(DataFusionError::Plan(
                "invalid time range expression".into(),
            ))
        }
    })
}

/// Simplifies `expr` to an InfluxQL duration and returns a DataFusion interval.
///
/// Returns an error if `expr` is not a duration expression.
///
/// ## NOTE
///
/// The returned interval is limited to a precision of milliseconds,
/// due to [issue #7204][]
///
/// [issue #7204]: https://github.com/influxdata/influxdb_iox/issues/7204
pub(super) fn expr_to_df_interval_dt(expr: &Expr) -> ExprResult {
    let v = duration_expr_to_nanoseconds(expr)?;
    if v % 1_000_000 != 0 {
        Err(DataFusionError::NotImplemented("interval limited to a precision of milliseconds. See https://github.com/influxdata/influxdb_iox/issues/7204".to_owned()))
    } else {
        let v = v / 1_000_000;
        let days = v / MILLISECONDS_IN_DAY;
        // keep the sign on `days` and remove it from `millis`
        let millis = (v - days * MILLISECONDS_IN_DAY).abs();

        // It is not possible for an InfluxQL duration to overflow an IntervalDayTime.
        // An InfluxQL duration encodes a number of nanoseconds into a 64-bit signed integer,
        // which is a maximum of 15,250.2845 days. An IntervalDayTime can encode days
        // as a signed 32-bit number.
        Ok(lit(ScalarValue::new_interval_dt(
            days as i32,
            millis as i32,
        )))
    }
}

/// Reduces an InfluxQL duration `expr` to a nanosecond interval.
pub(super) fn duration_expr_to_nanoseconds(expr: &Expr) -> Result<i64> {
    let df_expr = reduce_expr(expr, None)?;
    match df_expr {
        DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(v))) => Ok(v as i64),
        DFExpr::Literal(ScalarValue::Float64(Some(v))) => Ok(v as i64),
        DFExpr::Literal(ScalarValue::Int64(Some(v))) => Ok(v),
        _ => Err(DataFusionError::Plan("invalid duration expression".into())),
    }
}

fn map_expr_err(expr: &Expr) -> impl Fn(DataFusionError) -> DataFusionError + '_ {
    move |err| {
        DataFusionError::Plan(format!(
            "invalid expression \"{}\": {}",
            expr,
            match err {
                DataFusionError::Plan(str) => str,
                _ => err.to_string(),
            }
        ))
    }
}

fn reduce_expr(expr: &Expr, tz: Option<chrono_tz::Tz>) -> ExprResult {
    match expr {
        Expr::Binary { lhs, op, rhs } => reduce_binary_expr(lhs, *op, rhs, tz).map_err(map_expr_err(expr)),

        Expr::Call { name, .. } => {
            if !name.eq_ignore_ascii_case("now") {
                return Err(DataFusionError::Plan(
                    format!("invalid function call '{name}'"),
                ));
            }
            Ok(now())
        }
        Expr::Nested(expr) => reduce_expr(expr, tz),
        Expr::Literal(val) => match val {
            Literal::Integer(v) => Ok(lit(*v)),
            Literal::Float(v) => Ok(lit(*v)),
            Literal::String(v) => Ok(lit(v.clone())),
            Literal::Timestamp(v) => Ok(lit(ScalarValue::TimestampNanosecond(
                Some(v.timestamp_nanos()),
                None,
            ))),
            Literal::Duration(v) => Ok(lit(ScalarValue::new_interval_mdn(0, 0, **v))),
            _ => Err(DataFusionError::Plan(format!(
                "found literal '{val}', expected duration, float, integer, or timestamp string"
            ))),
        },

        Expr::VarRef { .. } | Expr::BindParameter(_) | Expr::Wildcard(_) | Expr::Distinct(_) => Err(DataFusionError::Plan(format!(
            "found symbol '{expr}', expected now() or a literal duration, float, integer and timestamp string"
        ))),
    }
}

fn reduce_binary_expr(
    lhs: &Expr,
    op: BinaryOperator,
    rhs: &Expr,
    tz: Option<chrono_tz::Tz>,
) -> ExprResult {
    let lhs = reduce_expr(lhs, tz)?;
    let rhs = reduce_expr(rhs, tz)?;

    match lhs {
        DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            reduce_binary_lhs_duration_df_expr(v, op, &rhs, tz)
        }
        DFExpr::Literal(ScalarValue::Int64(Some(v))) => {
            reduce_binary_lhs_integer_df_expr(v, op, &rhs, tz)
        }
        DFExpr::Literal(ScalarValue::Float64(Some(v))) => {
            reduce_binary_lhs_float_df_expr(v, op, &rhs)
        }
        DFExpr::Literal(ScalarValue::TimestampNanosecond(Some(v), _)) => {
            reduce_binary_lhs_timestamp_df_expr(v, op, &rhs, tz)
        }
        DFExpr::ScalarFunction { .. } => {
            reduce_binary_scalar_df_expr(&lhs, op, &expr_to_interval_df_expr(&rhs, tz)?)
        }
        DFExpr::Literal(ScalarValue::Utf8(Some(v))) => {
            reduce_binary_lhs_string_df_expr(&v, op, &rhs, tz)
        }
        _ => Ok(DFExpr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs),
            op: binary_operator_to_df_operator(op),
            right: Box::new(rhs),
        })),
    }
}

/// Reduce `duration OP expr`.
///
/// ```text
/// duration  = duration ( ADD | SUB ) ( duration | NOW() )
/// duration  = duration ( MUL | DIV ) ( float | integer )
/// timestamp = duration ADD string
/// timestamp = duration ADD timestamp
/// ```
fn reduce_binary_lhs_duration_df_expr(
    lhs: i128,
    op: BinaryOperator,
    rhs: &DFExpr,
    tz: Option<chrono_tz::Tz>,
) -> Result<DFExpr> {
    match rhs {
        DFExpr::Literal(val) => match val {
            // durations may be added and subtracted from other durations
            ScalarValue::IntervalMonthDayNano(Some(d)) => match op {
                BinaryOperator::Add => {
                    Ok(lit(ScalarValue::new_interval_mdn(0, 0, (lhs + *d) as i64)))
                }
                BinaryOperator::Sub => {
                    Ok(lit(ScalarValue::new_interval_mdn(0, 0, (lhs - *d) as i64)))
                }
                _ => Err(DataFusionError::Plan(format!(
                    "found operator '{op}', expected +, -"
                ))),
            },
            // durations may only be scaled by float literals
            ScalarValue::Float64(Some(v)) => {
                reduce_binary_lhs_duration_df_expr(lhs, op, &lit(*v as i64), tz)
            }
            ScalarValue::Int64(Some(v)) => match op {
                BinaryOperator::Mul => {
                    Ok(lit(ScalarValue::new_interval_mdn(0, 0, lhs as i64 * *v)))
                }
                BinaryOperator::Div => {
                    Ok(lit(ScalarValue::new_interval_mdn(0, 0, lhs as i64 / *v)))
                }
                _ => Err(DataFusionError::Plan(format!(
                    "found operator '{op}', expected *, /"
                ))),
            },
            // A timestamp may be added to a duration
            ScalarValue::TimestampNanosecond(Some(v), _) if matches!(op, BinaryOperator::Add) => {
                Ok(lit(ScalarValue::TimestampNanosecond(
                    Some(*v + lhs as i64),
                    None,
                )))
            }
            ScalarValue::Utf8(Some(s)) => {
                reduce_binary_lhs_duration_df_expr(lhs, op, &parse_timestamp_df_expr(s, tz)?, tz)
            }
            // This should not occur, as all the DataFusion literal values created by this process
            // are handled above.
            _ => Err(DataFusionError::Internal(format!(
                "unexpected DataFusion literal '{rhs}' for duration expression"
            ))),
        },
        DFExpr::ScalarFunction { .. } => reduce_binary_scalar_df_expr(
            &expr_to_interval_df_expr(&lit(ScalarValue::new_interval_mdn(0, 0, lhs as i64)), tz)?,
            op,
            rhs,
        ),
        _ => Err(DataFusionError::Plan("invalid duration expression".into())),
    }
}

/// Reduce `integer OP expr`.
///
/// ```text
/// integer   = integer ( ADD | SUB | MUL | DIV | MOD | BitwiseAND | BitwiseOR | BitwiseXOR ) integer
/// float     = integer as float OP float
/// timestamp = integer as timestamp OP duration
/// ```
fn reduce_binary_lhs_integer_df_expr(
    lhs: i64,
    op: BinaryOperator,
    rhs: &DFExpr,
    tz: Option<chrono_tz::Tz>,
) -> ExprResult {
    match rhs {
        DFExpr::Literal(ScalarValue::Float64(Some(_))) => {
            reduce_binary_lhs_float_df_expr(lhs as f64, op, rhs)
        }
        DFExpr::Literal(ScalarValue::Int64(Some(v))) => Ok(lit(op.reduce(lhs, *v))),
        DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(_))) => {
            reduce_binary_lhs_timestamp_df_expr(lhs, op, rhs, tz)
        }
        DFExpr::ScalarFunction { .. } | DFExpr::Literal(ScalarValue::TimestampNanosecond(..)) => {
            reduce_binary_lhs_duration_df_expr(lhs.into(), op, rhs, tz)
        }
        DFExpr::Literal(ScalarValue::Utf8(Some(s))) => {
            reduce_binary_lhs_duration_df_expr(lhs.into(), op, &parse_timestamp_df_expr(s, tz)?, tz)
        }
        _ => Err(DataFusionError::Plan("invalid integer expression".into())),
    }
}

/// Reduce `float OP expr`.
///
/// ```text
/// float = float ( ADD | SUB | MUL | DIV | MOD ) ( float | integer)
/// ```
fn reduce_binary_lhs_float_df_expr(lhs: f64, op: BinaryOperator, rhs: &DFExpr) -> ExprResult {
    Ok(lit(match rhs {
        DFExpr::Literal(ScalarValue::Float64(Some(rhs))) => {
            op.try_reduce(lhs, *rhs).ok_or_else(|| {
                DataFusionError::Plan("invalid operator for float expression".to_string())
            })?
        }
        DFExpr::Literal(ScalarValue::Int64(Some(rhs))) => {
            op.try_reduce(lhs, *rhs).ok_or_else(|| {
                DataFusionError::Plan("invalid operator for float expression".to_string())
            })?
        }
        _ => return Err(DataFusionError::Plan("invalid float expression".into())),
    }))
}

/// Reduce `timestamp OP expr`.
///
/// The right-hand `expr` must be of a type that can be
/// coalesced to a duration, which includes a `duration`, `integer` or a
/// `string`. A `string` is parsed as a timestamp an interpreted as
/// the number of nanoseconds from the Unix epoch.
///
/// ```text
/// timestamp = timestamp ( ADD | SUB ) ( duration | integer | string | timestamp )
/// ```
fn reduce_binary_lhs_timestamp_df_expr(
    lhs: i64,
    op: BinaryOperator,
    rhs: &DFExpr,
    tz: Option<chrono_tz::Tz>,
) -> ExprResult {
    match rhs {
        DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(d))) => match op {
            BinaryOperator::Add => Ok(lit(ScalarValue::TimestampNanosecond(Some(lhs + *d as i64), None))),
            BinaryOperator::Sub => Ok(lit(ScalarValue::TimestampNanosecond(Some(lhs - *d as i64), None))),
            _ => Err(DataFusionError::Plan(
                format!("invalid operator '{op}' for timestamp and duration: expected +, -"),
            )),
        }
        DFExpr::Literal(ScalarValue::Int64(_))
        // NOTE: This is a slight deviation from InfluxQL, for which the only valid binary
        // operator for two timestamps is subtraction. By converting the timestamp to a
        // duration and calling this function recursively, we permit the addition operator.
        | DFExpr::Literal(ScalarValue::TimestampNanosecond(..))
        | DFExpr::Literal(ScalarValue::Utf8(_)) => reduce_binary_lhs_timestamp_df_expr(
            lhs,
            op,
            &expr_to_interval_df_expr(rhs, tz)?,
            tz,
        ),
        _ => Err(DataFusionError::Plan(
            format!("invalid expression '{rhs}': expected duration, integer or timestamp string"),
        )),
    }
}

/// Reduce `expr ( + | - ) expr`.
///
/// This API is called when either the left or right hand expression is
/// a scalar function and ensures the operator is either addition or subtraction.
fn reduce_binary_scalar_df_expr(lhs: &DFExpr, op: BinaryOperator, rhs: &DFExpr) -> ExprResult {
    match op {
        BinaryOperator::Add => Ok(binary_expr(lhs.clone(), Operator::Plus, rhs.clone())),
        BinaryOperator::Sub => Ok(binary_expr(lhs.clone(), Operator::Minus, rhs.clone())),
        _ => Err(DataFusionError::Plan(format!(
            "found operator '{op}', expected +, -"
        ))),
    }
}

/// Converts `rhs` to a DataFusion interval literal.
fn expr_to_interval_df_expr(expr: &DFExpr, tz: Option<chrono_tz::Tz>) -> ExprResult {
    Ok(lit(ScalarValue::new_interval_mdn(
        0,
        0,
        match expr {
            DFExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(d))) => *d as i64,
            DFExpr::Literal(ScalarValue::Int64(Some(v))) => *v,
            DFExpr::Literal(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
            DFExpr::Literal(ScalarValue::Utf8(Some(s))) => parse_timestamp_nanos(s, tz)?,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "unable to cast '{expr}' to duration"
                )))
            }
        },
    )))
}

/// Reduce `string OP expr`.
///
/// If `expr` is a string, concatenates the two values and returns a new string.
/// If `expr` is a duration, integer or timestamp, the left-hand
/// string is parsed as a timestamp and the expression evaluated as
/// `timestamp OP expr`
fn reduce_binary_lhs_string_df_expr(
    lhs: &str,
    op: BinaryOperator,
    rhs: &DFExpr,
    tz: Option<chrono_tz::Tz>,
) -> ExprResult {
    match rhs {
        DFExpr::Literal(ScalarValue::Utf8(Some(s))) => match op {
            // concatenate the two strings
            BinaryOperator::Add => Ok(lit(lhs.to_string() + s)),
            _ => reduce_binary_lhs_timestamp_df_expr(parse_timestamp_nanos(lhs, tz)?, op, rhs, tz),
        },
        DFExpr::Literal(ScalarValue::IntervalMonthDayNano(_))
        | DFExpr::Literal(ScalarValue::TimestampNanosecond(..))
        | DFExpr::Literal(ScalarValue::Int64(_)) => {
            reduce_binary_lhs_timestamp_df_expr(parse_timestamp_nanos(lhs, tz)?, op, rhs, tz)
        }
        _ => Err(DataFusionError::Plan(format!(
            "found '{rhs}', expected duration, integer or timestamp string"
        ))),
    }
}

fn parse_timestamp_nanos(s: &str, tz: Option<chrono_tz::Tz>) -> Result<i64> {
    parse_timestamp(s, tz)
        .map(|ts| ts.timestamp_nanos())
        .map_err(|_| DataFusionError::Plan(format!("'{s}' is not a valid timestamp")))
}

/// Parse s as a timestamp in the specified timezone and return the timestamp
/// as a literal timestamp expression.
fn parse_timestamp_df_expr(s: &str, tz: Option<chrono_tz::Tz>) -> ExprResult {
    Ok(lit(ScalarValue::TimestampNanosecond(
        Some(parse_timestamp_nanos(s, tz)?),
        None,
    )))
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;
    use influxdb_influxql_parser::expression::ConditionalExpression;
    use test_helpers::assert_error;

    #[test]
    fn test_rewrite_time_expression_no_timezone() {
        fn process_expr(s: &str) -> ExprResult {
            let cond: ConditionalExpression =
                s.parse().expect("unexpected error parsing expression");
            time_range_to_df_expr(cond.expr().unwrap(), None)
        }

        macro_rules! assert_expr {
            ($S: expr, $EXPECTED: expr) => {
                let expr = process_expr($S).unwrap();
                assert_eq!(expr.to_string(), $EXPECTED);
            };
        }

        //
        // Valid literals
        //

        // Duration
        assert_expr!("1d", "TimestampNanosecond(86400000000000, None)");

        // Single integer interpreted as a Unix nanosecond epoch
        assert_expr!(
            "1157082310000000000",
            "TimestampNanosecond(1157082310000000000, None)"
        );

        // Single float interpreted as a Unix nanosecond epoch
        assert_expr!(
            "1157082310000000000.0",
            "TimestampNanosecond(1157082310000000000, None)"
        );

        // Single string interpreted as a timestamp
        assert_expr!(
            "'2004-04-09 02:33:45.123456789'",
            "TimestampNanosecond(1081478025123456789, None)"
        );

        // now
        assert_expr!("now()", "now()");

        //
        // Expressions
        //

        // now() OP expr
        assert_expr!(
            "now() - 5m",
            r#"now() - IntervalMonthDayNano("300000000000")"#
        );
        assert_expr!(
            "(now() - 5m)",
            r#"now() - IntervalMonthDayNano("300000000000")"#
        );
        assert_expr!(
            "now() - 5m - 60m",
            r#"now() - IntervalMonthDayNano("300000000000") - IntervalMonthDayNano("3600000000000")"#
        );
        assert_expr!("now() - 500", r#"now() - IntervalMonthDayNano("500")"#);
        assert_expr!(
            "now() - (5m + 60m)",
            r#"now() - IntervalMonthDayNano("3900000000000")"#
        );

        // expr OP now()
        assert_expr!(
            "5m - now()",
            r#"IntervalMonthDayNano("300000000000") - now()"#
        );
        assert_expr!(
            "5m + now()",
            r#"IntervalMonthDayNano("300000000000") + now()"#
        );

        // duration OP expr
        assert_expr!("1w3d + 1d", "TimestampNanosecond(950400000000000, None)");
        assert_expr!("1w3d - 1d", "TimestampNanosecond(777600000000000, None)");

        // string OP expr
        assert_expr!(
            "'2004-04-09' - '2004-04-08'",
            "TimestampNanosecond(86400000000000, None)"
        );

        assert_expr!(
            "'2004-04-09' + '02:33:45'",
            "TimestampNanosecond(1081478025000000000, None)"
        );

        // integer OP expr
        assert_expr!(
            "1157082310000000000 - 1s",
            "TimestampNanosecond(1157082309000000000, None)"
        );

        // nested evaluation order
        assert_expr!(
            "now() - (6m - (1m * 5))",
            r#"now() - IntervalMonthDayNano("60000000000")"#
        );

        // Fallible

        use DataFusionError::Plan;
        assert_error!(process_expr("foo + 1"), Plan(ref s) if s == "invalid expression \"foo + 1\": found symbol 'foo', expected now() or a literal duration, float, integer and timestamp string");

        assert_error!(process_expr("'2004-04-09' + false"), Plan(ref s) if s == "invalid expression \"'2004-04-09' + false\": found literal 'false', expected duration, float, integer, or timestamp string");

        assert_error!(process_expr("1s * 1s"), Plan(ref s) if s == "invalid expression \"1000ms * 1000ms\": found operator '*', expected +, -");
        assert_error!(process_expr("1s + 0.5"), Plan(ref s) if s == "invalid expression \"1000ms + 0.5\": found operator '+', expected *, /");

        assert_error!(process_expr("'2004-04-09T'"), Plan(ref s) if s == "invalid expression \"'2004-04-09T'\": '2004-04-09T' is not a valid timestamp");
        assert_error!(process_expr("now() + now()"), Plan(ref s) if s == "invalid expression \"now() + now()\": unable to cast 'now()' to duration");
        assert_error!(process_expr("now() * 1"), Plan(ref s) if s == "invalid expression \"now() * 1\": found operator '*', expected +, -");
        assert_error!(process_expr("'2' + now()"), Plan(ref s) if s == "invalid expression \"'2' + now()\": found 'now()', expected duration, integer or timestamp string");
        assert_error!(process_expr("'2' + '3'"), Plan(ref s) if s == "invalid expression \"'2' + '3'\": '23' is not a valid timestamp");
        assert_error!(process_expr("'2' + '3' + 10s"), Plan(ref s) if s == "invalid expression \"'2' + '3' + 10s\": '23' is not a valid timestamp");
    }

    #[test]
    fn test_rewrite_time_expression_with_timezone() {
        fn process_expr(s: &str) -> ExprResult {
            let cond: ConditionalExpression =
                s.parse().expect("unexpected error parsing expression");
            time_range_to_df_expr(cond.expr().unwrap(), Some(chrono_tz::Australia::Hobart))
        }

        macro_rules! assert_expr {
            ($S: expr, $EXPECTED: expr) => {
                let expr = process_expr($S).unwrap();
                assert_eq!(expr.to_string(), $EXPECTED);
            };
        }

        assert_expr!(
            "'2004-04-09 10:05:00.123456789'",
            "TimestampNanosecond(1081469100123456789, None)" // 2004-04-09T00:05:00.123456789Z
        );
        assert_expr!(
            "'2004-04-09'",
            "TimestampNanosecond(1081432800000000000, None)" // 2004-04-08T14:00:00Z
        );
        assert_expr!(
            "'2004-04-09T10:05:00.123456789Z'",
            "TimestampNanosecond(1081505100123456789, None)" // 2004-04-09T10:05:00.123456789Z
        );
    }

    #[test]
    fn test_expr_to_df_interval_dt() {
        fn parse(s: &str) -> ExprResult {
            let expr = s
                .parse::<ConditionalExpression>()
                .unwrap()
                .expr()
                .unwrap()
                .clone();
            expr_to_df_interval_dt(&expr)
        }

        use ScalarValue::IntervalDayTime;

        assert_matches!(parse("10s").unwrap(), DFExpr::Literal(IntervalDayTime(v)) if IntervalDayTime(v) == ScalarValue::new_interval_dt(0, 10_000));
        assert_matches!(parse("10s + 1d").unwrap(), DFExpr::Literal(IntervalDayTime(v)) if IntervalDayTime(v) == ScalarValue::new_interval_dt(1, 10_000));
        assert_matches!(parse("5d10ms").unwrap(), DFExpr::Literal(IntervalDayTime(v)) if IntervalDayTime(v) == ScalarValue::new_interval_dt(5, 10));
        assert_matches!(parse("-2d10ms").unwrap(), DFExpr::Literal(IntervalDayTime(v)) if IntervalDayTime(v) == ScalarValue::new_interval_dt(-2, 10));

        // Fallible

        use DataFusionError::NotImplemented;

        // Don't support a precision greater than milliseconds.
        //
        // See: https://github.com/influxdata/influxdb_iox/issues/7204
        assert_error!(parse("-2d10ns"), NotImplemented(ref s) if s == "interval limited to a precision of milliseconds. See https://github.com/influxdata/influxdb_iox/issues/7204");
    }
}
