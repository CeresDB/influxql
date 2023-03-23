// NOTE: This code is copied from DataFusion, as it is not public,
// so all warnings are disabled.
#![allow(warnings)]
#![allow(clippy::all)]
//! A collection of utility functions copied from DataFusion.
//!
//! If these APIs are stabilised and made public, they can be removed from IOx.
//!
//! NOTE
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    expr::{
        AggregateFunction, Between, BinaryExpr, Case, Cast, Expr, GetIndexedField, GroupingSet,
        Like, Sort, TryCast, WindowFunction,
    },
    utils::expr_as_column_expr,
    LogicalPlan,
};

/// Rebuilds an `Expr` as a projection on top of a collection of `Expr`'s.
///
/// For example, the expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base expressions already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
///
/// Source: <https://github.com/apache/arrow-datafusion/blob/e6d71068474f3b2ef9ad5e9af85f56f0d0560a1b/datafusion/sql/src/utils.rs#L63>
pub(crate) fn rebase_expr(expr: &Expr, base_exprs: &[Expr], plan: &LogicalPlan) -> Result<Expr> {
    clone_with_replacement(expr, &|nested_expr| {
        if base_exprs.contains(nested_expr) {
            Ok(Some(expr_as_column_expr(nested_expr, plan)?))
        } else {
            Ok(None)
        }
    })
}

/// Returns a cloned `Expr`, but any of the `Expr`'s in the tree may be
/// replaced/customized by the replacement function.
///
/// The replacement function is called repeatedly with `Expr`, starting with
/// the argument `expr`, then descending depth-first through its
/// descendants. The function chooses to replace or keep (clone) each `Expr`.
///
/// The function's return type is `Result<Option<Expr>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `Expr` is provided; it is
///       swapped in at the particular node in the tree. Any nested `Expr` are
///       not subject to cloning/replacement.
/// * `Ok(None)`: A replacement `Expr` is not provided. The `Expr` is
///       recreated, with all of its nested `Expr`'s subject to
///       cloning/replacement.
/// * `Err(err)`: Any error returned by the function is returned as-is by
///       `clone_with_replacement()`.
///
/// Source: <https://github.com/apache/arrow-datafusion/blob/26e1b20ea/datafusion/sql/src/utils.rs#L153>
fn clone_with_replacement<F>(expr: &Expr, replacement_fn: &F) -> Result<Expr>
where
    F: Fn(&Expr) -> Result<Option<Expr>>,
{
    let replacement_opt = replacement_fn(expr)?;

    match replacement_opt {
        // If we were provided a replacement, use the replacement. Do not
        // descend further.
        Some(replacement) => Ok(replacement),
        // No replacement was provided, clone the node and recursively call
        // clone_with_replacement() on any nested expressions.
        None => {
            match expr {
                Expr::AggregateFunction(AggregateFunction {
                    fun,
                    args,
                    distinct,
                    filter,
                }) => Ok(Expr::AggregateFunction(AggregateFunction::new(
                    fun.clone(),
                    args.iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    *distinct,
                    filter.clone(),
                ))),
                Expr::WindowFunction(WindowFunction {
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                }) => Ok(Expr::WindowFunction(WindowFunction::new(
                    fun.clone(),
                    args.iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<_>>>()?,
                    partition_by
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<_>>>()?,
                    order_by
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<_>>>()?,
                    window_frame.clone(),
                ))),
                Expr::AggregateUDF { fun, args, filter } => Ok(Expr::AggregateUDF {
                    fun: fun.clone(),
                    args: args
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    filter: filter.clone(),
                }),
                Expr::Alias(nested_expr, alias_name) => Ok(Expr::Alias(
                    Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    alias_name.clone(),
                )),
                Expr::Between(Between {
                    expr,
                    negated,
                    low,
                    high,
                }) => Ok(Expr::Between(Between::new(
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    *negated,
                    Box::new(clone_with_replacement(low, replacement_fn)?),
                    Box::new(clone_with_replacement(high, replacement_fn)?),
                ))),
                Expr::InList {
                    expr: nested_expr,
                    list,
                    negated,
                } => Ok(Expr::InList {
                    expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    list: list
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                    negated: *negated,
                }),
                Expr::BinaryExpr(BinaryExpr { left, right, op }) => {
                    Ok(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(clone_with_replacement(left, replacement_fn)?),
                        *op,
                        Box::new(clone_with_replacement(right, replacement_fn)?),
                    )))
                }
                Expr::Like(Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                }) => Ok(Expr::Like(Like::new(
                    *negated,
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    Box::new(clone_with_replacement(pattern, replacement_fn)?),
                    *escape_char,
                ))),
                Expr::ILike(Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                }) => Ok(Expr::ILike(Like::new(
                    *negated,
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    Box::new(clone_with_replacement(pattern, replacement_fn)?),
                    *escape_char,
                ))),
                Expr::SimilarTo(Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                }) => Ok(Expr::SimilarTo(Like::new(
                    *negated,
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    Box::new(clone_with_replacement(pattern, replacement_fn)?),
                    *escape_char,
                ))),
                Expr::Case(case) => Ok(Expr::Case(Case::new(
                    match &case.expr {
                        Some(case_expr) => {
                            Some(Box::new(clone_with_replacement(case_expr, replacement_fn)?))
                        }
                        None => None,
                    },
                    case.when_then_expr
                        .iter()
                        .map(|(a, b)| {
                            Ok((
                                Box::new(clone_with_replacement(a, replacement_fn)?),
                                Box::new(clone_with_replacement(b, replacement_fn)?),
                            ))
                        })
                        .collect::<Result<Vec<(_, _)>>>()?,
                    match &case.else_expr {
                        Some(else_expr) => {
                            Some(Box::new(clone_with_replacement(else_expr, replacement_fn)?))
                        }
                        None => None,
                    },
                ))),
                Expr::ScalarFunction { fun, args } => Ok(Expr::ScalarFunction {
                    fun: fun.clone(),
                    args: args
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                }),
                Expr::ScalarUDF { fun, args } => Ok(Expr::ScalarUDF {
                    fun: fun.clone(),
                    args: args
                        .iter()
                        .map(|arg| clone_with_replacement(arg, replacement_fn))
                        .collect::<Result<Vec<Expr>>>()?,
                }),
                Expr::Negative(nested_expr) => Ok(Expr::Negative(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::Not(nested_expr) => Ok(Expr::Not(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsNotNull(nested_expr) => Ok(Expr::IsNotNull(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNull(nested_expr) => Ok(Expr::IsNull(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsTrue(nested_expr) => Ok(Expr::IsTrue(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsFalse(nested_expr) => Ok(Expr::IsFalse(Box::new(clone_with_replacement(
                    nested_expr,
                    replacement_fn,
                )?))),
                Expr::IsUnknown(nested_expr) => Ok(Expr::IsUnknown(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNotTrue(nested_expr) => Ok(Expr::IsNotTrue(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNotFalse(nested_expr) => Ok(Expr::IsNotFalse(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::IsNotUnknown(nested_expr) => Ok(Expr::IsNotUnknown(Box::new(
                    clone_with_replacement(nested_expr, replacement_fn)?,
                ))),
                Expr::Cast(Cast { expr, data_type }) => Ok(Expr::Cast(Cast::new(
                    Box::new(clone_with_replacement(expr, replacement_fn)?),
                    data_type.clone(),
                ))),
                Expr::TryCast(TryCast {
                    expr: nested_expr,
                    data_type,
                }) => Ok(Expr::TryCast(TryCast::new(
                    Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    data_type.clone(),
                ))),
                Expr::Sort(Sort {
                    expr: nested_expr,
                    asc,
                    nulls_first,
                }) => Ok(Expr::Sort(Sort::new(
                    Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    *asc,
                    *nulls_first,
                ))),
                Expr::Column { .. }
                | Expr::OuterReferenceColumn(_, _)
                | Expr::Literal(_)
                | Expr::ScalarVariable(_, _)
                | Expr::Exists { .. }
                | Expr::ScalarSubquery(_) => Ok(expr.clone()),
                Expr::InSubquery {
                    expr: nested_expr,
                    subquery,
                    negated,
                } => Ok(Expr::InSubquery {
                    expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                    subquery: subquery.clone(),
                    negated: *negated,
                }),
                Expr::Wildcard => Ok(Expr::Wildcard),
                Expr::QualifiedWildcard { .. } => Ok(expr.clone()),
                Expr::GetIndexedField(GetIndexedField { key, expr }) => {
                    Ok(Expr::GetIndexedField(GetIndexedField::new(
                        Box::new(clone_with_replacement(expr.as_ref(), replacement_fn)?),
                        key.clone(),
                    )))
                }
                Expr::GroupingSet(set) => match set {
                    GroupingSet::Rollup(exprs) => Ok(Expr::GroupingSet(GroupingSet::Rollup(
                        exprs
                            .iter()
                            .map(|e| clone_with_replacement(e, replacement_fn))
                            .collect::<Result<Vec<Expr>>>()?,
                    ))),
                    GroupingSet::Cube(exprs) => Ok(Expr::GroupingSet(GroupingSet::Cube(
                        exprs
                            .iter()
                            .map(|e| clone_with_replacement(e, replacement_fn))
                            .collect::<Result<Vec<Expr>>>()?,
                    ))),
                    GroupingSet::GroupingSets(lists_of_exprs) => {
                        let mut new_lists_of_exprs = vec![];
                        for exprs in lists_of_exprs {
                            new_lists_of_exprs.push(
                                exprs
                                    .iter()
                                    .map(|e| clone_with_replacement(e, replacement_fn))
                                    .collect::<Result<Vec<Expr>>>()?,
                            );
                        }
                        Ok(Expr::GroupingSet(GroupingSet::GroupingSets(
                            new_lists_of_exprs,
                        )))
                    }
                },
                Expr::Placeholder { id, data_type } => Ok(Expr::Placeholder {
                    id: id.clone(),
                    data_type: data_type.clone(),
                }),
            }
        }
    }
}
