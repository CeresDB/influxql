//! This module contains DataFusion utility functions and helpers

use std::sync::Arc;

use datafusion::{
    self,
    common::ToDFSchema,
    error::DataFusionError,
    execution::context::ExecutionProps,
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::create_physical_expr,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};

use observability_deps::tracing::trace;

/// Build a datafusion physical expression from a logical one
pub fn df_physical_expr(
    input: &dyn ExecutionPlan,
    expr: Expr,
) -> std::result::Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let schema = input.schema();

    let df_schema = Arc::clone(&schema).to_dfschema_ref()?;

    let props = ExecutionProps::new();
    let simplifier =
        ExprSimplifier::new(SimplifyContext::new(&props).with_schema(Arc::clone(&df_schema)));

    // apply type coercion here to ensure types match
    trace!(%df_schema, "input schema");
    let expr = simplifier.coerce(expr, Arc::clone(&df_schema))?;
    trace!(%expr, "coerced logical expression");

    create_physical_expr(&expr, df_schema.as_ref(), schema.as_ref(), &props)
}
