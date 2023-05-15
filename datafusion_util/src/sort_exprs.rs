use datafusion::physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};

/// Structure to build [`PhysicalSortRequirement`]s for ExecutionPlans.
///
/// Replace with `PhysicalSortExpr::from_sort_exprs` when
/// <https://github.com/apache/arrow-datafusion/pull/5863> is merged
/// upstream.
pub fn requirements_from_sort_exprs<'a>(
    exprs: impl IntoIterator<Item = &'a PhysicalSortExpr>,
) -> Vec<PhysicalSortRequirement> {
    exprs
        .into_iter()
        .cloned()
        .map(PhysicalSortRequirement::from)
        .collect()
}
