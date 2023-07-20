//! This module contains code that implements
//! a gap-filling extension to DataFusion

mod algo;
#[cfg(test)]
mod exec_tests;
mod params;
mod stream;

use std::{
    fmt::{self, Debug},
    ops::{Bound, Range},
    sync::Arc,
};

use arrow::{compute::SortOptions, datatypes::SchemaRef};
use datafusion::{
    common::DFSchemaRef,
    error::{DataFusionError, Result},
    execution::{context::TaskContext, memory_pool::MemoryConsumer},
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    physical_expr::{
        create_physical_expr, execution_props::ExecutionProps, PhysicalSortExpr,
        PhysicalSortRequirement,
    },
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        SendableRecordBatchStream, Statistics,
    },
    prelude::Expr,
};
use datafusion_util::sort_exprs::requirements_from_sort_exprs;

use self::stream::GapFillStream;

/// A logical node that represents the gap filling operation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFill {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// Parameters to configure the behavior of the
    /// gap-filling operation
    pub params: GapFillParams,
}

/// Parameters to the GapFill operation
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFillParams {
    /// The stride argument from the call to DATE_BIN_GAPFILL
    pub stride: Expr,
    /// The source time column
    pub time_column: Expr,
    /// The origin argument from the call to DATE_BIN_GAPFILL
    pub origin: Expr,
    /// The time range of the time column inferred from predicates
    /// in the overall query. The lower bound may be [`Bound::Unbounded`]
    /// which implies that gap-filling should just start from the
    /// first point in each series.
    pub time_range: Range<Bound<Expr>>,
    /// What to do when filling aggregate columns.
    /// The first item in the tuple will be the column
    /// reference for the aggregate column.
    pub fill_strategy: Vec<(Expr, FillStrategy)>,
}

/// Describes how to fill gaps in an aggregate column.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum FillStrategy {
    /// Fill with null values.
    /// This is the InfluxQL behavior for `FILL(NULL)` or `FILL(NONE)`.
    Null,
    /// Fill with the most recent value in the input column.
    /// Null values in the input are preserved.
    #[allow(dead_code)]
    PrevNullAsIntentional,
    /// Fill with the most recent non-null value in the input column.
    /// This is the InfluxQL behavior for `FILL(PREVIOUS)`.
    PrevNullAsMissing,
    /// Fill the gaps between points linearly.
    /// Null values will not be considered as missing, so two non-null values
    /// with a null in between will not be filled.
    #[allow(dead_code)]
    LinearInterpolate,
}

impl GapFillParams {
    // Extract the expressions so they can be optimized.
    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![
            self.stride.clone(),
            self.time_column.clone(),
            self.origin.clone(),
        ];
        if let Some(start) = bound_extract(&self.time_range.start) {
            exprs.push(start.clone());
        }
        exprs.push(
            bound_extract(&self.time_range.end)
                .unwrap_or_else(|| panic!("upper time bound is required"))
                .clone(),
        );
        exprs
    }

    #[allow(clippy::wrong_self_convention)] // follows convention of UserDefinedLogicalNode
    fn from_template(&self, exprs: &[Expr], aggr_expr: &[Expr]) -> Self {
        assert!(
            exprs.len() >= 3,
            "should be a at least stride, source and origin in params"
        );
        let mut iter = exprs.iter().cloned();
        let stride = iter.next().unwrap();
        let time_column = iter.next().unwrap();
        let origin = iter.next().unwrap();
        let time_range = try_map_range(&self.time_range, |b| {
            try_map_bound(b.as_ref(), |_| {
                Ok(iter.next().expect("expr count should match template"))
            })
        })
        .unwrap();

        let fill_strategy = aggr_expr
            .iter()
            .cloned()
            .zip(
                self.fill_strategy
                    .iter()
                    .map(|(_expr, fill_strategy)| fill_strategy)
                    .cloned(),
            )
            .collect();

        Self {
            stride,
            time_column,
            origin,
            time_range,
            fill_strategy,
        }
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    fn replace_fill_strategy(&mut self, e: &Expr, mut fs: FillStrategy) -> Option<FillStrategy> {
        for expr_fs in &mut self.fill_strategy {
            if &expr_fs.0 == e {
                std::mem::swap(&mut fs, &mut expr_fs.1);
                return Some(fs);
            }
        }
        None
    }
}

impl GapFill {
    /// Create a new gap-filling operator.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        params: GapFillParams,
    ) -> Result<Self> {
        if params.time_range.end == Bound::Unbounded {
            return Err(DataFusionError::Internal(
                "missing upper bound in GapFill time range".to_string(),
            ));
        }
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            params,
        })
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    pub(crate) fn replace_fill_strategy(
        &mut self,
        e: &Expr,
        fs: FillStrategy,
    ) -> Option<FillStrategy> {
        self.params.replace_fill_strategy(e, fs)
    }
}

impl UserDefinedLogicalNodeCore for GapFill {
    fn name(&self) -> &str {
        "GapFill"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.group_expr
            .iter()
            .chain(&self.aggr_expr)
            .chain(&self.params.expressions())
            .cloned()
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let aggr_expr: String = self
            .params
            .fill_strategy
            .iter()
            .map(|(e, fs)| match fs {
                FillStrategy::PrevNullAsIntentional => format!("LOCF(null-as-intentional, {e})"),
                FillStrategy::PrevNullAsMissing => format!("LOCF({e})"),
                FillStrategy::LinearInterpolate => format!("INTERPOLATE({e})"),
                FillStrategy::Null => e.to_string(),
            })
            .collect::<Vec<String>>()
            .join(", ");
        write!(
            f,
            "{}: groupBy=[{:?}], aggr=[[{}]], time_column={}, stride={}, range={:?}",
            self.name(),
            self.group_expr,
            aggr_expr,
            self.params.time_column,
            self.params.stride,
            self.params.time_range,
        )
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        let mut group_expr: Vec<_> = exprs.to_vec();
        let mut aggr_expr = group_expr.split_off(self.group_expr.len());
        let param_expr = aggr_expr.split_off(self.aggr_expr.len());
        let params = self.params.from_template(&param_expr, &aggr_expr);
        Self::try_new(Arc::new(inputs[0].clone()), group_expr, aggr_expr, params)
            .expect("should not fail")
    }
}

/// Called by the extension planner to plan a [GapFill] node.
pub(crate) fn plan_gap_fill(
    execution_props: &ExecutionProps,
    gap_fill: &GapFill,
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<GapFillExec> {
    if logical_inputs.len() != 1 {
        return Err(DataFusionError::Internal(
            "GapFillExec: wrong number of logical inputs".to_string(),
        ));
    }
    if physical_inputs.len() != 1 {
        return Err(DataFusionError::Internal(
            "GapFillExec: wrong number of physical inputs".to_string(),
        ));
    }

    let input_dfschema = logical_inputs[0].schema().as_ref();
    let input_schema = physical_inputs[0].schema();
    let input_schema = input_schema.as_ref();

    let group_expr: Result<Vec<_>> = gap_fill
        .group_expr
        .iter()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .collect();
    let group_expr = group_expr?;

    let aggr_expr: Result<Vec<_>> = gap_fill
        .aggr_expr
        .iter()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .collect();
    let aggr_expr = aggr_expr?;

    let logical_time_column = gap_fill.params.time_column.try_into_col()?;
    let time_column = Column::new_with_schema(&logical_time_column.name, input_schema)?;

    let stride = create_physical_expr(
        &gap_fill.params.stride,
        input_dfschema,
        input_schema,
        execution_props,
    )?;

    let time_range = &gap_fill.params.time_range;
    let time_range = try_map_range(time_range, |b| {
        try_map_bound(b.as_ref(), |e| {
            create_physical_expr(e, input_dfschema, input_schema, execution_props)
        })
    })?;

    let origin = create_physical_expr(
        &gap_fill.params.origin,
        input_dfschema,
        input_schema,
        execution_props,
    )?;

    let fill_strategy = gap_fill
        .params
        .fill_strategy
        .iter()
        .map(|(e, fs)| {
            Ok((
                create_physical_expr(e, input_dfschema, input_schema, execution_props)?,
                fs.clone(),
            ))
        })
        .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>>>()?;

    let params = GapFillExecParams {
        stride,
        time_column,
        origin,
        time_range,
        fill_strategy,
    };
    GapFillExec::try_new(
        Arc::clone(&physical_inputs[0]),
        group_expr,
        aggr_expr,
        params,
    )
}

fn try_map_range<T, U, F>(tr: &Range<T>, mut f: F) -> Result<Range<U>>
where
    F: FnMut(&T) -> Result<U>,
{
    Ok(Range {
        start: f(&tr.start)?,
        end: f(&tr.end)?,
    })
}

fn try_map_bound<T, U, F>(bt: Bound<T>, mut f: F) -> Result<Bound<U>>
where
    F: FnMut(T) -> Result<U>,
{
    Ok(match bt {
        Bound::Excluded(t) => Bound::Excluded(f(t)?),
        Bound::Included(t) => Bound::Included(f(t)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

fn bound_extract<T>(b: &Bound<T>) -> Option<&T> {
    match b {
        Bound::Included(t) | Bound::Excluded(t) => Some(t),
        Bound::Unbounded => None,
    }
}

/// A physical node for the gap-fill operation.
pub struct GapFillExec {
    input: Arc<dyn ExecutionPlan>,
    // The group by expressions from the original aggregation node.
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The aggregate expressions from the original aggregation node.
    aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The sort expressions for the required sort order of the input:
    // all of the group exressions, with the time column being last.
    sort_expr: Vec<PhysicalSortExpr>,
    // Parameters (besides streaming data) to gap filling
    params: GapFillExecParams,
    /// Metrics reporting behavior during execution.
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Clone, Debug)]
struct GapFillExecParams {
    /// The uniform interval of incoming timestamps
    stride: Arc<dyn PhysicalExpr>,
    /// The timestamp column produced by date_bin
    time_column: Column,
    /// The origin argument from the all to DATE_BIN_GAPFILL
    origin: Arc<dyn PhysicalExpr>,
    /// The time range of source input to DATE_BIN_GAPFILL.
    /// Inferred from predicates in the overall query.
    time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
    /// What to do when filling aggregate columns.
    /// The 0th element in each tuple is the aggregate column.
    fill_strategy: Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>,
}

impl GapFillExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
        params: GapFillExecParams,
    ) -> Result<Self> {
        let sort_expr = {
            let mut sort_expr: Vec<_> = group_expr
                .iter()
                .map(|expr| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: SortOptions::default(),
                })
                .collect();

            // Ensure that the time column is the last component in the sort
            // expressions.
            let time_idx = group_expr
                .iter()
                .enumerate()
                .find(|(_i, e)| {
                    e.as_any()
                        .downcast_ref::<Column>()
                        .map_or(false, |c| c.index() == params.time_column.index())
                })
                .map(|(i, _)| i);

            if let Some(time_idx) = time_idx {
                let last_elem = sort_expr.len() - 1;
                sort_expr.swap(time_idx, last_elem);
            } else {
                return Err(DataFusionError::Internal(
                    "could not find time column for GapFillExec".to_string(),
                ));
            }

            sort_expr
        };

        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            sort_expr,
            params,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl Debug for GapFillExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GapFillExec")
    }
}

impl ExecutionPlan for GapFillExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // It seems like it could be possible to partition on all the
        // group keys except for the time expression. For now, keep it simple.
        vec![Distribution::SinglePartition]
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(requirements_from_sort_exprs(&self.sort_expr))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self::try_new(
                Arc::clone(&children[0]),
                self.group_expr.clone(),
                self.aggr_expr.clone(),
                self.params.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "GapFillExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec invalid partition {partition}, there can be only one partition"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_batch_size = context.session_config().batch_size();
        let reservation = MemoryConsumer::new(format!("GapFillExec[{partition}]"))
            .register(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(GapFillStream::try_new(
            self,
            output_batch_size,
            input_stream,
            reservation,
            baseline_metrics,
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for GapFillExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let group_expr: Vec<_> = self.group_expr.iter().map(|e| e.to_string()).collect();
                let aggr_expr: Vec<_> = self
                    .params
                    .fill_strategy
                    .iter()
                    .map(|(e, fs)| match fs {
                        FillStrategy::PrevNullAsIntentional => {
                            format!("LOCF(null-as-intentional, {})", e)
                        }
                        FillStrategy::PrevNullAsMissing => format!("LOCF({})", e),
                        FillStrategy::LinearInterpolate => format!("INTERPOLATE({})", e),
                        FillStrategy::Null => e.to_string(),
                    })
                    .collect();
                let time_range = try_map_range(&self.params.time_range, |b| {
                    try_map_bound(b.as_ref(), |e| Ok(e.to_string()))
                })
                .map_err(|_| fmt::Error {})?;
                write!(
                    f,
                    "GapFillExec: group_expr=[{}], aggr_expr=[{}], stride={}, time_range={:?}",
                    group_expr.join(", "),
                    aggr_expr.join(", "),
                    self.params.stride,
                    time_range
                )
            }
        }
    }
}
