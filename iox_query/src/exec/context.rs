use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_planner::{PhysicalPlanner, DefaultPhysicalPlanner, ExtensionPlanner};
use datafusion::{logical_expr::LogicalPlan, physical_plan::ExecutionPlan, prelude::*};
use std::sync::Arc;

use crate::exec::non_null_checker::NonNullCheckerExec;
use crate::exec::schema_pivot::SchemaPivotExec;
use crate::exec::split::StreamSplitExec;

use super::gapfill::{plan_gap_fill, GapFill};
use super::non_null_checker::NonNullCheckerNode;
use super::schema_pivot::SchemaPivotNode;
use super::split::StreamSplitNode;

/// This structure implements the DataFusion notion of "query planner"
/// and is needed to create plans with the IOx extension nodes.
struct IOxQueryPlanner {}

#[async_trait]
impl QueryPlanner for IOxQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan SchemaPivot
        // and StreamSplit nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(IOxExtensionPlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Physical planner for InfluxDB IOx extension plans
pub struct IOxExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for IOxExtensionPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(schema_pivot) = any.downcast_ref::<SchemaPivotNode>() {
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            Some(Arc::new(SchemaPivotExec::new(
                Arc::clone(&physical_inputs[0]),
                schema_pivot.schema().as_ref().clone().into(),
            )) as Arc<dyn ExecutionPlan>)
        } else if let Some(non_null_checker) = any.downcast_ref::<NonNullCheckerNode>() {
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            Some(Arc::new(NonNullCheckerExec::new(
                Arc::clone(&physical_inputs[0]),
                non_null_checker.schema().as_ref().clone().into(),
                non_null_checker.value(),
            )) as Arc<dyn ExecutionPlan>)
        } else if let Some(stream_split) = any.downcast_ref::<StreamSplitNode>() {
            assert_eq!(
                logical_inputs.len(),
                1,
                "Inconsistent number of logical inputs"
            );
            assert_eq!(
                physical_inputs.len(),
                1,
                "Inconsistent number of physical inputs"
            );

            let split_exprs = stream_split
                .split_exprs()
                .iter()
                .map(|e| {
                    planner.create_physical_expr(
                        e,
                        logical_inputs[0].schema(),
                        &physical_inputs[0].schema(),
                        session_state,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            Some(Arc::new(StreamSplitExec::new(
                Arc::clone(&physical_inputs[0]),
                split_exprs,
            )) as Arc<dyn ExecutionPlan>)
        } else if let Some(gap_fill) = any.downcast_ref::<GapFill>() {
            let gap_fill_exec = plan_gap_fill(
                session_state.execution_props(),
                gap_fill,
                logical_inputs,
                physical_inputs,
            )?;
            Some(Arc::new(gap_fill_exec) as Arc<dyn ExecutionPlan>)
        } else {
            None
        };
        Ok(plan)
    }
}

/// Configuration for an IOx execution context
///
/// Created from an Executor
/// We just keep it for here building without error so many contents have been removed.
pub struct IOxSessionContext {
    inner: SessionContext,
}

impl IOxSessionContext {
    pub fn new(inner: SessionContext) -> Self {
        Self { inner }
    }

    /// returns a reference to the inner datafusion execution context
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    /// Prepare (optimize + plan) a pre-created [`LogicalPlan`] for execution
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.child_ctx("create_physical_plan");
        let physical_plan = ctx.inner.state().create_physical_plan(logical_plan).await?;

        Ok(physical_plan)
    }

    /// Returns a IOxSessionContext with a SpanRecorder that is a child of the current
    pub fn child_ctx(&self, _name: &'static str) -> Self {
        Self::new(self.inner.clone())
    }
}
