use datafusion::error::{DataFusionError as Error, Result};
use datafusion::{logical_expr::LogicalPlan, physical_plan::ExecutionPlan, prelude::*};
use std::sync::Arc;

/// Configuration for an IOx execution context
///
/// Created from an Executor
#[derive(Clone)]
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
        // Make nicer erorrs for unsupported SQL
        // (By default datafusion returns Internal Error)
        match &logical_plan {
            LogicalPlan::CreateCatalog(_) => {
                return Err(Error::NotImplemented("CreateCatalog".to_string()));
            }
            LogicalPlan::CreateCatalogSchema(_) => {
                return Err(Error::NotImplemented("CreateCatalogSchema".to_string()));
            }
            LogicalPlan::CreateMemoryTable(_) => {
                return Err(Error::NotImplemented("CreateMemoryTable".to_string()));
            }
            LogicalPlan::DropTable(_) => {
                return Err(Error::NotImplemented("DropTable".to_string()));
            }
            LogicalPlan::DropView(_) => {
                return Err(Error::NotImplemented("DropView".to_string()));
            }
            LogicalPlan::CreateView(_) => {
                return Err(Error::NotImplemented("CreateView".to_string()));
            }
            _ => (),
        }

        let ctx = self.child_ctx("create_physical_plan");
        let physical_plan = ctx.inner.state().create_physical_plan(logical_plan).await?;

        Ok(physical_plan)
    }

    /// Returns a IOxSessionContext with a SpanRecorder that is a child of the current
    pub fn child_ctx(&self, _name: &'static str) -> Self {
        Self::new(self.inner.clone())
    }
}
