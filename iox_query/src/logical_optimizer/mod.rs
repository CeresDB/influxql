use std::sync::Arc;

use datafusion::execution::context::SessionState;

use self::{
    handle_gapfill::HandleGapFill, influx_regex_to_datafusion_regex::InfluxRegexToDataFusionRegex,
};

mod handle_gapfill;
mod influx_regex_to_datafusion_regex;
pub use handle_gapfill::range_predicate;

/// Register IOx-specific logical [`OptimizerRule`]s with the SessionContext
///
/// [`OptimizerRule`]: datafusion::optimizer::OptimizerRule
pub fn register_iox_logical_optimizers(state: SessionState) -> SessionState {
    state
        .add_optimizer_rule(Arc::new(InfluxRegexToDataFusionRegex::new()))
        .add_optimizer_rule(Arc::new(HandleGapFill::new()))
}
