//! Test helpers for constraints that use ValidationContext.

use crate::core::{
    validation_context::CURRENT_CONTEXT, Constraint, ConstraintResult, ValidationContext,
};
use datafusion::prelude::SessionContext;

/// Evaluates a constraint with the given table name in the validation context.
pub async fn evaluate_constraint_with_context(
    constraint: &dyn Constraint,
    ctx: &SessionContext,
    table_name: &str,
) -> Result<ConstraintResult, Box<dyn std::error::Error>> {
    let validation_ctx = ValidationContext::new(table_name.to_string());

    // Use the scope method to set the context for the duration of the evaluation
    let result = CURRENT_CONTEXT
        .scope(validation_ctx, constraint.evaluate(ctx))
        .await?;

    Ok(result)
}
