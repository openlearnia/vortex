// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Formatter;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::expr::display::ExprDisplay;
use vortex_array::scalar_fn::Arity;
use vortex_array::scalar_fn::ChildName;
use vortex_array::scalar_fn::EmptyOptions;
use vortex_array::scalar_fn::ExecutionArgs;
use vortex_array::scalar_fn::ScalarFnId;
use vortex_array::scalar_fn::ScalarFnVTable;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_session::registry::CachedId;

/// Zero-argument placeholder for the row count of the current evaluation scope.
///
/// Stats rewrite rules emit `RowCount` when a proof needs a scope-level value that is not stored
/// as a regular stats column — `is_not_null` is falsified by `null_count == row_count`, for
/// example. Keeping it as a placeholder lets a rewrite rule name the row count without knowing
/// anything about where the stats it sits beside are stored.
///
/// It is resolved during stat binding, by [`bind_stats`], which asks the [`StatBinder`] for the
/// row count of its scope. Binding is a single top-down pass that recurses into the expressions
/// it substitutes, so a binder may itself emit `RowCount` and have it resolved in the same pass.
///
/// This expression *MUST* be replaced before evaluation; calling
/// [`ScalarFnVTable::execute`] directly returns an error because this node is only a marker in a
/// lazy expression tree.
///
/// [`bind_stats`]: crate::stats::bind::bind_stats
/// [`StatBinder`]: crate::stats::bind::StatBinder
#[derive(Clone)]
pub struct RowCount;

impl ScalarFnVTable for RowCount {
    type Options = EmptyOptions;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.row_count");
        *ID
    }

    fn arity(&self, _options: &Self::Options) -> Arity {
        Arity::Exact(0)
    }

    fn child_name(&self, _options: &Self::Options, _child_idx: usize) -> ChildName {
        unreachable!("RowCount has arity 0")
    }

    fn fmt_sql(
        &self,
        _options: &Self::Options,
        _expr: &dyn ExprDisplay,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "row_count()")
    }

    fn return_dtype(&self, _options: &Self::Options, _args: &[DType]) -> VortexResult<DType> {
        Ok(DType::Primitive(PType::U64, Nullability::NonNullable))
    }

    fn execute(
        &self,
        _options: &Self::Options,
        _args: &dyn ExecutionArgs,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        vortex_bail!("RowCount must be substituted before evaluation")
    }

    fn is_strict(&self, _options: &Self::Options) -> bool {
        true
    }

    fn is_fallible(&self, _options: &Self::Options) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;

    use crate::scalar_fn::EmptyOptions;
    use crate::scalar_fn::internal::row_count::RowCount;
    use crate::scalar_fn::vtable::ScalarFnVTableExt;

    #[test]
    fn row_count_helper_dtype() {
        let expr = RowCount.new_expr(EmptyOptions, []);
        assert_eq!(
            expr.return_dtype(&DType::Primitive(PType::I32, Nullability::Nullable))
                .unwrap(),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        );
    }
}
