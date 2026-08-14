// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Bind abstract `vortex.stat` expressions to a concrete stats representation.
//!
//! Stats rewrite rules describe pruning in terms of `vortex.stat(input, aggregate_fn)` placeholders
//! so the rewrite is independent of where statistics are stored. These stat placeholders are
//! abstract because they name the statistic needed for a proof, but not how that statistic is
//! represented by a specific layout or reader.
//!
//! Binding is the later pass that replaces each abstract placeholder with the representation used
//! by a caller: zone-map field references, file-level stat literals, or typed nulls for missing
//! stats. This lets all callers share the same falsification rules while keeping layout-specific
//! stat storage behind [`StatBinder`].
//!
//! Binding also resolves [`RowCount`] placeholders, which rewrite rules emit when a proof needs
//! the number of rows the scope covers rather than a stored statistic.
//!
//! Rewrite rules are independent and are combined with `or`, so several of them may prove the same
//! thing through different statistics — `is_not_null` is falsified both by
//! `null_count == row_count` and by `all_null`. Which of those a stats source can actually answer
//! is only known here, and a source that answers both from the same column lowers them to the same
//! expression. Binding collapses those duplicates on the way back up, so the predicate is not
//! evaluated twice per row.

use vortex_error::VortexResult;

use crate::aggregate_fn::AggregateFnRef;
use crate::dtype::DType;
use crate::expr::BoundExpression;
use crate::expr::bound::lit;
use crate::expr::traversal::NodeExt;
use crate::expr::traversal::Transformed;
use crate::scalar::Scalar;
use crate::scalar_fn::fns::binary::Binary;
use crate::scalar_fn::fns::operators::Operator;
use crate::scalar_fn::fns::stat::StatFn;
use crate::scalar_fn::internal::row_count::RowCount;

/// A target that can bind abstract statistics to concrete expressions.
///
/// Implementations define how a pruning proof should read stats from a specific backing
/// representation. For example, a zone-map binder can translate a `max(col)` placeholder into a
/// field reference in the per-zone stats table, while a file-stats binder can translate the same
/// placeholder into a literal value from the file footer.
pub trait StatBinder {
    /// Bind `aggregate_fn(input)` to a concrete expression.
    ///
    /// Implementations should return `Ok(None)` when the requested aggregate
    /// statistic is unavailable in their backing representation.
    fn bind_aggregate(
        &self,
        input: &BoundExpression,
        aggregate_fn: &AggregateFnRef,
        stat_dtype: &DType,
    ) -> VortexResult<Option<BoundExpression>>;

    /// Bind the number of rows covered by each row of the stats scope.
    ///
    /// This resolves the [`RowCount`] placeholders that rewrite rules emit. It is an expression
    /// rather than a scalar because a scope may cover a different number of rows per row of its
    /// stats table: a zone map's final zone is often shorter than the rest.
    ///
    /// Implementations return `Ok(None)` when the row count is unknown, and must not return an
    /// expression that itself contains a [`RowCount`].
    fn bind_row_count(&self) -> VortexResult<Option<BoundExpression>>;

    /// Expression to use when a stat is unavailable.
    ///
    /// The default is a nullable null literal, which preserves three-valued
    /// pruning semantics for stats-table execution.
    fn missing_stat(&self, dtype: DType) -> VortexResult<BoundExpression> {
        null_expr(dtype)
    }
}

/// Bind all `vortex.stat` expressions in `predicate`.
///
/// The predicate is usually the output of a stats rewrite rule. Rewrite rules
/// are responsible for expressing stat semantics; binding maps aggregate-backed
/// stat requests to the concrete stats representation supported by the binder.
pub fn bind_stats<B: StatBinder + ?Sized>(
    predicate: BoundExpression,
    binder: &B,
) -> VortexResult<BoundExpression> {
    Ok(predicate
        .transform(bind_placeholder(binder), collapse_duplicate_operand)?
        .into_inner())
}

/// Substitute a `vortex.stat` or `vortex.row_count` placeholder with the binder's representation.
fn bind_placeholder<B: StatBinder + ?Sized>(
    binder: &B,
) -> impl FnMut(BoundExpression) -> VortexResult<Transformed<BoundExpression>> + '_ {
    move |expr| {
        // The traversal recurses into whatever it substitutes, so a binder may answer with an
        // expression that itself contains placeholders and have them resolved in this same pass.
        // That is what lets a stats source express `all_null` as `null_count == row_count`
        // without a second traversal.
        let bound = if expr.is::<StatFn>() {
            bind_stat_fn(&expr, binder)?
        } else if expr.is::<RowCount>() {
            binder.bind_row_count()?
        } else {
            return Ok(Transformed::no(expr));
        };

        match bound {
            Some(bound) => Ok(Transformed::yes(bound)),
            None => Ok(Transformed::yes(binder.missing_stat(expr.dtype().clone())?)),
        }
    }
}

/// Collapse `a or a` and `a and a` to `a`.
///
/// Both are idempotent under the three-valued logic pruning uses — `null or null` is `null`, just
/// as `null` is — so this only removes work, never changes the proof.
fn collapse_duplicate_operand(expr: BoundExpression) -> VortexResult<Transformed<BoundExpression>> {
    let is_duplicate = expr
        .as_opt::<Binary>()
        .is_some_and(|operator| matches!(operator, Operator::Or | Operator::And))
        && expr.child(0) == expr.child(1);

    if is_duplicate {
        return Ok(Transformed::yes(expr.child(0).clone()));
    }
    Ok(Transformed::no(expr))
}

fn bind_stat_fn(
    expr: &BoundExpression,
    binder: &(impl StatBinder + ?Sized),
) -> VortexResult<Option<BoundExpression>> {
    let options = expr.as_::<StatFn>();
    let aggregate_fn = options.aggregate_fn();
    // `StatFn` has exactly one child: the expression the aggregate statistic is computed over.
    let input = expr.child(0);

    binder.bind_aggregate(input, aggregate_fn, expr.dtype())
}

fn null_expr(dtype: DType) -> VortexResult<BoundExpression> {
    Ok(lit(Scalar::null(dtype.as_nullable())))
}

#[cfg(test)]
mod tests {
    use vortex_error::VortexResult;

    use super::*;
    use crate::aggregate_fn::fns::all_nan::AllNan;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::dtype::StructFields;
    use crate::expr::and;
    use crate::expr::col;
    use crate::expr::eq;
    use crate::expr::get_item;
    use crate::expr::is_null;
    use crate::expr::lit;
    use crate::expr::or;
    use crate::expr::root;
    use crate::expr::stats::Stat;
    use crate::scalar_fn::EmptyOptions;
    use crate::scalar_fn::ScalarFnVTableExt;
    use crate::scalar_fn::internal::row_count::RowCount as RowCountFn;
    use crate::stats::all_nan;
    use crate::stats::all_non_nan;
    use crate::stats::nan_count;

    struct TestBinder {
        input_scope: DType,
        stats_scope: DType,
        bind_nan_count: bool,
    }

    impl TestBinder {
        fn new(bind_nan_count: bool) -> Self {
            Self {
                input_scope: DType::Struct(
                    StructFields::from_iter([(
                        "f",
                        DType::Primitive(PType::F32, Nullability::NonNullable),
                    )]),
                    Nullability::NonNullable,
                ),
                stats_scope: DType::Struct(
                    StructFields::from_iter([(
                        "f_nan_count",
                        DType::Primitive(PType::U64, Nullability::NonNullable),
                    )]),
                    Nullability::NonNullable,
                ),
                bind_nan_count,
            }
        }
    }

    impl StatBinder for TestBinder {
        fn bind_aggregate(
            &self,
            _input: &BoundExpression,
            aggregate_fn: &AggregateFnRef,
            _stat_dtype: &DType,
        ) -> VortexResult<Option<BoundExpression>> {
            // `all_nan` is not stored, but it is `nan_count == row_count`. Answering with an
            // expression that still contains a `RowCount` exercises the binding pass recursing
            // into what it substitutes.
            if aggregate_fn.is::<AllNan>() && self.bind_nan_count {
                return Ok(Some(
                    eq(
                        get_item("f_nan_count", root()),
                        RowCountFn.new_expr(EmptyOptions, []),
                    )
                    .bind(&self.stats_scope)?,
                ));
            }

            let Some(stat) = Stat::from_aggregate_fn(aggregate_fn) else {
                return Ok(None);
            };

            if stat == Stat::NaNCount && self.bind_nan_count {
                Ok(Some(
                    get_item("f_nan_count", root()).bind(&self.stats_scope)?,
                ))
            } else {
                Ok(None)
            }
        }

        fn bind_row_count(&self) -> VortexResult<Option<BoundExpression>> {
            lit(10u64).bind(&self.stats_scope).map(Some)
        }
    }

    #[test]
    fn nan_count_binds_to_direct_stat_slot() -> VortexResult<()> {
        let binder = TestBinder::new(true);

        let bound = bind_stats(nan_count(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(bound, col("f_nan_count").bind(&binder.stats_scope)?);
        Ok(())
    }

    #[test]
    fn all_non_nan_does_not_derive_from_nan_count() -> VortexResult<()> {
        let binder = TestBinder::new(true);

        let bound = bind_stats(all_non_nan(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            lit(Scalar::null(DType::Bool(Nullability::Nullable))).bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn binder_emitted_row_count_resolves_in_the_same_pass() -> VortexResult<()> {
        let binder = TestBinder::new(true);

        let bound = bind_stats(all_nan(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            eq(col("f_nan_count"), lit(10u64)).bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn duplicate_proofs_collapse() -> VortexResult<()> {
        let binder = TestBinder::new(true);

        // Two independent proofs of the same fact, reached through different placeholders: one
        // states `nan_count == row_count` directly, the other asks for `all_nan`, which this
        // binder answers the same way.
        let predicate = or(
            eq(nan_count(col("f")), RowCountFn.new_expr(EmptyOptions, [])),
            all_nan(col("f")),
        );
        let bound = bind_stats(predicate.bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            eq(col("f_nan_count"), lit(10u64)).bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn distinct_proofs_are_both_kept() -> VortexResult<()> {
        let binder = TestBinder::new(true);

        // Only collapse operands that are actually equal.
        let predicate = or(eq(nan_count(col("f")), lit(0u64)), all_nan(col("f")));
        let bound = bind_stats(predicate.bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            or(
                eq(col("f_nan_count"), lit(0u64)),
                eq(col("f_nan_count"), lit(10u64)),
            )
            .bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn missing_stats_bind_to_null_without_reducing() -> VortexResult<()> {
        let binder = TestBinder::new(false);
        let null_bool = lit(Scalar::null(DType::Bool(Nullability::Nullable)));

        let bound = bind_stats(
            and(lit(false), all_non_nan(col("f"))).bind(&binder.input_scope)?,
            &binder,
        )?;

        assert_eq!(
            bound,
            and(lit(false), null_bool.clone()).bind(&binder.stats_scope)?
        );

        let bound = bind_stats(
            or(lit(true), all_non_nan(col("f"))).bind(&binder.input_scope)?,
            &binder,
        )?;

        assert_eq!(bound, or(lit(true), null_bool).bind(&binder.stats_scope)?);
        Ok(())
    }

    #[test]
    fn unrelated_expressions_do_not_request_nan_count() -> VortexResult<()> {
        let binder = TestBinder::new(false);

        let bound = bind_stats(is_null(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(bound, is_null(col("f")).bind(&binder.input_scope)?);
        Ok(())
    }
}
