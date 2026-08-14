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
//! Binding is also where scope-level quantities enter the predicate. The boolean aggregates
//! `all_null`, `all_non_null`, `all_nan`, and `all_non_nan` are derived from the corresponding
//! count statistic when a binder does not store them directly, which for the "all" variants needs
//! the number of rows the scope covers — see [`StatBinder::bind_row_count`].

use vortex_error::VortexResult;

use crate::aggregate_fn::AggregateFnRef;
use crate::aggregate_fn::AggregateFnVTableExt;
use crate::aggregate_fn::EmptyOptions;
use crate::aggregate_fn::fns::all_nan::AllNan;
use crate::aggregate_fn::fns::all_non_nan::AllNonNan;
use crate::aggregate_fn::fns::all_non_null::AllNonNull;
use crate::aggregate_fn::fns::all_null::AllNull;
use crate::aggregate_fn::fns::nan_count::NanCount;
use crate::aggregate_fn::fns::null_count::NullCount;
use crate::dtype::DType;
use crate::expr::BoundExpression;
use crate::expr::bound::eq;
use crate::expr::bound::lit;
use crate::expr::traversal::NodeExt;
use crate::expr::traversal::Transformed;
use crate::scalar::Scalar;
use crate::scalar_fn::fns::cast::Cast;
use crate::scalar_fn::fns::stat::StatFn;

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
    /// This backs the derivation of `all_null` and `all_nan` from their count statistics. It is an
    /// expression rather than a scalar because a scope may cover a different number of rows per
    /// row of its stats table: a zone map's final zone is often shorter than the rest.
    ///
    /// Implementations return `Ok(None)` when the row count is unknown.
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
        .transform_down(|expr| {
            if !expr.is::<StatFn>() {
                return Ok(Transformed::no(expr));
            }

            match bind_stat_fn(&expr, binder)? {
                Some(bound) => Ok(Transformed::yes(bound)),
                None => Ok(Transformed::yes(binder.missing_stat(expr.dtype().clone())?)),
            }
        })?
        .into_inner())
}

fn bind_stat_fn(
    expr: &BoundExpression,
    binder: &(impl StatBinder + ?Sized),
) -> VortexResult<Option<BoundExpression>> {
    let options = expr.as_::<StatFn>();
    let aggregate_fn = options.aggregate_fn();
    // `StatFn` has exactly one child: the expression the aggregate statistic is computed over.
    let input = expr.child(0);

    if let Some(bound) = binder.bind_aggregate(input, aggregate_fn, expr.dtype())? {
        return Ok(Some(bound));
    }

    derive_from_count(input, aggregate_fn, binder)
}

/// The count statistic a boolean "all" aggregate is derived from, and whether the derivation
/// compares it against the row count (`true`) or against zero (`false`).
fn count_derivation(aggregate_fn: &AggregateFnRef) -> Option<(AggregateFnRef, bool)> {
    if aggregate_fn.is::<AllNull>() {
        Some((NullCount.bind(EmptyOptions), true))
    } else if aggregate_fn.is::<AllNonNull>() {
        Some((NullCount.bind(EmptyOptions), false))
    } else if aggregate_fn.is::<AllNan>() {
        Some((NanCount.bind(EmptyOptions), true))
    } else if aggregate_fn.is::<AllNonNan>() {
        Some((NanCount.bind(EmptyOptions), false))
    } else {
        None
    }
}

/// Derive a boolean "all" aggregate from the count statistic a binder does store.
///
/// `all_null` holds exactly when every row is null, so it is `null_count == row_count`, and
/// `all_non_null` is `null_count == 0`; the NaN variants are the same shape over `nan_count`.
/// Binders that store the boolean aggregate directly answer from [`StatBinder::bind_aggregate`]
/// and never reach here.
fn derive_from_count(
    input: &BoundExpression,
    aggregate_fn: &AggregateFnRef,
    binder: &(impl StatBinder + ?Sized),
) -> VortexResult<Option<BoundExpression>> {
    // A cast can change how many values are null or NaN, so a count over the cast input proves
    // nothing about the cast output. The rewrite rules refuse to push counts through a cast for
    // the same reason.
    if input.is::<Cast>() {
        return Ok(None);
    }

    let Some((count_fn, against_row_count)) = count_derivation(aggregate_fn) else {
        return Ok(None);
    };
    let Some(count_dtype) = count_fn.state_dtype(input.dtype()) else {
        return Ok(None);
    };
    let Some(count) = binder.bind_aggregate(input, &count_fn, &count_dtype.as_nullable())? else {
        return Ok(None);
    };

    if !against_row_count {
        return Ok(Some(eq(count, lit(0u64))));
    }

    Ok(binder
        .bind_row_count()?
        .map(|row_count| eq(count, row_count)))
}

fn null_expr(dtype: DType) -> VortexResult<BoundExpression> {
    Ok(lit(Scalar::null(dtype.as_nullable())))
}

#[cfg(test)]
mod tests {
    use vortex_error::VortexResult;

    use super::*;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::dtype::StructFields;
    use crate::expr::and;
    use crate::expr::cast;
    use crate::expr::col;
    use crate::expr::eq;
    use crate::expr::get_item;
    use crate::expr::is_null;
    use crate::expr::lit;
    use crate::expr::or;
    use crate::expr::root;
    use crate::expr::stats::Stat;
    use crate::stats::all_nan;
    use crate::stats::all_non_nan;
    use crate::stats::nan_count;

    struct TestBinder {
        input_scope: DType,
        stats_scope: DType,
        bind_nan_count: bool,
        row_count: Option<u64>,
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
                row_count: Some(10),
            }
        }

        fn without_row_count(mut self) -> Self {
            self.row_count = None;
            self
        }
    }

    impl StatBinder for TestBinder {
        fn bind_aggregate(
            &self,
            _input: &BoundExpression,
            aggregate_fn: &AggregateFnRef,
            _stat_dtype: &DType,
        ) -> VortexResult<Option<BoundExpression>> {
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
            self.row_count
                .map(|row_count| lit(row_count).bind(&self.stats_scope))
                .transpose()
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
    fn all_non_nan_derives_from_nan_count() -> VortexResult<()> {
        let binder = TestBinder::new(true);

        let bound = bind_stats(all_non_nan(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            eq(col("f_nan_count"), lit(0u64)).bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn all_nan_derives_from_nan_count_and_row_count() -> VortexResult<()> {
        let binder = TestBinder::new(true);

        let bound = bind_stats(all_nan(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            eq(col("f_nan_count"), lit(10u64)).bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn all_nan_is_missing_without_a_row_count() -> VortexResult<()> {
        let binder = TestBinder::new(true).without_row_count();

        let bound = bind_stats(all_nan(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            lit(Scalar::null(DType::Bool(Nullability::Nullable))).bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn all_non_nan_does_not_derive_through_a_cast() -> VortexResult<()> {
        let binder = TestBinder::new(true);
        let cast_dtype = DType::Primitive(PType::F64, Nullability::NonNullable);

        let bound = bind_stats(
            all_non_nan(cast(col("f"), cast_dtype)).bind(&binder.input_scope)?,
            &binder,
        )?;

        assert_eq!(
            bound,
            lit(Scalar::null(DType::Bool(Nullability::Nullable))).bind(&binder.stats_scope)?
        );
        Ok(())
    }

    #[test]
    fn all_non_nan_is_missing_without_a_nan_count() -> VortexResult<()> {
        let binder = TestBinder::new(false);

        let bound = bind_stats(all_non_nan(col("f")).bind(&binder.input_scope)?, &binder)?;

        assert_eq!(
            bound,
            lit(Scalar::null(DType::Bool(Nullability::Nullable))).bind(&binder.stats_scope)?
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
