// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Sparse integer encoding for single-value-dominated arrays.

use vortex_array::ArrayId;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VTable;
use vortex_array::arrays::Constant;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::primitive::PrimitiveArrayExt;
use vortex_array::match_each_integer_ptype;
use vortex_array::scalar::PValue;
use vortex_array::scalar::Scalar;
use vortex_compressor::builtins::IntDictScheme;
use vortex_compressor::scheme::ChildSelection;
use vortex_compressor::scheme::CompressionEstimate;
use vortex_compressor::scheme::DescendantExclusion;
use vortex_compressor::scheme::EstimateVerdict;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_sparse::Sparse;
use vortex_sparse::SparseExt as _;
use vortex_utils::aliases::hash_map::HashMap;

use super::IntRLEScheme;
use super::RunEndScheme;
use crate::ArrayAndStats;
use crate::CascadingCompressor;
use crate::CompressorContext;
use crate::GenerateStatsOptions;
use crate::Scheme;
use crate::SchemeExt;

/// Sparse encoding for single-value-dominated arrays.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SparseScheme;

impl Scheme for SparseScheme {
    fn scheme_name(&self) -> &'static str {
        "vortex.int.sparse"
    }

    fn matches(&self, canonical: &Canonical) -> bool {
        canonical.dtype().is_int()
    }

    fn produced_encodings(&self) -> Vec<ArrayId> {
        vec![Sparse.id(), Constant.id()]
    }

    /// No distinct-value hash map: the 90% dominance test only needs the majority value, which
    /// [`dominant_value`] finds in two linear passes.
    fn stats_options(&self) -> GenerateStatsOptions {
        GenerateStatsOptions::default()
    }

    /// Children: values=0, indices=1.
    fn num_children(&self) -> usize {
        2
    }

    /// Sparse indices (child 1) are monotonically increasing positions with all unique values.
    /// Dict, RunEnd, RLE, and Sparse are all pointless on such data.
    fn descendant_exclusions(&self) -> Vec<DescendantExclusion> {
        vec![
            DescendantExclusion {
                excluded: IntDictScheme.id(),
                children: ChildSelection::One(1),
            },
            DescendantExclusion {
                excluded: RunEndScheme.id(),
                children: ChildSelection::One(1),
            },
            DescendantExclusion {
                excluded: IntRLEScheme.id(),
                children: ChildSelection::One(1),
            },
            DescendantExclusion {
                excluded: SparseScheme.id(),
                children: ChildSelection::One(1),
            },
        ]
    }

    fn expected_compression_ratio(
        &self,
        data: &ArrayAndStats,
        _compress_ctx: CompressorContext,
        exec_ctx: &mut ExecutionCtx,
    ) -> CompressionEstimate {
        let len = data.array_len() as f64;
        let stats = data.integer_stats(exec_ctx);
        let value_count = stats.value_count();

        // All-null arrays should be compressed as constant instead anyways.
        if value_count == 0 {
            return CompressionEstimate::Verdict(EstimateVerdict::Skip);
        }

        // If the majority (90%) of values is null, this will compress well.
        if stats.null_count() as f64 / len > 0.9 {
            return CompressionEstimate::Verdict(EstimateVerdict::Ratio(len / value_count as f64));
        }

        // A value holding >= 90% of `n` valid values leaves at most `0.1n` others, so at most
        // `0.2n + 1` runs. Fewer than two values per run rules that out for `n >= 4` without
        // looking at the data again.
        if value_count >= 4 && stats.average_run_length() < 2 {
            return CompressionEstimate::Verdict(EstimateVerdict::Skip);
        }

        // Any value with >= 90% frequency is a strict majority.
        let Some((_, most_frequent_count)) = majority_value(data, exec_ctx) else {
            return CompressionEstimate::Verdict(EstimateVerdict::Skip);
        };

        // If the most frequent value is the only value, we should compress as constant instead.
        if most_frequent_count == value_count {
            return CompressionEstimate::Verdict(EstimateVerdict::Skip);
        }
        debug_assert!(value_count > most_frequent_count);

        // See if the most frequent value accounts for >= 90% of the set values.
        let freq = most_frequent_count as f64 / value_count as f64;
        if freq < 0.9 {
            return CompressionEstimate::Verdict(EstimateVerdict::Skip);
        }

        // We only store the positions of the non-top values.
        CompressionEstimate::Verdict(EstimateVerdict::Ratio(
            value_count as f64 / (value_count - most_frequent_count) as f64,
        ))
    }

    fn compress(
        &self,
        compressor: &CascadingCompressor,
        data: &ArrayAndStats,
        compress_ctx: CompressorContext,
        exec_ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let len = data.array_len();
        let array = data.array();

        let (most_frequent_value, most_frequent_count) = most_frequent_value(data, exec_ctx);

        if most_frequent_count as usize == len {
            // If the most frequent value is the only value, we should compress as constant instead.
            return Ok(ConstantArray::new(
                Scalar::primitive_value(
                    most_frequent_value,
                    most_frequent_value.ptype(),
                    array.dtype().nullability(),
                ),
                len,
            )
            .into_array());
        }

        let sparse_encoded = Sparse::encode(
            array,
            Some(Scalar::primitive_value(
                most_frequent_value,
                most_frequent_value.ptype(),
                array.dtype().nullability(),
            )),
            exec_ctx,
        )?;

        if let Some(sparse) = sparse_encoded.as_opt::<Sparse>() {
            let sparse_values_primitive = sparse
                .patches()
                .values()
                .clone()
                .execute::<PrimitiveArray>(exec_ctx)?;
            let compressed_values = compressor.compress_child(
                &sparse_values_primitive.into_array(),
                &compress_ctx,
                self.id(),
                0,
                exec_ctx,
            )?;

            let indices = sparse
                .patches()
                .indices()
                .clone()
                .execute::<PrimitiveArray>(exec_ctx)?
                .narrow(exec_ctx)?;

            let compressed_indices = compressor.compress_child(
                &indices.into_array(),
                &compress_ctx,
                self.id(),
                1,
                exec_ctx,
            )?;

            Sparse::try_new(
                compressed_indices,
                compressed_values,
                sparse.len(),
                sparse.fill_scalar().clone(),
            )
            .map(|a| a.into_array())
        } else {
            Ok(sparse_encoded)
        }
    }
}

/// Strict-majority valid value and its exact count, if any, cached on the [`ArrayAndStats`]
/// bundle so the estimate and compress passes share one computation.
#[derive(Clone, Copy)]
struct Majority(Option<(PValue, u32)>);

/// Returns the valid value held by more than half of the valid values, with its exact count.
fn majority_value(data: &ArrayAndStats, exec_ctx: &mut ExecutionCtx) -> Option<(PValue, u32)> {
    let primitive = data.array_as_primitive().into_owned();
    data.get_or_insert_with::<Majority>(|| {
        Majority(
            compute_mode(&primitive, false, exec_ctx)
                .vortex_expect("majority of a canonical primitive array"),
        )
    })
    .0
}

/// Returns the most frequent valid value and its exact count. Only the null-dominated path can
/// reach this without a strict majority, and then the exact count covers few values.
fn most_frequent_value(data: &ArrayAndStats, exec_ctx: &mut ExecutionCtx) -> (PValue, u32) {
    majority_value(data, exec_ctx)
        .or_else(|| {
            let primitive = data.array_as_primitive().into_owned();
            compute_mode(&primitive, true, exec_ctx).vortex_expect("mode of a canonical primitive array")
        })
        .unwrap_or((PValue::U8(0), 0))
}

fn compute_mode(
    array: &PrimitiveArray,
    exact: bool,
    exec_ctx: &mut ExecutionCtx,
) -> VortexResult<Option<(PValue, u32)>> {
    let validity = array
        .as_ref()
        .validity()?
        .execute_mask(array.as_ref().len(), exec_ctx)?;
    let bits = (!validity.all_true()).then(|| validity.to_bit_buffer());
    match_each_integer_ptype!(array.ptype(), |T| {
        let values = array.as_slice::<T>();
        let mode = match &bits {
            None => mode_in(values.iter().copied(), exact),
            Some(bits) => mode_in(
                values
                    .iter()
                    .zip(bits.iter())
                    .filter_map(|(value, valid)| valid.then_some(*value)),
                exact,
            ),
        };
        Ok(mode.map(|(value, count)| (PValue::from(value), count)))
    })
}

/// A Boyer–Moore majority vote yields the only possible strict-majority candidate and a second
/// pass counts it exactly. Without a strict majority this returns `None`, or with `exact` falls
/// back to a full frequency count.
fn mode_in<T, I>(values: I, exact: bool) -> Option<(T, u32)>
where
    T: Copy + Eq + std::hash::Hash,
    I: Iterator<Item = T> + Clone,
{
    let mut candidate = None;
    let mut votes = 0u32;
    let mut total = 0u64;
    for value in values.clone() {
        total += 1;
        if votes == 0 {
            candidate = Some(value);
            votes = 1;
        } else if candidate == Some(value) {
            votes += 1;
        } else {
            votes -= 1;
        }
    }
    let candidate = candidate?;
    let count = values.clone().filter(|&value| value == candidate).count() as u64;
    if count * 2 > total {
        return Some((candidate, u32::try_from(count).ok()?));
    }
    if !exact {
        return None;
    }
    let mut frequencies: HashMap<T, u32> = HashMap::default();
    for value in values {
        *frequencies.entry(value).or_insert(0) += 1;
    }
    frequencies.into_iter().max_by_key(|&(_, count)| count)
}

#[cfg(test)]
mod tests {
    use super::mode_in;

    #[test]
    fn strict_majority_is_found_exactly() {
        let values = [7u32, 1, 7, 2, 7, 7, 3, 7, 7, 7];
        assert_eq!(mode_in(values.iter().copied(), false), Some((7, 7)));
    }

    #[test]
    fn majority_at_the_end_survives_early_votes() {
        let values = [1i64, 2, 3, 9, 9, 9, 9];
        assert_eq!(mode_in(values.iter().copied(), false), Some((9, 4)));
    }

    #[test]
    fn no_majority_is_none_unless_exact() {
        let values = [4u8, 4, 4, 1, 2, 3, 5, 6];
        assert_eq!(mode_in(values.iter().copied(), false), None);
        assert_eq!(mode_in(values.iter().copied(), true), Some((4, 3)));
    }

    #[test]
    fn half_is_not_a_strict_majority() {
        let values = [5u16, 5, 1, 2];
        assert_eq!(mode_in(values.iter().copied(), false), None);
    }

    #[test]
    fn empty_input_has_no_mode() {
        assert_eq!(mode_in(std::iter::empty::<u16>(), true), None);
    }
}
