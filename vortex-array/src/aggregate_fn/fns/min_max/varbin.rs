// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_error::vortex_panic;

use super::MinMaxPartial;
use super::MinMaxResult;
use crate::ExecutionCtx;
use crate::aggregate_fn::AggregateArgs;
use crate::aggregate_fn::NumericalAggregateOpts;
use crate::arrays::VarBinViewArray;
use crate::arrays::varbinview::BinaryView;
use crate::dtype::DType;
use crate::dtype::Nullability::NonNullable;
use crate::scalar::Scalar;

pub(super) fn accumulate_varbinview(
    args: AggregateArgs<'_, NumericalAggregateOpts>,
    partial: &mut MinMaxPartial,
    array: &VarBinViewArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    partial.merge(args, varbin_compute_min_max(array, array.dtype(), ctx)?);
    Ok(())
}

fn varbin_compute_min_max(
    array: &VarBinViewArray,
    dtype: &DType,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<MinMaxResult>> {
    let mask = array.validity()?.execute_mask(array.len(), ctx)?;
    let views = array.views();
    let buffers = array
        .data_buffers()
        .iter()
        .map(|b| b.as_host().as_slice())
        .collect::<Vec<_>>();
    let extrema = if mask.all_true() {
        view_extrema(views.iter(), &buffers)
    } else {
        let bits = mask.to_bit_buffer();
        view_extrema(
            views
                .iter()
                .zip(bits.iter())
                .filter_map(|(view, valid)| valid.then_some(view)),
            &buffers,
        )
    };
    Ok(extrema.map(|(min, max)| MinMaxResult {
        min: make_scalar(dtype, min.bytes(&buffers)),
        max: make_scalar(dtype, max.bytes(&buffers)),
    }))
}

/// First four value bytes, big-endian, zero-padded. Distinct keys order the same as the values
/// (a zero pad sorts a shorter value first), so full comparisons are only needed on key ties.
#[inline]
fn prefix_key(view: &BinaryView) -> u32 {
    // Bytes 4..8 hold the inlined data or the reference prefix. The shift leaves
    // bits 0..96 and the truncation keeps 0..32, so no data is discarded.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the `>> 32` provably clears the bits the `u32` cast would drop"
    )]
    let prefix = (view.as_u128() >> 32) as u32;
    prefix.swap_bytes()
}

fn view_extrema<'a>(
    mut views: impl Iterator<Item = &'a BinaryView>,
    buffers: &[&'a [u8]],
) -> Option<(&'a BinaryView, &'a BinaryView)> {
    let first = views.next()?;
    let (mut min, mut max) = (first, first);
    let (mut min_key, mut max_key) = (prefix_key(first), prefix_key(first));
    for view in views {
        let key = prefix_key(view);
        // Identical views hold identical values, which skips the byte compare for repeats.
        if key < min_key
            || (key == min_key
                && view.as_u128() != min.as_u128()
                && view.bytes(buffers) < min.bytes(buffers))
        {
            min = view;
            min_key = key;
        }
        if key > max_key
            || (key == max_key
                && view.as_u128() != max.as_u128()
                && view.bytes(buffers) > max.bytes(buffers))
        {
            max = view;
            max_key = key;
        }
    }
    Some((min, max))
}

fn make_scalar(dtype: &DType, value: &[u8]) -> Scalar {
    match dtype {
        DType::Binary(_) => Scalar::binary(value.to_vec(), NonNullable),
        DType::Utf8(_) => {
            // SAFETY: VarBin arrays always validate their data against their dtype.
            let value = unsafe { str::from_utf8_unchecked(value) };
            Scalar::utf8(value, NonNullable)
        }
        _ => vortex_panic!("cannot make Scalar from bytes with dtype {dtype}"),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::view_extrema;
    use crate::arrays::varbinview::BinaryView;

    fn extrema(values: &[&[u8]]) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut data = Vec::new();
        let views = values
            .iter()
            .map(|value| {
                let offset = u32::try_from(data.len()).unwrap();
                if value.len() > BinaryView::MAX_INLINED_SIZE {
                    data.extend_from_slice(value);
                }
                BinaryView::make_view(value, 0, offset)
            })
            .collect::<Vec<_>>();
        let buffers = [data.as_slice()];
        view_extrema(views.iter(), &buffers)
            .map(|(min, max)| (min.bytes(&buffers).to_vec(), max.bytes(&buffers).to_vec()))
    }

    #[rstest]
    #[case::empty(&[], None)]
    #[case::single(&["only"], Some(("only", "only")))]
    #[case::short_sorts_before_zero_extension(&["a\0", "a", "a\0\0"], Some(("a", "a\0\0")))]
    #[case::empty_string_is_min(&["b", "", "a"], Some(("", "b")))]
    #[case::high_bytes(&["\u{ff}\u{1}", "\u{7f}", "\u{80}"], Some(("\u{7f}", "\u{ff}\u{1}")))]
    #[case::shared_prefix_long(
        &["DELIVER IN PERSON", "DELIVER IN PERSOM", "DELIVER IN PERSONS", "DELI"],
        Some(("DELI", "DELIVER IN PERSONS"))
    )]
    #[case::repeats(&["MAIL", "AIR", "MAIL", "AIR", "TRUCK", "TRUCK"], Some(("AIR", "TRUCK")))]
    fn extrema_match_lexicographic_order(
        #[case] values: &[&str],
        #[case] expected: Option<(&str, &str)>,
    ) {
        let bytes = values.iter().map(|v| v.as_bytes()).collect::<Vec<_>>();
        let expected = expected.map(|(min, max)| (min.as_bytes().to_vec(), max.as_bytes().to_vec()));
        assert_eq!(extrema(&bytes), expected);
        let naive = bytes
            .iter()
            .min()
            .zip(bytes.iter().max())
            .map(|(min, max)| (min.to_vec(), max.to_vec()));
        assert_eq!(extrema(&bytes), naive);
    }
}
