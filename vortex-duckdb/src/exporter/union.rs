// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex::array::ExecutionCtx;
use vortex::array::IntoArray;
use vortex::array::arrays::BoolArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::UnionArray;
use vortex::array::builtins::ArrayBuiltins;
use vortex::array::validity::Validity;
use vortex::buffer::BitBuffer;
use vortex::buffer::Buffer;
use vortex::error::VortexResult;
use vortex::error::vortex_err;

use super::ConversionCache;
use super::new_array_exporter;
use super::validity;
use crate::duckdb::VectorRef;
use crate::exporter::ColumnExporter;

struct UnionExporter {
    type_ids: Box<dyn ColumnExporter>,
    children: Vec<Box<dyn ColumnExporter>>,
}

pub(crate) fn new_exporter(
    array: UnionArray,
    cache: &ConversionCache,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Box<dyn ColumnExporter>> {
    let parts = array.into_data_parts();
    let type_ids = parts.type_ids.execute::<PrimitiveArray>(ctx)?;
    let outer_validity = type_ids.validity()?.execute_mask(type_ids.len(), ctx)?;
    let raw_type_ids = type_ids.as_slice::<u8>().to_vec();
    let type_ids_nullability = type_ids.dtype().nullability();
    let remapped = type_ids
        .as_slice::<u8>()
        .iter()
        .enumerate()
        .map(|(idx, tag)| {
            if !outer_validity.value(idx) {
                return Ok(0);
            }
            parts
                .variants
                .tag_to_child_index(*tag)
                .and_then(|idx| u8::try_from(idx).ok())
                .ok_or_else(|| vortex_err!("Union tag {tag} has no DuckDB member"))
        })
        .collect::<VortexResult<Buffer<u8>>>()?;
    let duckdb_type_ids = PrimitiveArray::new(
        remapped,
        Validity::from_mask(outer_validity.clone(), type_ids_nullability),
    )
    .into_array();
    let type_ids = new_array_exporter(duckdb_type_ids, cache, ctx)?;

    let children = parts
        .children
        .into_iter()
        .zip(parts.variants.type_ids())
        .map(|(child, tag)| {
            let selected = BoolArray::new(
                BitBuffer::from_iter(
                    raw_type_ids
                        .iter()
                        .enumerate()
                        .map(|(idx, value)| outer_validity.value(idx) && value == tag),
                ),
                Validity::NonNullable,
            );
            new_array_exporter(child.mask(selected.into_array())?, cache, ctx)
        })
        .collect::<VortexResult<Vec<_>>>()?;

    Ok(validity::new_exporter(
        outer_validity,
        Box::new(UnionExporter { type_ids, children }),
    ))
}

impl ColumnExporter for UnionExporter {
    fn preferred_batch_len(&self, offset: usize, max_len: usize) -> usize {
        self.children.iter().fold(
            self.type_ids.preferred_batch_len(offset, max_len),
            |len, child| child.preferred_batch_len(offset, len),
        )
    }

    fn export(
        &self,
        offset: usize,
        len: usize,
        vector: &mut VectorRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        self.type_ids
            .export(offset, len, vector.struct_vector_get_child_mut(0), ctx)?;
        for (idx, child) in self.children.iter().enumerate() {
            child.export(
                offset,
                len,
                vector.struct_vector_get_child_mut(idx + 1),
                ctx,
            )?;
        }
        Ok(())
    }
}
