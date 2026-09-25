// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex::array::ExecutionCtx;
use vortex::array::arrays::ExtensionArray;
use vortex::array::arrays::FixedSizeListArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::TemporalArray;
use vortex::array::arrays::VarBinViewArray;
use vortex::array::arrays::extension::ExtensionArrayExt;
use vortex::array::extension::datetime::AnyTemporal;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::extension::uuid::Uuid;
use vortex_spatial::extension::LineString;
use vortex_spatial::extension::LineStringData;
use vortex_spatial::extension::MultiLineString;
use vortex_spatial::extension::MultiLineStringData;
use vortex_spatial::extension::MultiPoint;
use vortex_spatial::extension::MultiPointData;
use vortex_spatial::extension::MultiPolygon;
use vortex_spatial::extension::MultiPolygonData;
use vortex_spatial::extension::Point;
use vortex_spatial::extension::PointData;
use vortex_spatial::extension::Polygon;
use vortex_spatial::extension::PolygonData;
use vortex_spatial::extension::WellKnownBinary;
use vortex_spatial::extension::WellKnownBinaryData;

use crate::convert::ext_types::BIGNUM_EXT_ID;
use crate::convert::ext_types::BIT_EXT_ID;
use crate::convert::ext_types::DuckBignum;
use crate::convert::ext_types::DuckBit;
use crate::convert::ext_types::DuckEnum;
use crate::convert::ext_types::DuckHugeInt;
use crate::convert::ext_types::DuckInterval;
use crate::convert::ext_types::DuckTimeTz;
use crate::convert::ext_types::DuckUHugeInt;
use crate::convert::ext_types::DuckVariant;
use crate::convert::ext_types::DuckdbIntervalPhysical;
use crate::convert::ext_types::ENUM_EXT_ID;
use crate::convert::ext_types::HUGEINT_EXT_ID;
use crate::convert::ext_types::INTERVAL_EXT_ID;
use crate::convert::ext_types::UHUGEINT_EXT_ID;
use crate::exporter::ColumnExporter;
use crate::exporter::ConversionCache;
use crate::exporter::all_invalid;
use crate::exporter::primitive;
use crate::exporter::spatial;
use crate::exporter::struct_;
use crate::exporter::temporal;
// Upstream UUID exporter; DuckLake extension types (hugeint, interval, enum,
// bit/bignum, variant, timetz) export below.
use crate::exporter::uuid;
use crate::exporter::validity;
use crate::exporter::varbinview;
use crate::{cpp, duckdb::VectorRef};

fn new_hugeint_exporter(
    ext: ExtensionArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Box<dyn ColumnExporter>> {
    // Storage bytes are sign-flipped big-endian for signed HUGEINT and plain
    // big-endian for UHUGEINT (see `hugeint_vector_to_vortex`).
    let signed =
        ext.ext_dtype().is::<DuckHugeInt>() || ext.ext_dtype().id().as_ref() == HUGEINT_EXT_ID;
    let storage = ext
        .storage_array()
        .clone()
        .execute::<FixedSizeListArray>(ctx)?;
    let len = storage.len();
    let parts = storage.into_data_parts();
    if parts.validity.definitely_all_null() {
        return Ok(all_invalid::new_exporter());
    }
    let mask = parts.validity.to_array(len).execute(ctx)?;
    let bytes = parts.elements.execute::<PrimitiveArray>(ctx)?;
    let values = bytes
        .as_slice::<u8>()
        .chunks_exact(16)
        .map(|bytes| {
            let mut upper_bytes = [0u8; 8];
            upper_bytes.copy_from_slice(&bytes[..8]);
            let upper = u64::from_be_bytes(upper_bytes);
            let mut lower_bytes = [0u8; 8];
            lower_bytes.copy_from_slice(&bytes[8..]);
            cpp::duckdb_hugeint {
                lower: u64::from_be_bytes(lower_bytes),
                upper: if signed {
                    (upper ^ (1_u64 << 63)) as i64
                } else {
                    upper as i64
                },
            }
        })
        .collect();
    Ok(validity::new_exporter(
        mask,
        Box::new(HugeIntExporter { values }),
    ))
}

fn new_interval_exporter(
    ext: ExtensionArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Box<dyn ColumnExporter>> {
    let storage = ext.storage_array().clone().execute::<StructArray>(ctx)?;
    let len = storage.len();
    let parts = storage.into_data_parts();
    if parts.validity.definitely_all_null() {
        return Ok(all_invalid::new_exporter());
    }
    let mask = parts.validity.to_array(len).execute(ctx)?;
    let months = parts.fields[0]
        .clone()
        .execute::<PrimitiveArray>(ctx)?
        .as_slice::<i32>()
        .to_vec();
    let days = parts.fields[1]
        .clone()
        .execute::<PrimitiveArray>(ctx)?
        .as_slice::<i32>()
        .to_vec();
    let micros = parts.fields[2]
        .clone()
        .execute::<PrimitiveArray>(ctx)?
        .as_slice::<i64>()
        .to_vec();
    let values: Vec<DuckdbIntervalPhysical> = months
        .into_iter()
        .zip(days)
        .zip(micros)
        .map(|((months, days), micros)| DuckdbIntervalPhysical {
            months,
            days,
            micros,
        })
        .collect();
    Ok(validity::new_exporter(
        mask,
        Box::new(IntervalExporter { values }),
    ))
}

struct HugeIntExporter {
    values: Vec<cpp::duckdb_hugeint>,
}

impl ColumnExporter for HugeIntExporter {
    fn export(
        &self,
        offset: usize,
        len: usize,
        vector: &mut VectorRef,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        unsafe {
            vector
                .as_slice_mut::<cpp::duckdb_hugeint>(len)
                .copy_from_slice(&self.values[offset..offset + len]);
        }
        Ok(())
    }
}

struct IntervalExporter {
    values: Vec<DuckdbIntervalPhysical>,
}

impl ColumnExporter for IntervalExporter {
    fn export(
        &self,
        offset: usize,
        len: usize,
        vector: &mut VectorRef,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        unsafe {
            vector
                .as_slice_mut::<DuckdbIntervalPhysical>(len)
                .copy_from_slice(&self.values[offset..offset + len]);
        }
        Ok(())
    }
}

pub(crate) fn new_exporter(
    ext: ExtensionArray,
    cache: &ConversionCache,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Box<dyn ColumnExporter>> {
    if ext.ext_dtype().is::<AnyTemporal>() {
        return temporal::new_exporter(TemporalArray::try_from(ext)?, ctx);
    }

    if ext.ext_dtype().is::<Uuid>() {
        return uuid::new_exporter(ext, ctx);
    }

    if ext.ext_dtype().is::<DuckHugeInt>()
        || ext.ext_dtype().is::<DuckUHugeInt>()
        || ext.ext_dtype().id().as_ref() == HUGEINT_EXT_ID
        || ext.ext_dtype().id().as_ref() == UHUGEINT_EXT_ID
    {
        return new_hugeint_exporter(ext, ctx);
    }

    if ext.ext_dtype().is::<DuckInterval>() || ext.ext_dtype().id().as_ref() == INTERVAL_EXT_ID {
        return new_interval_exporter(ext, ctx);
    }

    if ext.ext_dtype().is::<DuckEnum>() || ext.ext_dtype().id().as_ref() == ENUM_EXT_ID {
        let storage = ext.storage_array().clone().execute::<PrimitiveArray>(ctx)?;
        return primitive::new_exporter(storage, ctx);
    }

    if ext.ext_dtype().is::<DuckTimeTz>()
        || ext.ext_dtype().id().as_ref() == crate::convert::ext_types::TIME_TZ_EXT_ID
    {
        let storage = ext.storage_array().clone().execute::<PrimitiveArray>(ctx)?;
        return primitive::new_exporter(storage, ctx);
    }

    if ext.ext_dtype().is::<DuckBit>()
        || ext.ext_dtype().is::<DuckBignum>()
        || ext.ext_dtype().id().as_ref() == BIT_EXT_ID
        || ext.ext_dtype().id().as_ref() == BIGNUM_EXT_ID
    {
        let storage = ext
            .storage_array()
            .clone()
            .execute::<VarBinViewArray>(ctx)?;
        return varbinview::new_exporter(storage, ctx);
    }

    if ext.ext_dtype().is::<DuckVariant>() {
        let storage = ext.storage_array().clone().execute::<StructArray>(ctx)?;
        return struct_::new_exporter(storage, cache, ctx);
    }

    if ext.ext_dtype().is::<WellKnownBinary>() {
        return spatial::new_wkb_exporter(WellKnownBinaryData::try_from(ext)?, ctx);
    }

    if ext.ext_dtype().is::<Point>() {
        return spatial::new_point_exporter(PointData::try_from(ext)?, ctx);
    }

    if ext.ext_dtype().is::<LineString>() {
        return spatial::new_linestring_exporter(LineStringData::try_from(ext)?, ctx);
    }

    if ext.ext_dtype().is::<MultiPoint>() {
        return spatial::new_multipoint_exporter(MultiPointData::try_from(ext)?, ctx);
    }

    if ext.ext_dtype().is::<Polygon>() {
        return spatial::new_polygon_exporter(PolygonData::try_from(ext)?, ctx);
    }

    if ext.ext_dtype().is::<MultiLineString>() {
        return spatial::new_multilinestring_exporter(MultiLineStringData::try_from(ext)?, ctx);
    }

    if ext.ext_dtype().is::<MultiPolygon>() {
        return spatial::new_multipolygon_exporter(MultiPolygonData::try_from(ext)?, ctx);
    }

    vortex_bail!(
        "no non-temporal extension exporter for \"{}\"",
        ext.ext_dtype().id()
    )
}
