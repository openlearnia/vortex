// SPDX-License-Identifier: Apache-2.0
//! DuckDB-specific Vortex extension dtypes for INTERVAL / ENUM / BIT / BIGNUM.
//!
//! These live in the adapter (not vortex-array core) and round-trip via ExtId strings.
//! On read without a registered plugin they deserialize as [`ForeignExtDType`]; we match by id.

use std::ffi::CStr;
use std::ffi::CString;
use std::fmt;
use std::os::raw::c_char;

use vortex::array::dtype::extension::ExtDType;
use vortex::array::dtype::extension::ExtVTable;
use vortex::dtype::DType;
use vortex::dtype::FieldNames;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::dtype::StructFields;
use vortex::dtype::extension::ExtId;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::scalar::ScalarValue;
use vortex::session::registry::CachedId;

use crate::cpp::DUCKDB_TYPE;
use crate::duckdb::LogicalType;
use crate::duckdb::LogicalTypeRef;

pub const INTERVAL_EXT_ID: &str = "vortex.interval";
pub const ENUM_EXT_ID: &str = "vortex.enum";
pub const BIT_EXT_ID: &str = "vortex.bit";
pub const BIGNUM_EXT_ID: &str = "vortex.bignum";
pub const HUGEINT_EXT_ID: &str = "vortex.hugeint";
pub const UHUGEINT_EXT_ID: &str = "vortex.uhugeint";
pub const VARIANT_EXT_ID: &str = "vortex.duckdb.variant";
pub const TIME_TZ_EXT_ID: &str = "vortex.duckdb.time_tz";

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct EmptyExtMetadata;

impl fmt::Display for EmptyExtMetadata {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckInterval;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckBit;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckBignum;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckEnum;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckHugeInt;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckUHugeInt;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckVariant;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DuckTimeTz;

/// ENUM dictionary names stored as length-prefixed UTF-8 blobs: `[u32 LE len][bytes]...`
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct EnumMetadata {
    pub names: Vec<String>,
}

impl fmt::Display for EnumMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} members", self.names.len())
    }
}

impl EnumMetadata {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for name in &self.names {
            let bytes = name.as_bytes();
            let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        out
    }

    pub fn decode(data: &[u8]) -> VortexResult<Self> {
        let mut names = Vec::new();
        let mut i = 0;
        while i < data.len() {
            if i + 4 > data.len() {
                vortex_bail!("truncated enum metadata");
            }
            let len = u32::from_le_bytes(data[i..i + 4].try_into().unwrap()) as usize;
            i += 4;
            if i + len > data.len() {
                vortex_bail!("truncated enum metadata name");
            }
            let name = std::str::from_utf8(&data[i..i + len])
                .map_err(|_| vortex_err!("enum member name is not utf8"))?
                .to_string();
            i += len;
            names.push(name);
        }
        Ok(Self { names })
    }
}

fn interval_storage(nullability: Nullability) -> DType {
    let fields = StructFields::new(
        FieldNames::from(["months", "days", "micros"]),
        vec![
            DType::Primitive(PType::I32, Nullability::NonNullable),
            DType::Primitive(PType::I32, Nullability::NonNullable),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ],
    );
    DType::Struct(fields, nullability)
}

pub fn interval_dtype(nullability: Nullability) -> VortexResult<DType> {
    Ok(DType::Extension(
        ExtDType::<DuckInterval>::try_new(EmptyExtMetadata, interval_storage(nullability))?
            .erased(),
    ))
}

pub fn bit_dtype(nullability: Nullability) -> VortexResult<DType> {
    Ok(DType::Extension(
        ExtDType::<DuckBit>::try_new(EmptyExtMetadata, DType::Binary(nullability))?.erased(),
    ))
}

pub fn bignum_dtype(nullability: Nullability) -> VortexResult<DType> {
    Ok(DType::Extension(
        ExtDType::<DuckBignum>::try_new(EmptyExtMetadata, DType::Binary(nullability))?.erased(),
    ))
}

fn fixed16_storage(nullability: Nullability) -> DType {
    DType::FixedSizeList(
        std::sync::Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        16,
        nullability,
    )
}

pub fn hugeint_dtype(nullability: Nullability) -> VortexResult<DType> {
    Ok(DType::Extension(
        ExtDType::<DuckHugeInt>::try_new(EmptyExtMetadata, fixed16_storage(nullability))?.erased(),
    ))
}

pub fn uhugeint_dtype(nullability: Nullability) -> VortexResult<DType> {
    Ok(DType::Extension(
        ExtDType::<DuckUHugeInt>::try_new(EmptyExtMetadata, fixed16_storage(nullability))?.erased(),
    ))
}

pub fn time_tz_dtype(nullability: Nullability) -> VortexResult<DType> {
    Ok(DType::Extension(
        ExtDType::<DuckTimeTz>::try_new(
            EmptyExtMetadata,
            DType::Primitive(PType::U64, nullability),
        )?
        .erased(),
    ))
}

impl ExtVTable for DuckTimeTz {
    type Metadata = EmptyExtMetadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new(TIME_TZ_EXT_ID);
        *ID
    }

    fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_metadata(&self, _data: &[u8]) -> VortexResult<Self::Metadata> {
        Ok(EmptyExtMetadata)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        match ext_dtype.storage_dtype() {
            DType::Primitive(PType::U64, _) => Ok(()),
            other => vortex_bail!("DuckDB TIME_TZ storage must be U64, got {other}"),
        }
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage_value)
    }
}

impl ExtVTable for DuckVariant {
    type Metadata = EmptyExtMetadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new(VARIANT_EXT_ID);
        *ID
    }

    fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_metadata(&self, _data: &[u8]) -> VortexResult<Self::Metadata> {
        Ok(EmptyExtMetadata)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        match ext_dtype.storage_dtype() {
            DType::Struct(_, _) => Ok(()),
            other => vortex_bail!("DuckDB VARIANT storage must be a struct, got {other}"),
        }
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage_value)
    }
}

pub fn enum_dtype(logical_type: &LogicalTypeRef, nullability: Nullability) -> VortexResult<DType> {
    let names = enum_member_names(logical_type)?;
    let internal = enum_internal_ptype(logical_type)?;
    let storage = DType::Primitive(internal, nullability);
    Ok(DType::Extension(
        ExtDType::<DuckEnum>::try_new(EnumMetadata { names }, storage)?.erased(),
    ))
}

impl ExtVTable for DuckInterval {
    type Metadata = EmptyExtMetadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new(INTERVAL_EXT_ID);
        *ID
    }

    fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_metadata(&self, _data: &[u8]) -> VortexResult<Self::Metadata> {
        Ok(EmptyExtMetadata)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        match ext_dtype.storage_dtype() {
            DType::Struct(fields, _) if fields.names().len() == 3 => Ok(()),
            other => {
                vortex_bail!("INTERVAL storage must be struct{{months,days,micros}}, got {other}")
            }
        }
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage_value)
    }
}

macro_rules! fixed16_vtable {
    ($type:ty, $id:ident, $name:literal) => {
        impl ExtVTable for $type {
            type Metadata = EmptyExtMetadata;
            type NativeValue<'a> = &'a ScalarValue;

            fn id(&self) -> ExtId {
                static ID: CachedId = CachedId::new($id);
                *ID
            }

            fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
                Ok(Vec::new())
            }

            fn deserialize_metadata(&self, _data: &[u8]) -> VortexResult<Self::Metadata> {
                Ok(EmptyExtMetadata)
            }

            fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
                match ext_dtype.storage_dtype() {
                    DType::FixedSizeList(element, 16, _)
                        if matches!(element.as_ref(), DType::Primitive(PType::U8, _)) =>
                    {
                        Ok(())
                    }
                    other => vortex_bail!(
                        "{} storage must be FixedSizeList(U8,16), got {}",
                        $name,
                        other
                    ),
                }
            }

            fn unpack_native<'a>(
                _ext_dtype: &'a ExtDType<Self>,
                storage_value: &'a ScalarValue,
            ) -> VortexResult<Self::NativeValue<'a>> {
                Ok(storage_value)
            }
        }
    };
}

fixed16_vtable!(DuckHugeInt, HUGEINT_EXT_ID, "HUGEINT");
fixed16_vtable!(DuckUHugeInt, UHUGEINT_EXT_ID, "UHUGEINT");

impl ExtVTable for DuckBit {
    type Metadata = EmptyExtMetadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new(BIT_EXT_ID);
        *ID
    }

    fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_metadata(&self, _data: &[u8]) -> VortexResult<Self::Metadata> {
        Ok(EmptyExtMetadata)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        match ext_dtype.storage_dtype() {
            DType::Binary(_) => Ok(()),
            other => vortex_bail!("BIT storage must be Binary, got {other}"),
        }
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage_value)
    }
}

impl ExtVTable for DuckBignum {
    type Metadata = EmptyExtMetadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new(BIGNUM_EXT_ID);
        *ID
    }

    fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_metadata(&self, _data: &[u8]) -> VortexResult<Self::Metadata> {
        Ok(EmptyExtMetadata)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        match ext_dtype.storage_dtype() {
            DType::Binary(_) => Ok(()),
            other => vortex_bail!("BIGNUM storage must be Binary, got {other}"),
        }
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage_value)
    }
}

impl ExtVTable for DuckEnum {
    type Metadata = EnumMetadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new(ENUM_EXT_ID);
        *ID
    }

    fn serialize_metadata(&self, metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(metadata.encode())
    }

    fn deserialize_metadata(&self, data: &[u8]) -> VortexResult<Self::Metadata> {
        EnumMetadata::decode(data)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        match ext_dtype.storage_dtype() {
            DType::Primitive(PType::U8 | PType::U16 | PType::U32, _) => Ok(()),
            other => vortex_bail!("ENUM storage must be U8/U16/U32, got {other}"),
        }
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage_value)
    }
}

pub fn logical_type_from_duckdb_ext(
    ext_dtype: &vortex::dtype::extension::ExtDTypeRef,
) -> VortexResult<Option<LogicalType>> {
    let id = ext_dtype.id();
    let id_str = id.as_ref();
    if id_str == INTERVAL_EXT_ID || ext_dtype.is::<DuckInterval>() {
        return Ok(Some(LogicalType::new(DUCKDB_TYPE::DUCKDB_TYPE_INTERVAL)));
    }
    if id_str == BIT_EXT_ID || ext_dtype.is::<DuckBit>() {
        return Ok(Some(LogicalType::new(DUCKDB_TYPE::DUCKDB_TYPE_BIT)));
    }
    if id_str == BIGNUM_EXT_ID || ext_dtype.is::<DuckBignum>() {
        return Ok(Some(LogicalType::new(DUCKDB_TYPE::DUCKDB_TYPE_BIGNUM)));
    }
    if id_str == HUGEINT_EXT_ID || ext_dtype.is::<DuckHugeInt>() {
        return Ok(Some(LogicalType::new(DUCKDB_TYPE::DUCKDB_TYPE_HUGEINT)));
    }
    if id_str == UHUGEINT_EXT_ID || ext_dtype.is::<DuckUHugeInt>() {
        return Ok(Some(LogicalType::new(DUCKDB_TYPE::DUCKDB_TYPE_UHUGEINT)));
    }
    if id_str == VARIANT_EXT_ID || ext_dtype.is::<DuckVariant>() {
        let ty = unsafe { crate::cpp::duckdb_vx_create_variant() };
        if ty.is_null() {
            vortex_bail!("duckdb_vx_create_variant failed");
        }
        return Ok(Some(unsafe { LogicalType::own(ty) }));
    }
    if id_str == TIME_TZ_EXT_ID || ext_dtype.is::<DuckTimeTz>() {
        return Ok(Some(LogicalType::new(DUCKDB_TYPE::DUCKDB_TYPE_TIME_TZ)));
    }
    if id_str == ENUM_EXT_ID || ext_dtype.is::<DuckEnum>() {
        let meta = if let Some(m) = ext_dtype.metadata_opt::<DuckEnum>() {
            m.clone()
        } else {
            EnumMetadata::decode(&ext_dtype.serialize_metadata()?)?
        };
        return Ok(Some(create_enum_logical_type(&meta.names)?));
    }
    Ok(None)
}

fn enum_internal_ptype(logical_type: &LogicalTypeRef) -> VortexResult<PType> {
    let ty = unsafe { crate::duckdb_c_api_extra::duckdb_enum_internal_type(logical_type.as_ptr()) };
    Ok(match ty {
        DUCKDB_TYPE::DUCKDB_TYPE_UTINYINT => PType::U8,
        DUCKDB_TYPE::DUCKDB_TYPE_USMALLINT => PType::U16,
        DUCKDB_TYPE::DUCKDB_TYPE_UINTEGER => PType::U32,
        other => vortex_bail!("unexpected ENUM internal type {other:?}"),
    })
}

fn enum_member_names(logical_type: &LogicalTypeRef) -> VortexResult<Vec<String>> {
    let size =
        unsafe { crate::duckdb_c_api_extra::duckdb_enum_dictionary_size(logical_type.as_ptr()) }
            as usize;
    let mut names = Vec::with_capacity(size);
    for i in 0..size {
        let ptr = unsafe {
            crate::duckdb_c_api_extra::duckdb_enum_dictionary_value(logical_type.as_ptr(), i as _)
        };
        if ptr.is_null() {
            vortex_bail!("null enum dictionary value at {i}");
        }
        let name = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { crate::cpp::duckdb_free(ptr.cast()) };
        names.push(name);
    }
    Ok(names)
}

fn create_enum_logical_type(names: &[String]) -> VortexResult<LogicalType> {
    let c_names: Vec<CString> = names
        .iter()
        .map(|n| CString::new(n.as_str()).map_err(|_| vortex_err!("enum name contains NUL")))
        .collect::<Result<_, _>>()?;
    let ptrs: Vec<*const c_char> = c_names.iter().map(|c| c.as_ptr()).collect();
    let ty = unsafe {
        crate::duckdb_c_api_extra::duckdb_create_enum_type(ptrs.as_ptr(), ptrs.len() as _)
    };
    if ty.is_null() {
        vortex_bail!("duckdb_create_enum_type failed");
    }
    Ok(unsafe { LogicalType::own(ty) })
}

/// DuckDB physical INTERVAL layout (matches `duckdb_interval`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DuckdbIntervalPhysical {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}
