// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Temporal extension data types.

use std::fmt;
use std::sync::Arc;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_session::registry::CachedId;

use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::PType;
use crate::dtype::extension::ExtDType;
use crate::dtype::extension::ExtId;
use crate::dtype::extension::ExtVTable;
use crate::extension::datetime::TimeUnit;
use crate::scalar::ScalarValue;

/// Timestamp DType.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Timestamp;

impl Timestamp {
    /// Creates a new Timestamp extension =dtype with the given time unit and nullability.
    pub fn new(time_unit: TimeUnit, nullability: Nullability) -> ExtDType<Self> {
        Self::new_with_tz(time_unit, None, nullability)
    }

    /// Creates a new Timestamp extension dtype with the given time unit, timezone, and nullability.
    pub fn new_with_tz(
        time_unit: TimeUnit,
        timezone: Option<Arc<str>>,
        nullability: Nullability,
    ) -> ExtDType<Self> {
        ExtDType::try_new(
            TimestampOptions {
                unit: time_unit,
                tz: timezone,
            },
            DType::Primitive(PType::I64, nullability),
        )
        .vortex_expect("failed to create timestamp dtype")
    }

    /// Creates a new `Timestamp` extension dtype with the given options and nullability.
    pub fn new_with_options(options: TimestampOptions, nullability: Nullability) -> ExtDType<Self> {
        ExtDType::try_new(options, DType::Primitive(PType::I64, nullability))
            .vortex_expect("failed to create timestamp dtype")
    }
}

/// Options for the Timestamp DType.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimestampOptions {
    /// The time unit of the timestamp.
    pub unit: TimeUnit,
    /// The timezone of the timestamp, if any.
    pub tz: Option<Arc<str>>,
}

impl fmt::Display for TimestampOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.tz {
            Some(tz) => write!(f, "{}, tz={}", self.unit, tz),
            None => write!(f, "{}", self.unit),
        }
    }
}

/// Unpacked value of a [`Timestamp`] extension scalar.
///
/// Each variant carries the raw storage value and an optional timezone.
pub enum TimestampValue<'a> {
    /// Seconds since the Unix epoch.
    Seconds(i64, Option<&'a Arc<str>>),
    /// Milliseconds since the Unix epoch.
    Milliseconds(i64, Option<&'a Arc<str>>),
    /// Microseconds since the Unix epoch.
    Microseconds(i64, Option<&'a Arc<str>>),
    /// Nanoseconds since the Unix epoch.
    Nanoseconds(i64, Option<&'a Arc<str>>),
}

impl fmt::Display for TimestampValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (raw, unit, tz) = match self {
            TimestampValue::Seconds(v, tz) => (*v, TimeUnit::Seconds, *tz),
            TimestampValue::Milliseconds(v, tz) => (*v, TimeUnit::Milliseconds, *tz),
            TimestampValue::Microseconds(v, tz) => (*v, TimeUnit::Microseconds, *tz),
            TimestampValue::Nanoseconds(v, tz) => (*v, TimeUnit::Nanoseconds, *tz),
        };
        if raw == i64::MAX {
            return f.write_str("infinity");
        }
        if raw == i64::MIN {
            return f.write_str("-infinity");
        }
        let Ok(span) = unit.to_jiff_span(raw) else {
            return write!(f, "{raw}{unit}");
        };
        let Ok(ts) = jiff::Timestamp::UNIX_EPOCH.checked_add(span) else {
            return write!(f, "{raw}{unit}");
        };

        match tz {
            None => write!(f, "{ts}"),
            Some(tz) => {
                let adjusted_ts = ts.in_tz(tz.as_ref()).vortex_expect("unknown timezone");
                write!(f, "{adjusted_ts}",)
            }
        }
    }
}

impl ExtVTable for Timestamp {
    type Metadata = TimestampOptions;

    type NativeValue<'a> = TimestampValue<'a>;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("vortex.timestamp");
        *ID
    }

    // NOTE(ngates): unfortunately we're stuck with this hand-rolled serialization format for
    //  backwards compatibility.
    fn serialize_metadata(&self, metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        let mut bytes = Vec::with_capacity(4);
        let unit_tag: u8 = metadata.unit.into();

        bytes.push(unit_tag);

        // Encode time_zone as u16 length followed by utf8 bytes.
        match &metadata.tz {
            None => bytes.extend_from_slice(0u16.to_le_bytes().as_slice()),
            Some(tz) => {
                let tz_bytes = tz.as_bytes();
                let tz_len = u16::try_from(tz_bytes.len())
                    .unwrap_or_else(|err| vortex_panic!("tz did not fit in u16: {}", err));
                bytes.extend_from_slice(tz_len.to_le_bytes().as_slice());
                bytes.extend_from_slice(tz_bytes);
            }
        }

        Ok(bytes)
    }

    fn deserialize_metadata(&self, data: &[u8]) -> VortexResult<Self::Metadata> {
        vortex_ensure!(
            data.len() >= 3,
            "Timestamp metadata must have at least 3 bytes, got {}",
            data.len()
        );

        let tag = data[0];
        let time_unit = TimeUnit::try_from(tag)?;
        let tz_len_bytes: [u8; 2] = data[1..3]
            .try_into()
            .ok()
            .vortex_expect("Verified to have two bytes");
        let tz_len = u16::from_le_bytes(tz_len_bytes) as usize;
        if tz_len == 0 {
            return Ok(TimestampOptions {
                unit: time_unit,
                tz: None,
            });
        }

        // Attempt to load from len-prefixed bytes
        vortex_ensure!(
            data.len() >= 3 + tz_len,
            "Timestamp metadata is truncated: declared timezone length {} but only {} bytes available",
            tz_len,
            data.len() - 3
        );
        let tz_bytes = &data[3..3 + tz_len];
        let tz: Arc<str> = str::from_utf8(tz_bytes)
            .map_err(|e| vortex_err!("timezone is not valid utf8 string: {e}"))?
            .to_string()
            .into();
        Ok(TimestampOptions {
            unit: time_unit,
            tz: Some(tz),
        })
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        vortex_ensure!(
            matches!(ext_dtype.storage_dtype(), DType::Primitive(PType::I64, _)),
            "Timestamp storage dtype must be i64"
        );
        Ok(())
    }

    fn unpack_native<'a>(
        ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        let metadata = ext_dtype.metadata();
        let ts_value = storage_value.as_primitive().cast::<i64>()?;
        let tz = metadata.tz.as_ref();

        let value = match metadata.unit {
            TimeUnit::Nanoseconds => TimestampValue::Nanoseconds(ts_value, tz),
            TimeUnit::Microseconds => TimestampValue::Microseconds(ts_value, tz),
            TimeUnit::Milliseconds => TimestampValue::Milliseconds(ts_value, tz),
            TimeUnit::Seconds => TimestampValue::Seconds(ts_value, tz),
            TimeUnit::Days => vortex_bail!("Timestamp does not support Days time unit"),
        };
        // DuckDB and Arrow reserve the i64 extrema as timestamp infinities.
        if matches!(ts_value, i64::MIN | i64::MAX) {
            return Ok(value);
        }

        if let Some(tz) = tz {
            jiff::tz::TimeZone::get(tz.as_ref())
                .map_err(|e| vortex_err!("Invalid timezone for timestamp scalar: {}", e))?;
        }

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_error::VortexResult;

    use crate::dtype::DType;
    use crate::dtype::Nullability::Nullable;
    use crate::extension::datetime::TimeUnit;
    use crate::extension::datetime::Timestamp;
    use crate::scalar::PValue;
    use crate::scalar::Scalar;
    use crate::scalar::ScalarValue;

    #[test]
    fn validate_timestamp_scalar() -> VortexResult<()> {
        let dtype = DType::Extension(Timestamp::new(TimeUnit::Seconds, Nullable).erased());
        Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(0))))?;

        Ok(())
    }

    #[test]
    fn validate_full_range_timestamp_scalars() -> VortexResult<()> {
        let dtype = DType::Extension(Timestamp::new(TimeUnit::Microseconds, Nullable).erased());
        let positive = Scalar::try_new(
            dtype.clone(),
            Some(ScalarValue::Primitive(PValue::I64(i64::MAX))),
        )?;
        let negative = Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(i64::MIN))))?;

        assert_eq!(format!("{}", positive.as_extension()), "infinity");
        assert_eq!(format!("{}", negative.as_extension()), "-infinity");

        let finite_extreme = Scalar::try_new(
            DType::Extension(Timestamp::new(TimeUnit::Microseconds, Nullable).erased()),
            Some(ScalarValue::Primitive(PValue::I64(i64::MAX - 1))),
        )?;
        assert_eq!(
            format!("{}", finite_extreme.as_extension()),
            "9223372036854775806µs"
        );
        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn reject_timestamp_with_invalid_timezone() {
        let dtype = DType::Extension(
            Timestamp::new_with_tz(
                TimeUnit::Seconds,
                Some(Arc::from("Not/A/Timezone")),
                Nullable,
            )
            .erased(),
        );
        let result = Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(0))));
        assert!(result.is_err());
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn display_timestamp_scalar() {
        // Local (no timezone) timestamp.
        let local_dtype = DType::Extension(Timestamp::new(TimeUnit::Seconds, Nullable).erased());
        let scalar = Scalar::new(local_dtype, Some(ScalarValue::Primitive(PValue::I64(0))));
        assert_eq!(format!("{}", scalar.as_extension()), "1970-01-01T00:00:00Z");

        // Zoned timestamp.
        let zoned_dtype = DType::Extension(
            Timestamp::new_with_tz(
                TimeUnit::Seconds,
                Some(Arc::from("America/New_York")),
                Nullable,
            )
            .erased(),
        );
        let scalar = Scalar::new(zoned_dtype, Some(ScalarValue::Primitive(PValue::I64(0))));
        assert_eq!(
            format!("{}", scalar.as_extension()),
            "1969-12-31T19:00:00-05:00[America/New_York]"
        );
    }

    #[test]
    fn deserialize_empty_metadata_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Timestamp;
        assert!(vtable.deserialize_metadata(&[]).is_err());
    }

    #[test]
    fn deserialize_too_short_metadata_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Timestamp;
        // Only 2 bytes - too short for the required 3-byte header.
        assert!(vtable.deserialize_metadata(&[0x00, 0x01]).is_err());
    }

    #[test]
    fn deserialize_truncated_timezone_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Timestamp;
        // Valid tag (0x00 = Nanoseconds), tz_len = 10 (little-endian: [0x0A, 0x00]),
        // but only 3 bytes of timezone data instead of the declared 10.
        let data = [0x00u8, 0x0A, 0x00, b'U', b'T', b'C'];
        assert!(vtable.deserialize_metadata(&data).is_err());
    }
}
