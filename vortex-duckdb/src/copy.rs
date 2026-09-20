// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use async_fs::OpenOptions;
use futures::SinkExt;
use futures::TryStreamExt;
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use object_store::ObjectStore;
use object_store::registry::ObjectStoreRegistry;
use parking_lot::Mutex;
use static_assertions::assert_impl_all;
use vortex::array::ArrayRef;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::buffer::ByteBuffer;
use vortex::dtype::DType;
use vortex::dtype::FieldName;
use vortex::dtype::Nullability::NonNullable;
use vortex::dtype::Nullability::Nullable;
use vortex::dtype::StructFields;
use vortex::editions::ComponentKind;
use vortex::editions::EditionSessionExt;
use vortex::error::VortexExpect;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::file::OpenOptionsSessionExt;
use vortex::expr::stats::Precision;
use vortex::expr::stats::Stat;
use vortex::file::WriteOptionsSessionExt;
use vortex::file::WriteStrategyBuilder;
use vortex::file::WriteSummary;
use vortex::file::multi::parse_uri_or_path;
use vortex::io::VortexWrite;
use vortex::io::compat::Compat;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::Task;
use vortex::io::session::RuntimeSessionExt;
use vortex::scalar::Scalar;
use vortex::scalar::ScalarValue;

/// DuckLake schema-identity metadata key written into Vortex user metadata segments.
pub const DUCKLAKE_FIELD_IDS_METADATA_KEY: &str = "ducklake.field_ids";

use crate::REGISTRY;
use crate::RUNTIME;
use crate::SESSION;
use crate::convert::FromLogicalType;
use crate::convert::ToDuckDBScalar;
use crate::convert::data_chunk_to_vortex;
use crate::duckdb::DataChunkRef;
use crate::duckdb::LogicalTypeRef;
use crate::duckdb::Value;

fn copy_write_options() -> vortex::file::VortexWriteOptions {
    let strategy = WriteStrategyBuilder::default()
        .with_allow_encodings(
            SESSION
                .enabled_component_ids(ComponentKind::Array)
                .into_iter()
                .collect(),
        )
        .for_ingest()
        .build();
    SESSION.write_options().with_strategy(strategy)
}

fn uuid_stats_string(value: &ScalarValue) -> Option<String> {
    let elements = value.as_list();
    if elements.len() != 16 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, elem) in elements.iter().enumerate() {
        let Some(scalar_value) = elem else {
            return None;
        };
        let vortex::scalar::PValue::U8(b) = *scalar_value.as_primitive() else {
            return None;
        };
        bytes[i] = b;
    }
    // RFC-4122 hyphenated form expected by DuckLake stats assertions.
    Some(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    ))
}

pub(crate) fn scalar_value_to_stats_string(dtype: &DType, value: ScalarValue) -> Option<String> {
    // UUID min/max must be hyphenated strings for DuckLake. Footer stats may round-trip the
    // extension as ForeignExtDType, so parse storage bytes by id instead of try_downcast.
    if let DType::Extension(ext) = dtype {
        if ext.id().as_ref() == "vortex.uuid" {
            return uuid_stats_string(&value);
        }
    }
    let scalar = Scalar::try_new(dtype.clone(), Some(value)).ok()?;
    let duck = scalar.try_to_duckdb_scalar().ok()?;
    // Typed NULLs from unsupported extension stats are not useful min/max.
    if matches!(duck.extract(), crate::duckdb::ExtractedValue::Null) {
        return None;
    }
    Some(duck.to_string())
}

#[derive(Clone)]
pub struct CopyFunctionBind {
    dtype: DType,
    fields: StructFields,
}
assert_impl_all!(CopyFunctionBind: Send, Clone);

/// The per-column compressed sizes are computed once here rather than per column, since DuckDB
/// queries statistics one column at a time.
struct FinishedWrite {
    summary: WriteSummary,
    column_sizes: Vec<u64>,
}

/// Write to a file has two phases, writing data chunks and then closing the file.
/// We use a spawned tokio task to actually compress arrays and write it to disk.
/// Each chunk is pushed into the sink and read from the task.
/// Once finished we can close all sinks and then the task can be awaited and the file
/// flushed to disk.
pub struct CopyFunctionGlobal {
    write_task: Mutex<Option<Task<VortexResult<WriteSummary>>>>,
    finished: Mutex<Option<FinishedWrite>>,
    sink: Option<Sender<VortexResult<ArrayRef>>>,
}
assert_impl_all!(CopyFunctionGlobal: Send, Sync);

pub fn copy_to_bind(
    column_names: &[String],
    column_types: &[&LogicalTypeRef],
) -> VortexResult<CopyFunctionBind> {
    let fields: StructFields = column_names
        .iter()
        .zip(column_types)
        .map(|(name, type_)| {
            Ok((
                FieldName::from(name.as_ref()),
                DType::from_logical_type(type_, Nullable)?,
            ))
        })
        .collect::<VortexResult<StructFields>>()?;

    Ok(CopyFunctionBind {
        dtype: DType::Struct(fields.clone(), NonNullable),
        fields,
    })
}

fn push_to_writer(global: &CopyFunctionGlobal, array: ArrayRef) -> VortexResult<()> {
    let mut sink = global
        .sink
        .as_ref()
        .ok_or_else(|| vortex_err!("sink closed early"))?
        .clone();
    RUNTIME.block_on(async {
        // send may error with "receiver is gone" which isn't the real error
        if sink.send(Ok(array)).await.is_ok() {
            return Ok(());
        }
        let task = global.write_task.lock().take();
        if let Some(task) = task {
            // we can get the real error (i.e invalid path) from here
            task.await?;
        }
        vortex_bail!("Writer stopped before all data was written")
    })
}

pub fn copy_to_sink(
    bind_data: &CopyFunctionBind,
    init_global: &CopyFunctionGlobal,
    chunk: &mut DataChunkRef,
) -> VortexResult<()> {
    push_to_writer(
        init_global,
        data_chunk_to_vortex(bind_data.fields.names(), chunk)?,
    )
}

#[derive(Default)]
pub struct CopyPreparedBatch {
    arrays: Vec<ArrayRef>,
}

pub fn prepare_batch_push(
    bind: &CopyFunctionBind,
    batch: &mut CopyPreparedBatch,
    chunk: &DataChunkRef,
) -> VortexResult<()> {
    batch
        .arrays
        .push(data_chunk_to_vortex(bind.fields.names(), chunk)?);
    Ok(())
}

pub fn flush_batch(global: &CopyFunctionGlobal, batch: &CopyPreparedBatch) -> VortexResult<()> {
    for array in &batch.arrays {
        push_to_writer(global, array.clone())?;
    }
    Ok(())
}

pub fn copy_to_finalize(init_global: &mut CopyFunctionGlobal) -> VortexResult<()> {
    RUNTIME.block_on(async {
        if let Some(sink) = init_global.sink.take() {
            drop(sink)
        }
        let task = init_global
            .write_task
            .lock()
            .take()
            .vortex_expect("no file to close");
        // Keep the write summary (footer + size) so DuckLake can read per-file statistics back
        // without re-opening the file. Compute the per-column compressed sizes once, up front.
        let summary = task.await?;
        let column_sizes = summary.compressed_column_sizes().unwrap_or_default();
        *init_global.finished.lock() = Some(FinishedWrite {
            summary,
            column_sizes,
        });
        Ok(())
    })
}

/// File-level statistics of the written Vortex file, for the WRITTEN_FILE_STATISTICS return path.
pub(crate) struct WrittenFileStats {
    pub row_count: u64,
    pub file_size_bytes: u64,
    pub footer_size_bytes: u64,
    pub num_columns: usize,
}

/// Per-column statistics of the written Vortex file. `min`/`max` are DuckDB values converted from
/// the Vortex scalar; every field is optional and omitted when the statistic is not available.
pub(crate) struct WrittenColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: Option<u64>,
    pub num_values: u64,
    pub column_size_bytes: Option<u64>,
    pub has_nan: Option<bool>,
}

/// Read file-level statistics back from the finished write. `None` before finalize.
pub(crate) fn written_file_stats(global: &CopyFunctionGlobal) -> Option<WrittenFileStats> {
    let guard = global.finished.lock();
    Some(file_stats_from_summary(&guard.as_ref()?.summary))
}

/// Read per-column statistics for `column_index` from the finished write. `Ok(None)` if the file is
/// not finalized.
pub(crate) fn written_column_stats(
    global: &CopyFunctionGlobal,
    column_index: usize,
) -> VortexResult<Option<WrittenColumnStats>> {
    let guard = global.finished.lock();
    let Some(finished) = guard.as_ref() else {
        return Ok(None);
    };
    column_stats_from_summary(&finished.summary, column_index, &finished.column_sizes).map(Some)
}

fn file_stats_from_summary(summary: &WriteSummary) -> WrittenFileStats {
    let num_columns = summary
        .footer()
        .statistics()
        .map_or(0, |s| s.stats_sets().len());
    WrittenFileStats {
        row_count: summary.row_count(),
        file_size_bytes: summary.size(),
        // Vortex has no separate footer-size hint; 0 means "read the footer normally".
        footer_size_bytes: 0,
        num_columns,
    }
}

/// Per-column statistics from a finished write's summary and its precomputed compressed sizes
/// (`column_sizes`, indexed the same as the footer's stats sets).
///
/// Only top-level columns are covered: the footer exposes one statistics set per top-level field,
/// so nested struct/list leaf columns are not reported (parquet, by contrast, recurses to leaf
/// paths). Flat tables - the common DuckLake case - are fully covered.
fn column_stats_from_summary(
    summary: &WriteSummary,
    column_index: usize,
    column_sizes: &[u64],
) -> VortexResult<WrittenColumnStats> {
    let file_stats = summary
        .footer()
        .statistics()
        .ok_or_else(|| vortex_err!("written file has no statistics"))?;
    let stats_sets = file_stats.stats_sets();
    if column_index >= stats_sets.len() {
        vortex_bail!(
            "column index {column_index} out of range for {} statistics sets",
            stats_sets.len()
        );
    }
    let stats = &stats_sets[column_index];
    let dtype = &file_stats.dtypes()[column_index];

    Ok(WrittenColumnStats {
        min: exact_scalar_to_duckdb(stats.get(Stat::Min), dtype)?,
        max: exact_scalar_to_duckdb(stats.get(Stat::Max), dtype)?,
        null_count: exact_u64(stats.get(Stat::NullCount)),
        // NaNCount is exact only for float columns, so this is emitted just for them (as in parquet).
        has_nan: exact_u64(stats.get(Stat::NaNCount)).map(|count| count > 0),
        num_values: summary.row_count(),
        // On-disk compressed size; excludes bytes not attributable to a column (e.g. struct validity).
        column_size_bytes: column_sizes.get(column_index).copied(),
    })
}

/// Convert an exact scalar statistic to a DuckDB value, propagating a conversion failure rather than
/// dropping it. `Ok(None)` when the statistic is not exactly known.
fn exact_scalar_to_duckdb(
    stat: Precision<ScalarValue>,
    dtype: &DType,
) -> VortexResult<Option<Value>> {
    match stat {
        Precision::Exact(value) => Ok(Some(
            Scalar::try_new(dtype.clone(), Some(value))?.try_to_duckdb_scalar()?,
        )),
        _ => Ok(None),
    }
}

/// Extract an exact `u64` statistic (e.g. a count), or `None` if not exactly known.
fn exact_u64(stat: Precision<ScalarValue>) -> Option<u64> {
    match stat {
        Precision::Exact(value) => value.as_primitive().as_u64(),
        _ => None,
    }
}

pub fn copy_to_initialize_global(
    bind_data: &CopyFunctionBind,
    file_path: String,
    field_ids_metadata: Option<Vec<u8>>,
    encryption_key: Option<Vec<u8>>,
) -> VortexResult<CopyFunctionGlobal> {
    // The channel size 32 was chosen arbitrarily.
    let (sink, rx) = mpsc::channel(32);
    let array_stream = ArrayStreamAdapter::new(bind_data.dtype.clone(), rx.into_stream());

    let handle = SESSION.handle();
    let field_ids_metadata = field_ids_metadata.map(ByteBuffer::from);
    let encryption_key = match encryption_key {
        Some(bytes) => Some(vortex::file::SegmentEncryptionKey::try_new(bytes)?),
        None => None,
    };

    let url = parse_uri_or_path(&file_path)?;
    let write_task = if url.scheme() == "file" {
        handle.spawn(async move {
            let mut writer = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(file_path)
                .await?;
            let mut options = copy_write_options();
            if let Some(meta) = field_ids_metadata {
                options = options.with_metadata_segment(DUCKLAKE_FIELD_IDS_METADATA_KEY, meta);
            }
            if let Some(key) = encryption_key {
                options = options.with_encryption_key(key);
            }
            let summary = options.write(&mut writer, array_stream).await?;
            writer.shutdown().await?;
            Ok(summary)
        })
    } else {
        let (object_store, path) = REGISTRY.resolve(&url)?;
        let object_store = Arc::new(Compat::new(object_store)) as Arc<dyn ObjectStore>;
        handle.spawn(async move {
            let mut writer = ObjectStoreWrite::new(object_store, &path).await?;
            let mut options = copy_write_options();
            if let Some(meta) = field_ids_metadata {
                options = options.with_metadata_segment(DUCKLAKE_FIELD_IDS_METADATA_KEY, meta);
            }
            if let Some(key) = encryption_key {
                options = options.with_encryption_key(key);
            }
            let summary = options.write(&mut writer, array_stream).await?;
            writer.shutdown().await?;
            Ok(summary)
        })
    };

    Ok(CopyFunctionGlobal {
        write_task: Mutex::new(Some(write_task)),
        finished: Mutex::new(None),
        sink: Some(sink),
    })
}

/// Read the `ducklake.field_ids` user metadata segment from a Vortex file, if present.
pub fn read_ducklake_field_ids_metadata(file_path: &str) -> VortexResult<Option<Vec<u8>>> {
    RUNTIME.block_on(async {
        let file = SESSION
            .open_options()
            .include_metadata()
            .open_path(file_path)
            .await?;
        Ok(file
            .metadata_segment(DUCKLAKE_FIELD_IDS_METADATA_KEY)
            .map(|buf| buf.as_slice().to_vec()))
    })
}

#[cfg(test)]
mod tests {
    use vortex::array::IntoArray;
    use vortex::array::arrays::StructArray;
    use vortex::array::stats::PRUNING_STATS;
    use vortex::buffer::ByteBufferMut;
    use vortex::buffer::buffer;

    use super::*;

    /// Writes a one-column file and returns its summary, with `file_statistics` controlling which
    /// statistics the footer carries (empty means none at all).
    fn write_summary(file_statistics: Vec<Stat>) -> WriteSummary {
        RUNTIME.block_on(async {
            let array = StructArray::from_fields(&[("i", buffer![1u32, 2, 3].into_array())])
                .unwrap()
                .into_array();
            let mut buf = ByteBufferMut::empty();
            let mut writer = SESSION
                .write_options()
                .with_file_statistics(file_statistics)
                .writer(&mut buf, array.dtype().clone());
            writer.push(array).await.unwrap();
            writer.finish().await.unwrap()
        })
    }

    #[test]
    fn column_stats_out_of_range_is_an_error() {
        let summary = write_summary(PRUNING_STATS.to_vec());
        assert!(column_stats_from_summary(&summary, 0, &[]).is_ok());
        assert!(column_stats_from_summary(&summary, 1, &[]).is_err());
    }

    #[test]
    fn column_stats_without_file_statistics_is_an_error() {
        let summary = write_summary(vec![]);
        assert!(column_stats_from_summary(&summary, 0, &[]).is_err());
    }
}
