// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::cmp::Ordering;
use std::collections::BTreeMap;
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
use vortex::array::ExecutionCtx;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::FixedSizeListArray;
use vortex::array::arrays::ListViewArray;
use vortex::array::arrays::MapArray;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::fixed_size_list::FixedSizeListArrayExt;
use vortex::array::arrays::fixed_size_list::FixedSizeListArraySlotsExt;
use vortex::array::arrays::listview::ListViewArraySlotsExt;
use vortex::array::arrays::map::MapArraySlotsExt;
use vortex::array::arrays::struct_::StructArrayExt;
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
use vortex::error::vortex_err;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::file::WriteStrategyBuilder;
use vortex::file::WriteSummary;
use vortex::file::multi::parse_uri_or_path;
use vortex::io::VortexWrite;
use vortex::io::compat::Compat;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::Task;
use vortex::io::runtime::current::CurrentThreadWorkerPool;
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

/// DuckLake-shaped per-column stats collected at finalize time.
#[derive(Debug, Clone)]
pub struct ExportedColumnStatistics {
    pub name: String,
    pub null_count: Option<u64>,
    pub num_values: Option<u64>,
    pub column_size_bytes: Option<u64>,
    pub min: Option<String>,
    pub max: Option<String>,
    pub has_nan: Option<bool>,
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

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

fn join_stats_path(prefix: &str, name: &str) -> String {
    let seg = quote_ident(name);
    if prefix.is_empty() {
        seg
    } else {
        format!("{prefix}.{seg}")
    }
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

#[derive(Default)]
struct LeafAcc {
    dtype: Option<DType>,
    null_count: u64,
    num_values: u64,
    min: Option<Scalar>,
    max: Option<Scalar>,
    has_nan: Option<bool>,
}

impl LeafAcc {
    fn merge_leaf(&mut self, array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<()> {
        use vortex::expr::stats::Stat;

        self.dtype.get_or_insert_with(|| array.dtype().clone());
        self.num_values += array.len() as u64;

        // ponytail: UUID stats scan each scalar until the extension gains Min/Max kernels.
        if is_uuid_ext(array.dtype()) {
            for i in 0..array.len() {
                let s = array.execute_scalar(i, ctx)?;
                if s.is_null() {
                    self.null_count += 1;
                    continue;
                }
                match &self.min {
                    None => self.min = Some(s.clone()),
                    Some(cur) if s.partial_cmp(cur) == Some(Ordering::Less) => {
                        self.min = Some(s.clone())
                    }
                    _ => {}
                }
                match &self.max {
                    None => self.max = Some(s.clone()),
                    Some(cur) if s.partial_cmp(cur) == Some(Ordering::Greater) => {
                        self.max = Some(s.clone())
                    }
                    _ => {}
                }
            }
            return Ok(());
        }

        if let Some(v) = array.statistics().compute_stat(Stat::NullCount, ctx)? {
            if let Some(n) = v.as_primitive().as_::<u64>() {
                self.null_count += n;
            }
        }
        if let Some(s) = array.statistics().compute_stat(Stat::Min, ctx)? {
            match &self.min {
                None => self.min = Some(s),
                Some(cur) if s.partial_cmp(cur) == Some(Ordering::Less) => self.min = Some(s),
                _ => {}
            }
        }
        if let Some(s) = array.statistics().compute_stat(Stat::Max, ctx)? {
            match &self.max {
                None => self.max = Some(s),
                Some(cur) if s.partial_cmp(cur) == Some(Ordering::Greater) => self.max = Some(s),
                _ => {}
            }
        }
        if let Some(v) = array.statistics().compute_stat(Stat::NaNCount, ctx)? {
            if let Some(n) = v.as_primitive().as_::<u64>() {
                self.has_nan = Some(self.has_nan.unwrap_or(false) || n > 0);
            }
        }
        Ok(())
    }

    fn into_exported(self, name: String) -> Option<ExportedColumnStatistics> {
        let dtype = self.dtype?;
        let min = self.min.as_ref().and_then(|s| {
            s.value()
                .cloned()
                .and_then(|v| scalar_value_to_stats_string(&dtype, v))
        });
        let max = self.max.as_ref().and_then(|s| {
            s.value()
                .cloned()
                .and_then(|v| scalar_value_to_stats_string(&dtype, v))
        });
        Some(ExportedColumnStatistics {
            name,
            null_count: Some(self.null_count),
            num_values: Some(self.num_values),
            column_size_bytes: None,
            min,
            max,
            has_nan: self.has_nan,
        })
    }
}

fn is_uuid_ext(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Extension(ext) if ext.id().as_ref() == "vortex.uuid"
    )
}

fn is_variant_ext(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Extension(ext) if ext.id().as_ref() == crate::convert::ext_types::VARIANT_EXT_ID
    )
}

/// Recursively accumulate DuckLake-shaped nested leaf stats (`"col"."element"`, `"s"."a"`, …).
///
/// Vortex file footers only store top-level field stats today; DuckLake RETURN_STATS needs leaf
/// paths matching Parquet. ponytail: ListView may retain unreferenced element slots — upgrade to a
/// referenced-only compact when writers start leaving sparse garbage that skews min/max.
fn accumulate_nested(
    path: &str,
    array: &ArrayRef,
    out: &mut BTreeMap<String, LeafAcc>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    if is_variant_ext(array.dtype()) {
        return Ok(());
    }

    match array.dtype() {
        DType::Struct(fields, _) => {
            let struct_array = array.clone().execute::<StructArray>(ctx)?;
            for (name, field) in fields
                .names()
                .iter()
                .zip(struct_array.iter_unmasked_fields())
            {
                let child_path = join_stats_path(path, name.as_ref());
                accumulate_nested(&child_path, field, out, ctx)?;
            }
            Ok(())
        }
        DType::List(_, _) => {
            // DuckDB COPY always builds ListView for lists.
            let elements = array
                .clone()
                .execute::<ListViewArray>(ctx)?
                .elements()
                .clone();
            let child_path = join_stats_path(path, "element");
            accumulate_nested(&child_path, &elements, out, ctx)
        }
        DType::FixedSizeList(_, _, _) => {
            let fsl = array.clone().execute::<FixedSizeListArray>(ctx)?;
            let child_path = join_stats_path(path, "element");
            accumulate_nested(&child_path, fsl.elements(), out, ctx)
        }
        DType::Map(_, _) => {
            // Parquet/DuckLake map paths are `"m"."key"` / `"m"."value"` (no list "element").
            let map = array.clone().execute::<MapArray>(ctx)?;
            let entries = map.entries().clone();
            let entry_structs = entries.execute::<ListViewArray>(ctx)?.elements().clone();
            accumulate_nested(path, &entry_structs, out, ctx)
        }
        _ => {
            if path.is_empty() {
                return Ok(());
            }
            out.entry(path.to_string())
                .or_default()
                .merge_leaf(array, ctx)
        }
    }
}

fn accumulate_top_level_nested(
    array: &ArrayRef,
    out: &mut BTreeMap<String, LeafAcc>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    let DType::Struct(fields, _) = array.dtype() else {
        return Ok(());
    };
    let struct_array = array.clone().execute::<StructArray>(ctx)?;
    for (name, field) in fields
        .names()
        .iter()
        .zip(struct_array.iter_unmasked_fields())
    {
        match field.dtype() {
            DType::Struct(_, _)
            | DType::List(_, _)
            | DType::FixedSizeList(_, _, _)
            | DType::Map(_, _) => {
                let path = quote_ident(name.as_ref());
                accumulate_nested(&path, field, out, ctx)?;
            }
            // File-footer Min/Max are not computed for UUID extension arrays; gather here.
            dtype if is_uuid_ext(dtype) => {
                out.entry(name.to_string())
                    .or_default()
                    .merge_leaf(field, ctx)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn export_write_summary_stats(
    summary: &WriteSummary,
    row_count: u64,
    nested_leaves: BTreeMap<String, LeafAcc>,
) -> VortexResult<Vec<ExportedColumnStatistics>> {
    let mut out = Vec::new();
    let Some(file_stats) = summary.footer().statistics() else {
        for (name, acc) in nested_leaves {
            if let Some(exported) = acc.into_exported(name) {
                out.push(exported);
            }
        }
        return Ok(out);
    };
    let column_sizes = summary.compressed_column_sizes().unwrap_or_default();
    let names: Vec<String> = match summary.footer().dtype() {
        DType::Struct(fields, _) => fields.names().iter().map(|n| n.to_string()).collect(),
        _ => vec!["value".to_string()],
    };

    for (idx, (stats, dtype)) in file_stats
        .stats_sets()
        .iter()
        .zip(file_stats.dtypes().iter())
        .enumerate()
    {
        use vortex::expr::stats::Precision;
        use vortex::expr::stats::Stat;

        if is_variant_ext(dtype) {
            continue;
        }
        // Nested containers: leaf paths are emitted separately below.
        // UUID: footer Min/Max unsupported; sink-accumulated leaves used instead.
        if matches!(
            dtype,
            DType::Struct(_, _)
                | DType::List(_, _)
                | DType::FixedSizeList(_, _, _)
                | DType::Map(_, _)
        ) || is_uuid_ext(dtype)
        {
            continue;
        }
        let name = names
            .get(idx)
            .cloned()
            .unwrap_or_else(|| format!("col_{idx}"));
        let null_count = match stats.get(Stat::NullCount) {
            Precision::Exact(v) => v.as_primitive().as_u64(),
            _ => None,
        };
        let has_nan = match stats.get(Stat::NaNCount) {
            Precision::Exact(v) => v.as_primitive().as_u64().map(|c| c > 0),
            _ => None,
        };
        let min = match stats.get(Stat::Min) {
            Precision::Exact(v) => scalar_value_to_stats_string(dtype, v),
            _ => None,
        };
        let max = match stats.get(Stat::Max) {
            Precision::Exact(v) => scalar_value_to_stats_string(dtype, v),
            _ => None,
        };
        out.push(ExportedColumnStatistics {
            name,
            null_count,
            num_values: Some(row_count),
            column_size_bytes: column_sizes.get(idx).copied(),
            min,
            max,
            has_nan,
        });
    }

    for (name, acc) in nested_leaves {
        if let Some(exported) = acc.into_exported(name) {
            out.push(exported);
        }
    }
    Ok(out)
}

#[derive(Clone)]
pub struct CopyFunctionBind {
    dtype: DType,
    fields: StructFields,
}
assert_impl_all!(CopyFunctionBind: Send, Clone);

/// Write to a file has two phases, writing data chunks and then closing the file.
/// We use a spawned tokio task to actually compress arrays and write it to disk.
/// Each chunk is pushed into the sink and read from the task.
/// Once finished we can close all sinks and then the task can be awaited and the file
/// flushed to disk.
pub struct CopyFunctionGlobal {
    write_task: Mutex<Option<Task<VortexResult<WriteSummary>>>>,
    sink: Option<Sender<VortexResult<ArrayRef>>>,
    // Pool of background workers helping to drive the write task.
    // Note that this is optional and without it, we would only drive the task when DuckDB calls
    // into us, and we call `RUNTIME.block_on`.
    // TODO(myrrc): we should rely only on host threads, remove this
    #[expect(dead_code)]
    worker_pool: CurrentThreadWorkerPool,
    /// Nested leaf stats accumulated during sink (Parquet-shaped dotted paths).
    nested_leaf_stats: Mutex<BTreeMap<String, LeafAcc>>,
    /// Populated by finalize for C++ to pull into CopyFunctionFileStatistics.
    pub exported_stats: Vec<ExportedColumnStatistics>,
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

pub fn copy_to_sink(
    bind_data: &CopyFunctionBind,
    init_global: &CopyFunctionGlobal,
    chunk: &mut DataChunkRef,
) -> VortexResult<()> {
    let chunk = data_chunk_to_vortex(bind_data.fields.names(), chunk);
    if let Ok(ref array) = chunk {
        let mut nested = init_global.nested_leaf_stats.lock();
        let mut ctx = SESSION.create_execution_ctx();
        accumulate_top_level_nested(array, &mut nested, &mut ctx)?;
    }
    let mut sink = init_global
        .sink
        .as_ref()
        .ok_or_else(|| vortex_err!("sink closed early"))?
        .clone();
    RUNTIME
        .block_on(sink.send(chunk))
        .map_err(|e| vortex_err!("send error {e}"))?;
    Ok(())
}

pub fn copy_to_finalize(init_global: &mut CopyFunctionGlobal) -> VortexResult<(u64, u64)> {
    RUNTIME.block_on(async {
        if let Some(sink) = init_global.sink.take() {
            drop(sink)
        }
        let task = init_global
            .write_task
            .lock()
            .take()
            .vortex_expect("no file to close");
        let summary = task.await?;
        let row_count = summary.row_count();
        let size = summary.size();
        let nested = std::mem::take(&mut *init_global.nested_leaf_stats.lock());
        init_global.exported_stats = export_write_summary_stats(&summary, row_count, nested)?;
        Ok((row_count, size))
    })
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

    let worker_pool = RUNTIME.new_pool();
    worker_pool.set_workers_to_available_parallelism();
    Ok(CopyFunctionGlobal {
        worker_pool,
        write_task: Mutex::new(Some(write_task)),
        sink: Some(sink),
        nested_leaf_stats: Mutex::new(BTreeMap::new()),
        exported_stats: Vec::new(),
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
    use super::join_stats_path;
    use super::quote_ident;

    #[test]
    fn nested_stats_paths_match_parquet_shape() {
        assert_eq!(quote_ident("l"), "\"l\"");
        assert_eq!(
            join_stats_path(&quote_ident("l"), "element"),
            "\"l\".\"element\""
        );
        assert_eq!(
            join_stats_path(&join_stats_path(&quote_ident("m"), "key"), "x"),
            "\"m\".\"key\".\"x\""
        );
    }
}
