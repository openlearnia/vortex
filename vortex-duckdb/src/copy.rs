// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_fs::OpenOptions;
use futures::SinkExt;
use futures::TryStreamExt;
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use num_traits::Zero;
use object_store::ObjectStore;
use object_store::registry::ObjectStoreRegistry;
use parking_lot::Mutex;
use static_assertions::assert_impl_all;
use vortex::array::ArrayRef;
use vortex::array::Canonical;
use vortex::array::ExecutionCtx;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::ListView;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::extension::ExtensionArrayExt;
use vortex::array::arrays::fixed_size_list::FixedSizeListArrayExt;
use vortex::array::arrays::fixed_size_list::FixedSizeListArraySlotsExt;
use vortex::array::arrays::listview::ListViewArraySlotsExt;
use vortex::array::arrays::map::MapArraySlotsExt;
use vortex::array::arrays::primitive::PrimitiveArrayExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::match_each_unsigned_integer_ptype;
use vortex::array::stats::StatsSet;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::buffer::BufferString;
use vortex::buffer::ByteBuffer;
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::dtype::DType;
use vortex::dtype::Field;
use vortex::dtype::FieldName;
use vortex::dtype::FieldNames;
use vortex::dtype::FieldPath;
use vortex::dtype::Nullability::NonNullable;
use vortex::dtype::Nullability::Nullable;
use vortex::dtype::StructFields;

use vortex::error::VortexExpect;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_ensure;
use vortex::error::vortex_err;
use vortex::extension::uuid::Uuid;
use vortex::file::CompressedFieldSizes;
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
use vortex::mask::Mask;
use vortex::scalar::Scalar;
use vortex::scalar::ScalarTruncation;
use vortex::scalar::ScalarValue;
use vortex::scalar::lower_bound;
use vortex::scalar::upper_bound;
use vortex_btrblocks::SchemeExt;

/// DuckLake schema-identity metadata key written into Vortex user metadata segments.
pub const DUCKLAKE_FIELD_IDS_METADATA_KEY: &str = "ducklake.field_ids";

use crate::REGISTRY;
use crate::RUNTIME;
use crate::SESSION;
use crate::convert::FromLogicalType;
use crate::convert::ToDuckDBScalar;
use crate::convert::data_chunk_to_vortex;
use crate::convert::ext_types::DuckVariant;
use crate::duckdb::DataChunkRef;
use crate::duckdb::LogicalTypeRef;
use crate::duckdb::Value;

fn copy_write_options() -> vortex::file::VortexWriteOptions {
    // for_ingest plus DeltaScheme: fastlanes.delta is not part of any declared edition, so the
    // writer's serialization context would reject files that contain it.
    let strategy = WriteStrategyBuilder::default()
        .for_ingest()
        .with_btrblocks_builder(
            BtrBlocksCompressorBuilder::default().exclude_schemes([
                vortex_btrblocks::schemes::integer::RunEndScheme.id(),
                vortex_btrblocks::schemes::integer::IntRLEScheme.id(),
                vortex_btrblocks::schemes::float::ALPRDScheme.id(),
                vortex_btrblocks::schemes::float::FloatRLEScheme.id(),
                vortex_btrblocks::schemes::integer::DeltaScheme::default().id(),
            ]),
        )
        .with_data_block_target_bytes(Some(16 << 20))
        .build();
    SESSION.write_options().with_strategy(strategy)
}

/// RFC-4122 hyphenated form expected by DuckLake stats assertions.
fn uuid_bytes_string(bytes: &[u8; 16]) -> String {
    format!(
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
    )
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
    Some(uuid_bytes_string(&bytes))
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
    field_sizes: CompressedFieldSizes,
}

/// Statistics accumulated for a leaf path nested below a top-level column
/// (e.g. `["s", "a"]` or `["l", "element"]`), merged across pushed chunks.
struct LeafStatsAccum {
    path: Vec<Field>,
    dtype: DType,
    stats: StatsSet,
    num_values: u64,
    /// Null count of enclosing list/array/map nodes, added to the leaf's
    /// `null_count` to match parquet definition-level semantics (a NULL list
    /// contributes one null leaf value).
    extra_nulls: u64,
}

/// Untruncated min/max bounds for a top-level Utf8/Binary column, merged across
/// pushed chunks. The footer truncates varlen bounds at 64 bytes, while
/// DuckLake stores the parquet writer's 256-byte bounds.
struct VarlenBounds {
    dtype: DType,
    stats: StatsSet,
}

/// Min/max accumulated for `vortex.uuid` leaf paths. UUID's
/// `FixedSizeList<Primitive<U8>, 16>` storage has no statistics kernels, and its
/// big-endian byte layout compares in the same order as the UUID values.
#[derive(Default)]
struct UuidStats {
    min: Option<[u8; 16]>,
    max: Option<[u8; 16]>,
}

/// Statistics accumulated on the converted chunk arrays, keyed by quoted dot
/// path. The Vortex footer only stores per-top-level-field stats.
#[derive(Default)]
struct StatsAccumulators {
    /// Leaf-level stats nested below a top-level column (e.g. `"l"."element"`).
    leaf: BTreeMap<String, LeafStatsAccum>,
    /// Per-path extrema for `vortex.uuid` columns at any depth.
    uuid: BTreeMap<String, UuidStats>,
    /// Untruncated bounds for top-level Utf8/Binary columns (e.g. `"s"`).
    varlen_bounds: BTreeMap<String, VarlenBounds>,
}

impl StatsAccumulators {
    /// Fold another accumulator into this one. All tracked stats (bounds and
    /// counts) are order-independent, so partial accumulators computed on
    /// disjoint chunks in parallel merge to the same result.
    fn merge(&mut self, other: StatsAccumulators) {
        for (key, theirs) in other.leaf {
            match self.leaf.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(theirs);
                }
                Entry::Occupied(mut entry) => {
                    let ours = entry.get_mut();
                    ours.stats =
                        std::mem::take(&mut ours.stats).merge_unordered(&theirs.stats, &ours.dtype);
                    ours.num_values += theirs.num_values;
                    ours.extra_nulls += theirs.extra_nulls;
                }
            }
        }
        for (key, theirs) in other.uuid {
            let ours = self.uuid.entry(key).or_default();
            if let Some(min) = theirs.min
                && ours.min.is_none_or(|m| min < m)
            {
                ours.min = Some(min);
            }
            if let Some(max) = theirs.max
                && ours.max.is_none_or(|m| max > m)
            {
                ours.max = Some(max);
            }
        }
        for (key, theirs) in other.varlen_bounds {
            match self.varlen_bounds.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(theirs);
                }
                Entry::Occupied(mut entry) => {
                    let ours = entry.get_mut();
                    ours.stats =
                        std::mem::take(&mut ours.stats).merge_unordered(&theirs.stats, &ours.dtype);
                }
            }
        }
    }
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
    pushed_bytes: AtomicU64,
    stats: Mutex<StatsAccumulators>,
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
    global
        .pushed_bytes
        .fetch_add(array.nbytes(), Ordering::Relaxed);
    let mut sink = global
        .sink
        .as_ref()
        .ok_or_else(|| vortex_err!("sink closed early"))?
        .clone();
    // Park until the channel has room rather than `RUNTIME.block_on`, which would run queued
    // compression tasks on this thread. DuckDB flushes batches from one thread at a time, so
    // stealing work here stalls the whole pipeline; the worker pool drives the writer instead.
    // send may error with "receiver is gone" which isn't the real error
    if futures::executor::block_on(sink.send(Ok(array))).is_ok() {
        return Ok(());
    }
    RUNTIME.block_on(async {
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
    let array = data_chunk_to_vortex(bind_data.fields.names(), chunk)?;
    let mut stats = StatsAccumulators::default();
    accumulate_chunk_leaf_stats(&mut stats, &array)?;
    init_global.stats.lock().merge(stats);
    push_to_writer(init_global, array)
}

/// Accumulate leaf stats for a synthetic single-column chunk: the C++ side
/// evaluates the `variant_to_parquet_variant` transform for a VARIANT column
/// and pushes the resulting parquet-shaped struct here, producing the
/// `"col"."metadata"`/`"col"."value"`/`"col"."typed_value"...` leaf paths
/// DuckLake expects. Physical VARIANT arrays never contribute leaf stats.
pub fn accumulate_stats_chunk(
    global: &CopyFunctionGlobal,
    name: &str,
    chunk: &DataChunkRef,
) -> VortexResult<()> {
    let fields = FieldNames::from([FieldName::from(name)]);
    let array = data_chunk_to_vortex(&fields, chunk)?;
    let mut stats = StatsAccumulators::default();
    accumulate_chunk_leaf_stats(&mut stats, &array)?;
    global.stats.lock().merge(stats);
    Ok(())
}

/// Statistics tracked per leaf path; `column_size_bytes` is attached at
/// finalize from the footer's per-field compressed sizes.
const LEAF_STATS: &[Stat] = &[Stat::Min, Stat::Max, Stat::NullCount, Stat::NaNCount];

/// Accumulate leaf statistics for every nested field of the pushed chunk, matching
/// the leaf paths the parquet writer emits (`"s"."child"`, `"l"."element"`).
fn accumulate_chunk_leaf_stats(acc: &mut StatsAccumulators, chunk: &ArrayRef) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let Canonical::Struct(struct_array) = chunk.clone().execute(&mut ctx)? else {
        vortex_bail!("COPY chunk is not a struct array, got {}", chunk.dtype());
    };
    let mut path = Vec::new();
    for (name, child) in struct_array
        .names()
        .iter()
        .zip(struct_array.iter_unmasked_fields())
    {
        path.push(Field::Name(name.clone()));
        accumulate_leaf_stats(acc, &mut path, child, 0, &mut ctx)?;
        path.pop();
    }
    Ok(())
}

/// Count the NULL leaf slots a list-like node contributes to its element
/// children: each NULL list contributes one null leaf value and each empty
/// (size 0) list contributes one null placeholder, matching parquet
/// definition-level statistics where a row contributes `max(size, 1)` leaf
/// values.
fn list_extra_null_slots(
    array: &ArrayRef,
    sizes: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<u64> {
    let extras = array.invalid_count(ctx)? as u64;
    if extras == array.len() as u64 {
        return Ok(extras);
    }
    let sizes = sizes
        .clone()
        .execute::<PrimitiveArray>(ctx)?
        .reinterpret_cast(sizes.dtype().as_ptype().to_unsigned());
    let validity = array.validity()?.execute_mask(array.len(), ctx)?;
    let empties = match_each_unsigned_integer_ptype!(sizes.ptype(), |S| {
        validity
            .iter()
            .zip(sizes.as_slice::<S>())
            .filter(|(valid, size)| *valid && size.is_zero())
            .count() as u64
    });
    Ok(extras + empties)
}

/// `extra_nulls` carries down the count of enclosing list/array/map rows that
/// contribute a NULL leaf value rather than their elements (see
/// [`list_extra_null_slots`]). Struct nulls need no adjustment since the leaf
/// array retains a (null) slot per parent row.
fn accumulate_leaf_stats(
    acc: &mut StatsAccumulators,
    path: &mut Vec<Field>,
    array: &ArrayRef,
    extra_nulls: u64,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    // UUID storage has no stats kernels; track extrema over its big-endian bytes,
    // which order the same as the UUID values.
    if let DType::Extension(ext) = array.dtype() {
        if ext.is::<Uuid>() {
            accumulate_uuid_stats(
                acc.uuid.entry(quoted_leaf_key(path)).or_default(),
                array,
                ctx,
            )?;
        }
    }
    match array.dtype() {
        DType::Struct(fields, _) => {
            let Canonical::Struct(struct_array) = array.clone().execute(ctx)? else {
                vortex_bail!("struct dtype but non-struct canonical array");
            };
            for (name, child) in fields
                .names()
                .iter()
                .zip(struct_array.iter_unmasked_fields())
            {
                path.push(Field::Name(name.clone()));
                accumulate_leaf_stats(acc, path, child, extra_nulls, ctx)?;
                path.pop();
            }
        }
        DType::List(..) => {
            let Canonical::List(list) = array.clone().execute(ctx)? else {
                vortex_bail!("list dtype but non-list canonical array");
            };
            let child_nulls = extra_nulls + list_extra_null_slots(array, list.sizes(), ctx)?;
            path.push(Field::ElementType);
            accumulate_leaf_stats(acc, path, list.elements(), child_nulls, ctx)?;
            path.pop();
        }
        DType::FixedSizeList(..) => {
            let Canonical::FixedSizeList(list) = array.clone().execute(ctx)? else {
                vortex_bail!("fixed-size list dtype but non-list canonical array");
            };
            // A zero-width fixed-size list is empty for every row.
            let child_nulls = extra_nulls
                + if list.list_size() == 0 {
                    array.len() as u64
                } else {
                    array.invalid_count(ctx)? as u64
                };
            path.push(Field::ElementType);
            accumulate_leaf_stats(acc, path, list.elements(), child_nulls, ctx)?;
            path.pop();
        }
        // Variant columns report stats through the dedicated `metadata` leaf path.
        DType::Extension(ext) if ext.is::<DuckVariant>() => {}
        // A map's leaf stats live on its entry struct fields: `"m"."key"` and
        // `"m"."value"` — the parquet REPEATED `key_value` node has no stats path
        // segment, matching DuckLake's key/value field children.
        DType::Map(..) => {
            let Canonical::Map(map) = array.clone().execute(ctx)? else {
                vortex_bail!("map dtype but non-map canonical array");
            };
            let entries_list = map.entries().as_::<ListView>();
            let child_nulls =
                extra_nulls + list_extra_null_slots(array, entries_list.sizes(), ctx)?;
            let Canonical::Struct(entries) = entries_list.elements().clone().execute(ctx)?
            else {
                vortex_bail!("map entries are not a struct array");
            };
            for (name, child) in entries
                .names()
                .iter()
                .zip(entries.iter_unmasked_fields())
            {
                path.push(Field::Name(name.clone()));
                accumulate_leaf_stats(acc, path, child, child_nulls, ctx)?;
                path.pop();
            }
        }
        DType::Union(..) => {}
        _ => {
            if path.len() < 2 {
                // Top-level Utf8/Binary bounds are truncated in the footer at
                // 64 bytes; accumulate them untruncated so the written stats
                // can use the parquet-compatible 256-byte limit.
                if path.len() == 1
                    && matches!(array.dtype(), DType::Utf8(_) | DType::Binary(_))
                    && let Ok(stats) = array
                        .statistics()
                        .compute_all(&[Stat::Min, Stat::Max], ctx)
                {
                    match acc.varlen_bounds.entry(quoted_leaf_key(path)) {
                        Entry::Vacant(entry) => {
                            entry.insert(VarlenBounds {
                                dtype: array.dtype().clone(),
                                stats,
                            });
                        }
                        Entry::Occupied(mut entry) => {
                            let bounds = entry.get_mut();
                            bounds.stats = std::mem::take(&mut bounds.stats)
                                .merge_unordered(&stats, array.dtype());
                        }
                    }
                }
                return Ok(());
            }
            // Statistics are advisory: leaf types without a compute kernel simply
            // produce no stats rather than failing the write.
            let Ok(stats) = array.statistics().compute_all(LEAF_STATS, ctx) else {
                return Ok(());
            };
            // Chunks are disjoint arrays, so bounds merge by union (min of mins);
            // combine_sets' intersection is for stats of the same array and
            // rejects any two chunks with different bounds.
            match acc.leaf.entry(quoted_leaf_key(path)) {
                Entry::Vacant(entry) => {
                    entry.insert(LeafStatsAccum {
                        path: path.clone(),
                        dtype: array.dtype().clone(),
                        stats,
                        num_values: array.len() as u64 + extra_nulls,
                        extra_nulls,
                    });
                }
                Entry::Occupied(mut entry) => {
                    let accum = entry.get_mut();
                    accum.stats =
                        std::mem::take(&mut accum.stats).merge_unordered(&stats, array.dtype());
                    accum.num_values += array.len() as u64 + extra_nulls;
                    accum.extra_nulls += extra_nulls;
                }
            }
        }
    }
    Ok(())
}

/// Scan a `vortex.uuid` array's storage bytes for row-wise min/max. The 16-byte
/// rows are big-endian with the sign bit flipped, so byte order is UUID order.
fn accumulate_uuid_stats(
    acc: &mut UuidStats,
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    let Canonical::Extension(ext_array) = array.clone().execute(ctx)? else {
        vortex_bail!("uuid dtype but non-extension canonical array");
    };
    let len = ext_array.len();
    let parts = ext_array
        .storage_array()
        .clone()
        .execute::<Canonical>(ctx)?
        .into_fixed_size_list()
        .into_data_parts();
    if len == 0 || parts.validity.definitely_all_null() {
        return Ok(());
    }
    let mask = parts.validity.to_array(len).execute::<Mask>(ctx)?;
    let bytes = parts
        .elements
        .execute::<Canonical>(ctx)?
        .into_primitive()
        .to_buffer::<u8>();
    vortex_ensure!(
        bytes.len() == len * 16,
        "UUID storage has {} bytes, expected {}",
        bytes.len(),
        len * 16
    );
    for row in 0..len {
        if !mask.value(row) {
            continue;
        }
        let mut value = [0u8; 16];
        value.copy_from_slice(&bytes[row * 16..(row + 1) * 16]);
        if acc.min.is_none_or(|min| value < min) {
            acc.min = Some(value);
        }
        if acc.max.is_none_or(|max| value > max) {
            acc.max = Some(value);
        }
    }
    Ok(())
}

/// Quote a leaf path the way DuckLake's `ParseQuotedList` expects: each segment
/// wrapped in double quotes (internal quotes doubled), joined by dots.
fn quoted_leaf_key(path: &[Field]) -> String {
    path.iter()
        .map(|field| {
            let name = match field {
                Field::Name(name) => name.as_ref(),
                Field::ElementType => "element",
            };
            format!("\"{}\"", name.replace('"', "\"\""))
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Leaf stats are computed here during the parallel prepare phase; DuckDB
/// flushes prepared batches one at a time, so flush only merges them.
#[derive(Default)]
pub struct CopyPreparedBatch {
    arrays: Vec<ArrayRef>,
    stats: Mutex<Option<StatsAccumulators>>,
}

pub fn prepare_batch_push(
    bind: &CopyFunctionBind,
    batch: &mut CopyPreparedBatch,
    chunk: &DataChunkRef,
) -> VortexResult<()> {
    let array = data_chunk_to_vortex(bind.fields.names(), chunk)?;
    accumulate_chunk_leaf_stats(batch.stats.get_mut().get_or_insert_default(), &array)?;
    batch.arrays.push(array);
    Ok(())
}

pub fn flush_batch(global: &CopyFunctionGlobal, batch: &CopyPreparedBatch) -> VortexResult<()> {
    if let Some(stats) = batch.stats.lock().take() {
        global.stats.lock().merge(stats);
    }
    for array in &batch.arrays {
        push_to_writer(global, array.clone())?;
    }
    Ok(())
}

/// Uncompressed bytes pushed to the writer so far. DuckLake compares this against
/// `target_file_size` to decide file rotation; compression means it is an upper bound.
pub fn file_size_bytes(global: &CopyFunctionGlobal) -> u64 {
    if let Some(finished) = global.finished.lock().as_ref() {
        return finished.summary.size();
    }
    global.pushed_bytes.load(Ordering::Relaxed)
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
        let field_sizes = summary.footer().compressed_field_sizes()?;
        *init_global.finished.lock() = Some(FinishedWrite {
            summary,
            column_sizes,
            field_sizes,
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
    pub row_group_count: u64,
}

/// Per-column statistics of the written Vortex file. `min`/`max` are DuckDB values converted from
/// the Vortex scalar; every field is optional and omitted when the statistic is not available.
pub(crate) struct WrittenColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    /// Whether `min`/`max` are untruncated bounds. Truncated string bounds are
    /// still valid bounds but must be marked inexact so DuckLake cannot fold
    /// aggregates from them (see `StringStatsType::TRUNCATED_STATS`).
    pub min_is_exact: bool,
    pub max_is_exact: bool,
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
    let mut stats =
        column_stats_from_summary(&finished.summary, column_index, &finished.column_sizes)?;
    let acc = global.stats.lock();
    let Some(key) = top_level_stats_key(&finished.summary, column_index) else {
        return Ok(Some(stats));
    };
    if let Some(uuid) = acc.uuid.get(&key) {
        apply_uuid_stats(&mut stats, uuid);
    }
    if let Some(bounds) = acc.varlen_bounds.get(&key) {
        // The footer's varlen bounds were truncated at 64 bytes; re-truncate the
        // accumulated untruncated bounds at the DuckLake 256-byte limit.
        let (min, min_is_exact) =
            stats_bound_to_duckdb(bounds.stats.get(Stat::Min), Stat::Min, &bounds.dtype)?;
        let (max, max_is_exact) =
            stats_bound_to_duckdb(bounds.stats.get(Stat::Max), Stat::Max, &bounds.dtype)?;
        stats.min = min;
        stats.min_is_exact = min_is_exact;
        stats.max = max;
        stats.max_is_exact = max_is_exact;
    }
    Ok(Some(stats))
}

/// The leaf-stats key of a top-level column (`"col"`), if `column_index` names a
/// struct field of the written file's dtype.
fn top_level_stats_key(summary: &WriteSummary, column_index: usize) -> Option<String> {
    let DType::Struct(fields, _) = summary.footer().dtype() else {
        return None;
    };
    let name = fields.field_name(column_index)?;
    Some(quoted_leaf_key(&[Field::Name(name.clone())]))
}

/// Fill in min/max the footer cannot compute for `vortex.uuid` columns, formatted
/// the way the parquet writer reports UUID stats.
fn apply_uuid_stats(stats: &mut WrittenColumnStats, uuid: &UuidStats) {
    stats.min = uuid
        .min
        .map(|bytes| Value::from(uuid_bytes_string(&bytes).as_str()));
    stats.max = uuid
        .max
        .map(|bytes| Value::from(uuid_bytes_string(&bytes).as_str()));
}

/// Statistics for one nested leaf path (e.g. `"l"."element"`), mirroring the leaf
/// entries the parquet writer reports for nested columns.
pub(crate) struct WrittenLeafStats {
    /// Quoted dot path, e.g. `"l"."element"`.
    pub path: String,
    pub stats: WrittenColumnStats,
}

/// Number of leaf paths with accumulated statistics. `0` before finalize.
pub(crate) fn written_leaf_stats_count(global: &CopyFunctionGlobal) -> u64 {
    if global.finished.lock().is_none() {
        return 0;
    }
    global.stats.lock().leaf.len() as u64
}

/// Read statistics for the leaf at `index` (BTreeMap order = sorted by quoted path).
/// `Ok(None)` before finalize or if `index` is out of range.
pub(crate) fn written_leaf_stats(
    global: &CopyFunctionGlobal,
    index: usize,
) -> VortexResult<Option<WrittenLeafStats>> {
    let guard = global.finished.lock();
    let Some(finished) = guard.as_ref() else {
        return Ok(None);
    };
    let acc = global.stats.lock();
    let Some((key, leaf)) = acc.leaf.iter().nth(index) else {
        return Ok(None);
    };
    let column_size_bytes = finished
        .field_sizes
        .get(&FieldPath::from(leaf.path.clone()));
    let (min, min_is_exact) =
        stats_bound_to_duckdb(leaf.stats.get(Stat::Min), Stat::Min, &leaf.dtype)?;
    let (max, max_is_exact) =
        stats_bound_to_duckdb(leaf.stats.get(Stat::Max), Stat::Max, &leaf.dtype)?;
    let mut stats = WrittenColumnStats {
        min,
        max,
        min_is_exact,
        max_is_exact,
        null_count: exact_u64(leaf.stats.get(Stat::NullCount))
            .map(|count| count + leaf.extra_nulls),
        has_nan: exact_u64(leaf.stats.get(Stat::NaNCount)).map(|count| count > 0),
        num_values: leaf.num_values,
        column_size_bytes,
    };
    if let Some(uuid) = acc.uuid.get(key) {
        apply_uuid_stats(&mut stats, uuid);
    }
    Ok(Some(WrittenLeafStats {
        path: key.clone(),
        stats,
    }))
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
        // Zones are the Vortex analog of parquet row groups; each column is a zoned
        // layout whose zone-map child holds one row per zone.
        row_group_count: file_zone_count(summary.footer().layout()),
    }
}

fn file_zone_count(layout: &vortex::layout::LayoutRef) -> u64 {
    // The root is a struct layout whose field slots start at 1 (slot 0 is validity).
    let Ok(Some(field)) = layout.slot(1) else {
        return 0;
    };
    if let Some(zoned) = field.as_opt::<vortex::layout::layouts::zoned::Zoned>() {
        zoned.nzones() as u64
    } else {
        // Unzoned fallback: count the column's chunk children.
        field.nchildren() as u64
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

    let (min, min_is_exact) = stats_bound_to_duckdb(stats.get(Stat::Min), Stat::Min, dtype)?;
    let (max, max_is_exact) = stats_bound_to_duckdb(stats.get(Stat::Max), Stat::Max, dtype)?;

    Ok(WrittenColumnStats {
        min,
        max,
        min_is_exact,
        max_is_exact,
        null_count: exact_u64(stats.get(Stat::NullCount)),
        // NaNCount is exact only for float columns, so this is emitted just for them (as in parquet).
        has_nan: exact_u64(stats.get(Stat::NaNCount)).map(|count| count > 0),
        num_values: summary.row_count(),
        // On-disk compressed size; excludes bytes not attributable to a column (e.g. struct validity).
        column_size_bytes: column_sizes.get(column_index).copied(),
    })
}

/// Byte limit for Utf8/Binary min/max reported to DuckLake, matching the parquet
/// writer's `MAX_STRING_STATISTICS_SIZE`.
const STATS_BOUND_MAX_BYTES: usize = 256;

/// Convert a Min/Max statistic to a DuckDB value plus its exactness. Bounds that
/// are only `Precision::Inexact` are still reported (as inexact) rather than
/// dropped, and Utf8/Binary bounds longer than `STATS_BOUND_MAX_BYTES` are
/// truncated to a still-valid inexact bound, like the parquet writer.
fn stats_bound_to_duckdb(
    stat: Precision<ScalarValue>,
    stat_kind: Stat,
    dtype: &DType,
) -> VortexResult<(Option<Value>, bool)> {
    let mut is_exact = stat.is_exact();
    let Some(value) = stat.into_inner() else {
        return Ok((None, true));
    };
    let scalar = Scalar::try_new(dtype.clone(), Some(value))?;
    let nullability = dtype.nullability();
    let bound = match (stat_kind, dtype) {
        (Stat::Min, DType::Utf8(_)) => lower_bound(
            BufferString::from_scalar(scalar)?,
            STATS_BOUND_MAX_BYTES,
            nullability,
        ),
        (Stat::Min, DType::Binary(_)) => lower_bound(
            ByteBuffer::from_scalar(scalar)?,
            STATS_BOUND_MAX_BYTES,
            nullability,
        ),
        (Stat::Max, DType::Utf8(_)) => upper_bound(
            BufferString::from_scalar(scalar)?,
            STATS_BOUND_MAX_BYTES,
            nullability,
        ),
        (Stat::Max, DType::Binary(_)) => upper_bound(
            ByteBuffer::from_scalar(scalar)?,
            STATS_BOUND_MAX_BYTES,
            nullability,
        ),
        _ => Some((scalar, false)),
    };
    // A missing bound (e.g. an upper bound that cannot be incremented) drops the stat.
    let Some((bound, was_truncated)) = bound else {
        return Ok((None, false));
    };
    is_exact &= !was_truncated;
    Ok((Some(bound.try_to_duckdb_scalar()?), is_exact))
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
        pushed_bytes: AtomicU64::new(0),
        stats: Mutex::new(StatsAccumulators::default()),
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
