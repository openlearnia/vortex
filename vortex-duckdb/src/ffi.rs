// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ffi::CStr;
use std::ffi::c_char;
use std::ffi::c_void;
use std::ptr;

use num_traits::AsPrimitive;
use vortex::error::VortexExpect;
use vortex::error::vortex_err;
use vortex::file::Footer;

use crate::convert::can_push_expression;
use crate::copy::CopyFunctionBind;
use crate::copy::CopyFunctionGlobal;
use crate::copy::CopyPreparedBatch;
use crate::copy::copy_to_bind;
use crate::copy::copy_to_finalize;
use crate::copy::copy_to_initialize_global;
use crate::copy::copy_to_sink;
use crate::copy::read_ducklake_field_ids_metadata;
use crate::copy::flush_batch;
use crate::copy::prepare_batch_push;
use crate::copy::written_column_stats;
use crate::copy::written_file_stats;
use crate::cpp;
use crate::duckdb::AggregatePushdownInput;
use crate::duckdb::BindResult;
use crate::duckdb::Data;
use crate::duckdb::DataChunk;
use crate::duckdb::DuckdbStringMap;
use crate::duckdb::Expression;
use crate::duckdb::LogicalType;
use crate::duckdb::LogicalTypeRef;
use crate::duckdb::TableInitInput;
use crate::duckdb::try_or;
use crate::duckdb::try_or_null;
use crate::file_reader::OpenFileReader;
use crate::file_reader::can_get_partition_stats;
use crate::file_reader::footer_get_cached;
use crate::file_reader::footer_get_statistics;
use crate::file_reader::reader_bind;
use crate::file_reader::reader_get_progress_in_file;
use crate::file_reader::reader_get_statistics;
use crate::file_reader::reader_initialize;
use crate::file_reader::reader_open;
use crate::file_reader::reader_open_with_key;
use crate::file_reader::reader_scan;
use crate::file_reader::reader_try_initialize_scan;
use crate::table_function::BindState;
use crate::table_function::Cardinality;
use crate::table_function::GlobalState;
use crate::table_function::LocalState;
use crate::table_function::cardinality;
use crate::table_function::finalize_scan;
use crate::table_function::finish_reading;
use crate::table_function::init_global;
use crate::table_function::init_local;
use crate::table_function::pushdown_complex_filter;
use crate::table_function::pushdown_projection_aggregates;
use crate::table_function::pushdown_projection_expression;
use crate::table_function::to_string;

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_to_string(
    bind: *const c_void,
    map: cpp::duckdb_vx_string_map,
) {
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    let map = unsafe { DuckdbStringMap::borrow_mut(map) };
    to_string(bind, map);
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_pushdown_complex_filter(
    bind: *mut c_void,
    expr: cpp::duckdb_vx_expr,
    error: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind = unsafe { bind.cast::<BindState>().as_mut() }.vortex_expect("null pointer");
    let expr = unsafe { Expression::borrow(expr) };
    try_or(error, || pushdown_complex_filter(bind, expr))
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_pushdown_projection_expression(
    bind: *mut c_void,
    expr: cpp::duckdb_vx_expr,
    column_id: usize,
    error: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind = unsafe { bind.cast::<BindState>().as_mut() }.vortex_expect("null pointer");
    let expr = unsafe { Expression::borrow(expr) };
    try_or(error, || {
        pushdown_projection_expression(bind, expr, column_id)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_pushdown_projection_aggregates(
    bind: *mut c_void,
    input: cpp::duckdb_vx_agg_input,
    error: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind = unsafe { bind.cast::<BindState>().as_mut() }.vortex_expect("null pointer");
    let input = unsafe { AggregatePushdownInput::borrow(input) };
    try_or(error, || pushdown_projection_aggregates(bind, input))
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_pushdown_expression(
    expr: cpp::duckdb_vx_expr,
) -> bool {
    can_push_expression(unsafe { Expression::borrow(expr) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_cardinality(
    bind: *const c_void,
    file_count: u64,
    stats: *mut cpp::duckdb_vx_node_statistics,
) {
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    let stats = unsafe { stats.as_mut() }.vortex_expect("null pointer");

    match cardinality(bind, file_count) {
        Cardinality::Exact(c) => {
            stats.has_estimated_cardinality = true;
            stats.estimated_cardinality = c as _;
            stats.has_max_cardinality = true;
            stats.max_cardinality = c as _;
        }
        Cardinality::Estimate(c) => {
            stats.has_estimated_cardinality = true;
            stats.estimated_cardinality = c as _;
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_init_global(
    init_input: *const cpp::duckdb_vx_tfunc_init_input,
    error: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let init_input =
        TableInitInput::new(unsafe { init_input.as_ref() }.vortex_expect("null pointer"));

    match init_global(&init_input) {
        Ok(init_data) => Data::from(Box::new(init_data)).as_ptr(),
        Err(e) => {
            // Set the error in the error output.
            let msg = e.to_string();
            unsafe { error.write(cpp::duckdb_vx_error_create(msg.as_ptr().cast(), msg.len())) };
            ptr::null_mut::<cpp::duckdb_vx_data_>().cast()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_init_local(
    bind: *const c_void,
    global: *const c_void,
) -> cpp::duckdb_vx_data {
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    let global = unsafe { global.cast::<GlobalState>().as_ref() }.vortex_expect("null pointer");
    let local = init_local(bind, global);
    Data::from(Box::new(local)).as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_bind(
    first_file: *const c_void,
    result: cpp::duckdb_bind_result,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let first_file =
        unsafe { first_file.cast::<OpenFileReader>().as_ref() }.vortex_expect("null pointer");
    let mut result = unsafe { BindResult::own(result) };

    try_or_null(error_out, || {
        let bind_data = reader_bind(first_file, &mut result)?;
        Ok(Data::from(Box::new(bind_data)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_open(
    file_path: *const c_char,
    file_path_len: usize,
    error: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let path = unsafe { std::slice::from_raw_parts(file_path.cast::<u8>(), file_path_len) };

    try_or_null(error, || {
        let path = str::from_utf8(path).map_err(|_| vortex_err!("invalid utf-8"))?;
        let file = reader_open(path)?;
        Ok(Data::from(Box::new(file)).as_ptr())
    })
}

/// Open a Vortex file with an optional raw AES-GCM segment key (DuckLake
/// `encryption_key` OpenFileInfo option / `encryption_config` COPY option).
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_open_with_key(
    file_path: *const c_char,
    file_path_len: usize,
    encryption_key_bytes: *const u8,
    encryption_key_len: usize,
    error: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let path = unsafe { std::slice::from_raw_parts(file_path.cast::<u8>(), file_path_len) };

    try_or_null(error, || {
        let path = str::from_utf8(path).map_err(|_| vortex_err!("invalid utf-8"))?;
        let encryption_key = if encryption_key_bytes.is_null() || encryption_key_len == 0 {
            None
        } else {
            Some(
                unsafe { std::slice::from_raw_parts(encryption_key_bytes, encryption_key_len) }.to_vec(),
            )
        };
        let file = reader_open_with_key(path, encryption_key)?;
        Ok(Data::from(Box::new(file)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_get_statistics(
    file: *const c_void,
    bind: *const c_void,
    column_name: *const c_char,
    column_name_len: usize,
    stats_out: *mut cpp::duckdb_column_statistics,
) -> bool {
    let file = unsafe { file.cast::<OpenFileReader>().as_ref() }.vortex_expect("null pointer");
    let name_bytes =
        unsafe { std::slice::from_raw_parts(column_name.cast::<u8>(), column_name_len) };
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    let name = String::from_utf8_lossy(name_bytes);

    let Some(stats) = reader_get_statistics(file, bind, &name) else {
        return false;
    };
    let stats_out = unsafe { &mut *stats_out };
    stats_out.min = stats.min.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max = stats.max.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max_string_length = stats.max_string_length;
    stats_out.has_null = stats.has_null;
    stats_out.type_ = stats.logical_type.into_ptr();
    true
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_can_get_partition_stats(
    bind: *const c_void,
) -> bool {
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    can_get_partition_stats(bind)
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_footer_get_cached(
    bind: *mut c_void,
    path: *const c_char,
    len: usize,
    row_count_out: *mut u64,
    error: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let bind = unsafe { bind.cast::<BindState>().as_mut() }.vortex_expect("null pointer");
    let path = unsafe { std::slice::from_raw_parts(path.cast::<u8>(), len) };
    try_or_null(error, || {
        let path = str::from_utf8(path).map_err(|_| vortex_err!("invalid utf-8"))?;
        Ok(match footer_get_cached(bind, path)? {
            Some(footer) => {
                unsafe { *row_count_out = footer.row_count() };
                Data::from(Box::new(footer)).as_ptr()
            }
            None => ptr::null_mut(),
        })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_footer_get_statistics(
    footer: *const c_void,
    column_index: usize,
    stats_out: *mut cpp::duckdb_column_statistics,
) -> bool {
    let footer = unsafe { footer.cast::<Footer>().as_ref() }.vortex_expect("null pointer");
    let Some(stats) = footer_get_statistics(footer, column_index) else {
        return false;
    };
    let stats_out = unsafe { &mut *stats_out };
    stats_out.min = stats.min.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max = stats.max.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max_string_length = stats.max_string_length;
    stats_out.has_null = stats.has_null;
    stats_out.type_ = stats.logical_type.into_ptr();
    true
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_initialize(
    global: *const c_void,
    file: *mut c_void,
    error: *mut cpp::duckdb_vx_error,
) -> bool {
    let global = unsafe { global.cast::<GlobalState>().as_ref() }.vortex_expect("null pointer");
    let file = unsafe { file.cast::<OpenFileReader>().as_mut() }.vortex_expect("null pointer");
    try_or(error, || reader_initialize(file, global))
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_bind_column_type(
    bind: *const c_void,
    index: usize,
) -> cpp::duckdb_logical_type {
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    bind.columns[index].logical_type.as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_is_aggregate(bind: *const c_void) -> bool {
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    !bind.aggregates.is_empty()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_try_initialize_scan(
    local: *mut c_void,
    file: *mut c_void,
) -> bool {
    let file = unsafe { file.cast::<OpenFileReader>().as_mut() }.vortex_expect("null pointer");
    let local = unsafe { local.cast::<LocalState>().as_mut() }.vortex_expect("null pointer");
    reader_try_initialize_scan(file, local)
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_scan(
    file: *const c_void,
    global: *const c_void,
    local: *mut c_void,
    chunk: cpp::duckdb_data_chunk,
    error: *mut cpp::duckdb_vx_error,
) -> bool {
    let file = unsafe { file.cast::<OpenFileReader>().as_ref() }.vortex_expect("null pointer");
    let global = unsafe { global.cast::<GlobalState>().as_ref() }.vortex_expect("null pointer");
    let local = unsafe { local.cast::<LocalState>().as_mut() }.vortex_expect("null pointer");
    let chunk = unsafe { DataChunk::borrow_mut(chunk) };
    try_or(error, || reader_scan(file, global, local, chunk))
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_get_progress_in_file(file: *const c_void) -> f64 {
    let file = unsafe { file.cast::<OpenFileReader>().as_ref() }.vortex_expect("null pointer");
    reader_get_progress_in_file(file)
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_finalize_scan(
    global: *const c_void,
    chunk: cpp::duckdb_data_chunk,
    error: *mut cpp::duckdb_vx_error,
) -> bool {
    let global = unsafe { global.cast::<GlobalState>().as_ref() }.vortex_expect("null pointer");
    let chunk = unsafe { DataChunk::borrow_mut(chunk) };
    try_or(error, || finalize_scan(global, chunk))
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_reader_finish_reading(
    global: *const c_void,
    local: *mut c_void,
) {
    let global = unsafe { global.cast::<GlobalState>().as_ref() }.vortex_expect("null pointer");
    let local = unsafe { local.cast::<LocalState>().as_mut() }.vortex_expect("null pointer");
    finish_reading(global, local);
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_bind_data_clone(
    bind: *const c_void,
) -> cpp::duckdb_vx_data {
    let bind = unsafe { bind.cast::<BindState>().as_ref() }.vortex_expect("null pointer");
    let copied_data = bind.clone();
    Data::from(Box::new(copied_data)).as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_bind(
    column_names: *const *const c_char,
    column_name_count: usize,
    column_types: *const cpp::duckdb_logical_type,
    column_type_count: usize,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let column_names: Vec<String> =
        unsafe { std::slice::from_raw_parts(column_names, column_name_count.as_()) }
            .iter()
            .map(|name| {
                unsafe { CStr::from_ptr(name.cast()) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();

    let column_types: Vec<&LogicalTypeRef> =
        unsafe { std::slice::from_raw_parts(column_types, column_type_count.as_()) }
            .iter()
            .map(|c| unsafe { LogicalType::borrow(*c) })
            .collect();

    try_or_null(error_out, || {
        let bind_data = copy_to_bind(&column_names, &column_types)?;
        Ok(Data::from(Box::new(bind_data)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_initialize_global(
    bind_data: *const c_void,
    file_path: *const c_char,
    field_ids_bytes: *const u8,
    field_ids_len: usize,
    encryption_key_bytes: *const u8,
    encryption_key_len: usize,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let file_path = unsafe { CStr::from_ptr(file_path) }
        .to_string_lossy()
        .into_owned();
    let bind_data = unsafe { bind_data.cast::<CopyFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let field_ids_metadata = if field_ids_bytes.is_null() || field_ids_len == 0 {
        None
    } else {
        Some(unsafe { std::slice::from_raw_parts(field_ids_bytes, field_ids_len) }.to_vec())
    };
    let encryption_key = if encryption_key_bytes.is_null() || encryption_key_len == 0 {
        None
    } else {
        Some(
            unsafe { std::slice::from_raw_parts(encryption_key_bytes, encryption_key_len) }
                .to_vec(),
        )
    };
    try_or_null(error_out, || {
        let bind_data =
            copy_to_initialize_global(bind_data, file_path, field_ids_metadata, encryption_key)?;
        Ok(Data::from(Box::new(bind_data)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_sink(
    bind_data: *const c_void,
    global_data: *const c_void,
    data_chunk: cpp::duckdb_data_chunk,
    error_out: *mut cpp::duckdb_vx_error,
) {
    let bind_data = unsafe { bind_data.cast::<CopyFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let global_data = unsafe { global_data.cast::<CopyFunctionGlobal>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let data_chunk = unsafe { DataChunk::borrow_mut(data_chunk) };
    try_or(error_out, || {
        copy_to_sink(bind_data, global_data, data_chunk)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_finalize(
    global_data: *mut c_void,
    error_out: *mut cpp::duckdb_vx_error,
) {
    let global_data = unsafe { global_data.cast::<CopyFunctionGlobal>().as_mut() }
        .vortex_expect("bind_data null pointer");
    try_or(error_out, || copy_to_finalize(global_data))
}

#[repr(C)]
pub struct duckdb_vx_schema_node {
    pub name: *const c_char,
    pub name_len: usize,
    pub duckdb_type: *const c_char,
    pub duckdb_type_len: usize,
    pub num_children: u64,
}

#[repr(C)]
pub struct duckdb_vx_column_stat {
    pub column_id: u64,
    pub stats_min: *const c_char,
    pub stats_min_len: usize,
    pub has_stats_min: bool,
    pub stats_max: *const c_char,
    pub stats_max_len: usize,
    pub has_stats_max: bool,
    pub stats_null_count: u64,
    pub has_null_count: bool,
    pub stats_num_values: u64,
    pub has_num_values: bool,
    pub total_compressed_size: u64,
    pub has_compressed_size: bool,
    pub contains_nan: bool,
    pub has_contains_nan: bool,
}

/// Opens Vortex footer metadata for one file. Caller owns the returned `duckdb_vx_data`.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_full_metadata_open(
    file_path: *const c_char,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let file_path = unsafe { CStr::from_ptr(file_path) }.to_string_lossy();
    match crate::full_metadata::open_full_metadata(file_path.as_ref()) {
        Ok(meta) => Data::from(Box::new(meta)).as_ptr(),
        Err(e) => {
            if !error_out.is_null() {
                let msg = e.to_string();
                unsafe {
                    error_out.write(cpp::duckdb_vx_error_create(msg.as_ptr().cast(), msg.len()));
                }
            }
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_full_metadata_row_count(meta: *const c_void) -> u64 {
    let meta = unsafe { &*(meta as *const crate::full_metadata::FullMetadata) };
    meta.num_rows
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_full_metadata_file_size(meta: *const c_void) -> u64 {
    let meta = unsafe { &*(meta as *const crate::full_metadata::FullMetadata) };
    meta.file_size_bytes
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_full_metadata_schema_count(
    meta: *const c_void,
) -> usize {
    let meta = unsafe { &*(meta as *const crate::full_metadata::FullMetadata) };
    meta.schema.len()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_full_metadata_schema_at(
    meta: *const c_void,
    index: usize,
    out: *mut duckdb_vx_schema_node,
) -> bool {
    let meta = unsafe { &*(meta as *const crate::full_metadata::FullMetadata) };
    let Some(node) = meta.schema.get(index) else {
        return false;
    };
    unsafe {
        *out = duckdb_vx_schema_node {
            name: node.name.as_ptr().cast(),
            name_len: node.name.len(),
            duckdb_type: node.duckdb_type.as_ptr().cast(),
            duckdb_type_len: node.duckdb_type.len(),
            num_children: node.num_children,
        };
    }
    true
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_full_metadata_stats_count(
    meta: *const c_void,
) -> usize {
    let meta = unsafe { &*(meta as *const crate::full_metadata::FullMetadata) };
    meta.stats.len()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_full_metadata_stat_at(
    meta: *const c_void,
    index: usize,
    out: *mut duckdb_vx_column_stat,
) -> bool {
    let meta = unsafe { &*(meta as *const crate::full_metadata::FullMetadata) };
    let Some(stat) = meta.stats.get(index) else {
        return false;
    };
    unsafe {
        *out = duckdb_vx_column_stat {
            column_id: stat.column_id,
            stats_min: stat
                .stats_min
                .as_ref()
                .map(|s| s.as_ptr().cast())
                .unwrap_or(ptr::null()),
            stats_min_len: stat.stats_min.as_ref().map(|s| s.len()).unwrap_or(0),
            has_stats_min: stat.stats_min.is_some(),
            stats_max: stat
                .stats_max
                .as_ref()
                .map(|s| s.as_ptr().cast())
                .unwrap_or(ptr::null()),
            stats_max_len: stat.stats_max.as_ref().map(|s| s.len()).unwrap_or(0),
            has_stats_max: stat.stats_max.is_some(),
            stats_null_count: stat.stats_null_count.unwrap_or(0),
            has_null_count: stat.stats_null_count.is_some(),
            stats_num_values: stat.stats_num_values.unwrap_or(0),
            has_num_values: stat.stats_num_values.is_some(),
            total_compressed_size: stat.total_compressed_size.unwrap_or(0),
            has_compressed_size: stat.total_compressed_size.is_some(),
            contains_nan: stat.contains_nan.unwrap_or(false),
            has_contains_nan: stat.contains_nan.is_some(),
        };
    }
    true
}

/// Reads the `ducklake.field_ids` metadata segment from a Vortex file.
/// Returns a malloc'd buffer (caller frees with `free`) or null when absent.
/// On I/O/parse error, sets `error_out` and returns null.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_vortex_read_ducklake_field_ids(
    file_path: *const c_char,
    len_out: *mut usize,
    error_out: *mut cpp::duckdb_vx_error,
) -> *mut u8 {
    if !len_out.is_null() {
        unsafe { *len_out = 0 };
    }
    let file_path = unsafe { CStr::from_ptr(file_path) }.to_string_lossy();
    let resolved = match read_ducklake_field_ids_metadata(file_path.as_ref()) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                let msg = e.to_string();
                unsafe {
                    error_out.write(cpp::duckdb_vx_error_create(msg.as_ptr().cast(), msg.len()));
                }
            }
            return ptr::null_mut();
        }
    };
    let Some(bytes) = resolved else {
        return ptr::null_mut();
    };
    if bytes.is_empty() {
        return ptr::null_mut();
    }
    // Caller frees with free(); match C allocator.
    unsafe extern "C" {
        fn malloc(size: usize) -> *mut c_void;
    }
    let buf = unsafe { malloc(bytes.len()) as *mut u8 };
    if buf.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), buf, bytes.len());
        if !len_out.is_null() {
            *len_out = bytes.len();
        }
    }
    buf
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_prepare_batch_new() -> cpp::duckdb_vx_data {
    Data::from(Box::new(CopyPreparedBatch::default())).as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_prepare_batch_push(
    bind: *const c_void,
    batch: *mut c_void,
    chunk: cpp::duckdb_data_chunk,
    error: *mut cpp::duckdb_vx_error,
) {
    let bind = unsafe { bind.cast::<CopyFunctionBind>().as_ref() }.vortex_expect("null pointer");
    let batch = unsafe { batch.cast::<CopyPreparedBatch>().as_mut() }.vortex_expect("null pointer");
    let chunk = unsafe { DataChunk::borrow_mut(chunk) };
    try_or(error, || prepare_batch_push(bind, batch, chunk))
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_flush_batch(
    global: *const c_void,
    batch: *const c_void,
    error: *mut cpp::duckdb_vx_error,
) {
    let global =
        unsafe { global.cast::<CopyFunctionGlobal>().as_ref() }.vortex_expect("null pointer");
    let batch = unsafe { batch.cast::<CopyPreparedBatch>().as_ref() }.vortex_expect("null pointer");
    try_or(error, || flush_batch(global, batch))
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_get_written_file_statistics(
    global_data: *const c_void,
    out: *mut cpp::duckdb_vx_written_file_statistics,
) -> bool {
    let global_data = unsafe { global_data.cast::<CopyFunctionGlobal>().as_ref() }
        .vortex_expect("global_data null pointer");
    let Some(stats) = written_file_stats(global_data) else {
        return false;
    };
    let out = unsafe { &mut *out };
    out.row_count = stats.row_count;
    out.file_size_bytes = stats.file_size_bytes;
    out.footer_size_bytes = stats.footer_size_bytes;
    out.num_columns = stats.num_columns as u64;
    true
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_get_written_column_statistics(
    global_data: *const c_void,
    column_index: usize,
    out: *mut cpp::duckdb_vx_written_column_statistics,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    let global_data = unsafe { global_data.cast::<CopyFunctionGlobal>().as_ref() }
        .vortex_expect("global_data null pointer");
    try_or(error_out, || {
        let Some(stats) = written_column_stats(global_data, column_index)? else {
            return Ok(false);
        };
        let out = unsafe { &mut *out };
        out.min = stats.min.map_or(ptr::null_mut(), |v| v.into_ptr());
        out.max = stats.max.map_or(ptr::null_mut(), |v| v.into_ptr());
        out.has_null_count = stats.null_count.is_some();
        out.null_count = stats.null_count.unwrap_or(0);
        out.num_values = stats.num_values;
        out.has_column_size = stats.column_size_bytes.is_some();
        out.column_size_bytes = stats.column_size_bytes.unwrap_or(0);
        out.has_nan_stat = stats.has_nan.is_some();
        out.contains_nan = stats.has_nan.unwrap_or(false);
        Ok(true)
    })
}
