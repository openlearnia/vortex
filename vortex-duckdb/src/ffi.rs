// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ffi::CStr;
use std::ffi::c_char;
use std::ffi::c_void;
use std::ptr;

use num_traits::AsPrimitive;
use vortex::error::VortexExpect;

use crate::convert::can_push_expression;
use crate::copy::CopyFunctionBind;
use crate::copy::CopyFunctionGlobal;
use crate::copy::copy_to_bind;
use crate::copy::copy_to_finalize;
use crate::copy::copy_to_initialize_global;
use crate::copy::copy_to_sink;
use crate::copy::read_ducklake_field_ids_metadata;
use crate::cpp;
use crate::duckdb::AggregatePushdownInput;
use crate::duckdb::BindInput;
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
use crate::table_function::Cardinality;
use crate::table_function::TableFunctionBind;
use crate::table_function::TableFunctionGlobal;
use crate::table_function::TableFunctionLocal;
use crate::table_function::bind;
use crate::table_function::cardinality;
use crate::table_function::get_partition_data;
use crate::table_function::init_global;
use crate::table_function::init_local;
use crate::table_function::pushdown_complex_filter;
use crate::table_function::pushdown_projection_aggregates;
use crate::table_function::pushdown_projection_expression;
use crate::table_function::scan;
use crate::table_function::statistics;
use crate::table_function::table_scan_progress;
use crate::table_function::to_string;

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_to_string(
    bind_data: *const c_void,
    map: cpp::duckdb_vx_string_map,
) {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let map = unsafe { DuckdbStringMap::borrow_mut(map) };
    to_string(bind_data, map);
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_statistics(
    bind_data: *const c_void,
    column_index: usize,
    stats_out: *mut cpp::duckdb_column_statistics,
) -> bool {
    let stats_out = unsafe { &mut *stats_out };
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let Some(stats) = statistics(bind_data, column_index) else {
        return false;
    };
    stats_out.min = stats.min.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max = stats.max.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max_string_length = stats.max_string_length;
    stats_out.has_null = stats.has_null;
    true
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_scan_progress(global_state: *mut c_void) -> f64 {
    let global_state = unsafe { global_state.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");
    table_scan_progress(global_state)
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_get_partition_data(
    global_init_data: *mut c_void,
    local_init_data: *mut c_void,
    partition_data_out: *mut cpp::duckdb_vx_partition_data,
) {
    let global_init_data = unsafe { global_init_data.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");
    let local_init_data = unsafe { local_init_data.cast::<TableFunctionLocal>().as_mut() }
        .vortex_expect("local_init_data null pointer");
    let data = get_partition_data(global_init_data, local_init_data);
    let out = unsafe { &mut *partition_data_out };

    out.partition_index = data.partition_index;
    out.file_index_column_pos = data.file_index_column_pos.unwrap_or(usize::MAX);
    out.file_index = data.file_index;
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_pushdown_complex_filter(
    bind_data: *mut c_void,
    expr: cpp::duckdb_vx_expr,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_mut() }
        .vortex_expect("bind_data null pointer");
    let expr = unsafe { Expression::borrow(expr) };
    try_or(error_out, || pushdown_complex_filter(bind_data, expr))
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_pushdown_projection_expression(
    bind_data: *mut c_void,
    expr: cpp::duckdb_vx_expr,
    column_id: usize,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_mut() }
        .vortex_expect("bind_data null pointer");
    let expr = unsafe { Expression::borrow(expr) };
    try_or(error_out, || {
        pushdown_projection_expression(bind_data, expr, column_id)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_pushdown_projection_aggregates(
    bind_data: *mut c_void,
    input: cpp::duckdb_vx_agg_input,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_mut() }
        .vortex_expect("bind_data null pointer");
    let input = unsafe { AggregatePushdownInput::borrow(input) };
    try_or(error_out, || {
        pushdown_projection_aggregates(bind_data, input)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_scan(
    global_init_data: *mut c_void,
    local_init_data: *mut c_void,
    output: cpp::duckdb_data_chunk,
    error_out: *mut cpp::duckdb_vx_error,
) {
    let global_init_data = unsafe { global_init_data.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");
    let local_init_data = unsafe { local_init_data.cast::<TableFunctionLocal>().as_mut() }
        .vortex_expect("local_init_data null pointer");
    let data_chunk = unsafe { DataChunk::borrow_mut(output) };

    match scan(local_init_data, global_init_data, data_chunk) {
        Ok(()) => {
            // The data chunk is already filled by the function.
            // No need to do anything here.
        }
        Err(e) => unsafe {
            error_out.write(cpp::duckdb_vx_error_create(
                e.to_string().as_ptr().cast(),
                e.to_string().len(),
            ));
        },
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_pushdown_expression(
    expr: cpp::duckdb_vx_expr,
) -> bool {
    can_push_expression(unsafe { Expression::borrow(expr) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_cardinality(
    bind_data: *const c_void,
    node_stats_out: *mut cpp::duckdb_vx_node_statistics,
) {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let node_stats =
        unsafe { node_stats_out.as_mut() }.vortex_expect("node_stats_out null pointer");

    match cardinality(bind_data) {
        Cardinality::Unknown => {}
        Cardinality::Exact(c) => {
            node_stats.has_estimated_cardinality = true;
            node_stats.estimated_cardinality = c as _;
            node_stats.has_max_cardinality = true;
            node_stats.max_cardinality = c as _;
        }
        Cardinality::Estimate(c) => {
            node_stats.has_estimated_cardinality = true;
            node_stats.estimated_cardinality = c as _;
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_init_global(
    init_input: *const cpp::duckdb_vx_tfunc_init_input,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let init_input = TableInitInput::new(
        unsafe { init_input.as_ref() }.vortex_expect("init_input null pointer"),
    );

    match init_global(&init_input) {
        Ok(init_data) => Data::from(Box::new(init_data)).as_ptr(),
        Err(e) => {
            // Set the error in the error output.
            let msg = e.to_string();
            unsafe { error_out.write(cpp::duckdb_vx_error_create(msg.as_ptr().cast(), msg.len())) };
            ptr::null_mut::<cpp::duckdb_vx_data_>().cast()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_init_local(
    bind_data: *const c_void,
    global_init_data: *mut c_void,
) -> cpp::duckdb_vx_data {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let global_init_data = unsafe { global_init_data.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");

    let init_data = init_local(bind_data, global_init_data);
    Data::from(Box::new(init_data)).as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_bind(
    bind_input: cpp::duckdb_vx_tfunc_bind_input,
    bind_result: cpp::duckdb_vx_tfunc_bind_result,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let bind_input = unsafe { BindInput::own(bind_input) };
    let mut bind_result = unsafe { BindResult::own(bind_result) };

    try_or_null(error_out, || {
        let bind_data = bind(&bind_input, &mut bind_result)?;
        Ok(Data::from(Box::new(bind_data)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_bind_data_clone(
    bind_data: *const c_void,
) -> cpp::duckdb_vx_data {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let copied_data = bind_data.clone();
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
    global_data: *mut c_void,
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
    row_count_out: *mut u64,
    file_size_out: *mut u64,
    error_out: *mut cpp::duckdb_vx_error,
) {
    let global_data = unsafe { global_data.cast::<CopyFunctionGlobal>().as_mut() }
        .vortex_expect("bind_data null pointer");
    try_or(error_out, || {
        let (row_count, file_size) = copy_to_finalize(global_data)?;
        if !row_count_out.is_null() {
            unsafe { *row_count_out = row_count };
        }
        if !file_size_out.is_null() {
            unsafe { *file_size_out = file_size };
        }
        Ok(())
    })
}

/// Returns the number of exported column statistics entries after finalize.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_exported_stats_count(
    global_data: *const c_void,
) -> u64 {
    let Some(global_data) = (unsafe { global_data.cast::<CopyFunctionGlobal>().as_ref() }) else {
        return 0;
    };
    global_data.exported_stats.len() as u64
}

/// Copies one exported column statistic into C-compatible out params.
/// String out-params are heap-allocated with `malloc` and must be freed by the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_exported_stat_at(
    global_data: *const c_void,
    index: u64,
    name_out: *mut *mut c_char,
    null_count_out: *mut u64,
    has_null_count_out: *mut bool,
    num_values_out: *mut u64,
    has_num_values_out: *mut bool,
    column_size_out: *mut u64,
    has_column_size_out: *mut bool,
    min_out: *mut *mut c_char,
    max_out: *mut *mut c_char,
    has_nan_out: *mut bool,
    has_has_nan_out: *mut bool,
) -> bool {
    use std::ffi::CString;

    let Some(global_data) = (unsafe { global_data.cast::<CopyFunctionGlobal>().as_ref() }) else {
        return false;
    };
    let Some(stat) = global_data.exported_stats.get(index as usize) else {
        return false;
    };

    unsafe fn set_cstr(out: *mut *mut c_char, value: &str) {
        if out.is_null() {
            return;
        }
        let c = CString::new(value).unwrap_or_default();
        unsafe { *out = c.into_raw() };
    }

    unsafe {
        set_cstr(name_out, &stat.name);
        if let Some(v) = stat.null_count {
            *null_count_out = v;
            *has_null_count_out = true;
        } else {
            *has_null_count_out = false;
        }
        if let Some(v) = stat.num_values {
            *num_values_out = v;
            *has_num_values_out = true;
        } else {
            *has_num_values_out = false;
        }
        if let Some(v) = stat.column_size_bytes {
            *column_size_out = v;
            *has_column_size_out = true;
        } else {
            *has_column_size_out = false;
        }
        if let Some(ref min) = stat.min {
            set_cstr(min_out, min);
        } else if !min_out.is_null() {
            *min_out = ptr::null_mut();
        }
        if let Some(ref max) = stat.max {
            set_cstr(max_out, max);
        } else if !max_out.is_null() {
            *max_out = ptr::null_mut();
        }
        if let Some(v) = stat.has_nan {
            *has_nan_out = v;
            *has_has_nan_out = true;
        } else {
            *has_has_nan_out = false;
        }
    }
    true
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
