// SPDX-License-Identifier: Apache-2.0
//! Extra DuckDB C API bindings not present in the generated `cpp.rs`.
//! cbindgen:ignore

use crate::cpp::duckdb_logical_type;
use crate::cpp::duckdb_type;
use crate::cpp::idx_t;

unsafe extern "C-unwind" {
    #[link_name = "\u{1}_duckdb_enum_internal_type"]
    pub fn duckdb_enum_internal_type(type_: duckdb_logical_type) -> duckdb_type;

    #[link_name = "\u{1}_duckdb_enum_dictionary_size"]
    pub fn duckdb_enum_dictionary_size(type_: duckdb_logical_type) -> u32;

    #[link_name = "\u{1}_duckdb_enum_dictionary_value"]
    pub fn duckdb_enum_dictionary_value(
        type_: duckdb_logical_type,
        index: idx_t,
    ) -> *mut ::std::os::raw::c_char;

    #[link_name = "\u{1}_duckdb_create_enum_type"]
    pub fn duckdb_create_enum_type(
        member_names: *const *const ::std::os::raw::c_char,
        member_count: idx_t,
    ) -> duckdb_logical_type;
}
