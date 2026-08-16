// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
//
// THIS FILE IS AUTO-GENERATED, DO NOT MAKE EDITS DIRECTLY
//

// clang-format off

#include "duckdb.h"


#pragma once

#define COUNT_STAR_PROJ_IDX UINT64_MAX

typedef struct {
  const char *name;
  size_t name_len;
  const char *duckdb_type;
  size_t duckdb_type_len;
  uint64_t num_children;
} duckdb_vx_schema_node;

typedef struct {
  uint64_t column_id;
  const char *stats_min;
  size_t stats_min_len;
  bool has_stats_min;
  const char *stats_max;
  size_t stats_max_len;
  bool has_stats_max;
  uint64_t stats_null_count;
  bool has_null_count;
  uint64_t stats_num_values;
  bool has_num_values;
  uint64_t total_compressed_size;
  bool has_compressed_size;
  bool contains_nan;
  bool has_contains_nan;
} duckdb_vx_column_stat;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

extern void duckdb_table_function_to_string(const void *bind_data, duckdb_vx_string_map map);

extern
bool duckdb_table_function_statistics(const void *bind_data,
                                      size_t column_index,
                                      duckdb_column_statistics *stats_out);

extern double duckdb_table_function_scan_progress(void *global_state);

extern
void duckdb_table_function_get_partition_data(void *global_init_data,
                                              void *local_init_data,
                                              duckdb_vx_partition_data *partition_data_out);

extern
bool duckdb_table_function_pushdown_complex_filter(void *bind_data,
                                                   duckdb_vx_expr expr,
                                                   duckdb_vx_error *error_out);

extern
bool duckdb_table_function_pushdown_projection_expression(void *bind_data,
                                                          duckdb_vx_expr expr,
                                                          size_t column_id,
                                                          duckdb_vx_error *error_out);

extern
bool duckdb_table_function_pushdown_projection_aggregates(void *bind_data,
                                                          duckdb_vx_agg_input input,
                                                          duckdb_vx_error *error_out);

extern
void duckdb_table_function_scan(void *global_init_data,
                                void *local_init_data,
                                duckdb_data_chunk output,
                                duckdb_vx_error *error_out);

extern bool duckdb_table_function_pushdown_expression(duckdb_vx_expr expr);

extern
void duckdb_table_function_cardinality(const void *bind_data,
                                       duckdb_vx_node_statistics *node_stats_out);

extern
duckdb_vx_data duckdb_table_function_init_global(const duckdb_vx_tfunc_init_input *init_input,
                                                 duckdb_vx_error *error_out);

extern
duckdb_vx_data duckdb_table_function_init_local(const void *bind_data,
                                                void *global_init_data);

extern
duckdb_vx_data duckdb_table_function_bind(duckdb_vx_tfunc_bind_input bind_input,
                                          duckdb_vx_tfunc_bind_result bind_result,
                                          duckdb_vx_error *error_out);

extern duckdb_vx_data duckdb_table_function_bind_data_clone(const void *bind_data);

extern
duckdb_vx_data duckdb_copy_function_copy_to_bind(const char *const *column_names,
                                                 size_t column_name_count,
                                                 const duckdb_logical_type *column_types,
                                                 size_t column_type_count,
                                                 duckdb_vx_error *error_out);

extern
duckdb_vx_data duckdb_copy_function_copy_to_initialize_global(const void *bind_data,
                                                              const char *file_path,
                                                              const uint8_t *field_ids_bytes,
                                                              size_t field_ids_len,
                                                              const uint8_t *encryption_key_bytes,
                                                              size_t encryption_key_len,
                                                              duckdb_vx_error *error_out);

extern
void duckdb_copy_function_copy_to_sink(const void *bind_data,
                                       void *global_data,
                                       duckdb_data_chunk data_chunk,
                                       duckdb_vx_error *error_out);

extern
void duckdb_copy_function_copy_to_finalize(void *global_data,
                                           uint64_t *row_count_out,
                                           uint64_t *file_size_out,
                                           duckdb_vx_error *error_out);

extern uint64_t duckdb_copy_function_exported_stats_count(const void *global_data);

extern
bool duckdb_copy_function_exported_stat_at(const void *global_data,
                                           uint64_t index,
                                           char **name_out,
                                           uint64_t *null_count_out,
                                           bool *has_null_count_out,
                                           uint64_t *num_values_out,
                                           bool *has_num_values_out,
                                           uint64_t *column_size_out,
                                           bool *has_column_size_out,
                                           char **min_out,
                                           char **max_out,
                                           bool *has_nan_out,
                                           bool *has_has_nan_out);

extern
duckdb_vx_data duckdb_vortex_full_metadata_open(const char *file_path,
                                                duckdb_vx_error *error_out);

extern uint64_t duckdb_vortex_full_metadata_row_count(const void *meta);

extern uint64_t duckdb_vortex_full_metadata_file_size(const void *meta);

extern size_t duckdb_vortex_full_metadata_schema_count(const void *meta);

extern
bool duckdb_vortex_full_metadata_schema_at(const void *meta,
                                           size_t index,
                                           duckdb_vx_schema_node *out);

extern size_t duckdb_vortex_full_metadata_stats_count(const void *meta);

extern
bool duckdb_vortex_full_metadata_stat_at(const void *meta,
                                         size_t index,
                                         duckdb_vx_column_stat *out);

extern
uint8_t *duckdb_vortex_read_ducklake_field_ids(const char *file_path,
                                               size_t *len_out,
                                               duckdb_vx_error *error_out);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

// clang-format on
