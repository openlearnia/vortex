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

extern void duckdb_table_function_to_string(const void *bind, duckdb_vx_string_map map);

extern
bool duckdb_table_function_pushdown_complex_filter(void *bind,
                                                   duckdb_vx_expr expr,
                                                   duckdb_vx_error *error);

extern
bool duckdb_table_function_pushdown_projection_expression(void *bind,
                                                          duckdb_vx_expr expr,
                                                          size_t column_id,
                                                          duckdb_vx_error *error);

extern
bool duckdb_table_function_pushdown_projection_aggregates(void *bind,
                                                          duckdb_vx_agg_input input,
                                                          duckdb_vx_error *error);

extern bool duckdb_table_function_pushdown_expression(duckdb_vx_expr expr);

extern
void duckdb_table_function_cardinality(const void *bind,
                                       uint64_t file_count,
                                       duckdb_vx_node_statistics *stats);

extern
duckdb_vx_data duckdb_table_function_init_global(const duckdb_vx_tfunc_init_input *init_input,
                                                 duckdb_vx_error *error);

extern duckdb_vx_data duckdb_table_function_init_local(const void *bind, const void *global);

extern
duckdb_vx_data duckdb_reader_bind(const void *first_file,
                                  duckdb_bind_result result,
                                  duckdb_vx_error *error_out);

extern
duckdb_vx_data duckdb_reader_open(const char *file_path,
                                  size_t file_path_len,
                                  duckdb_vx_error *error);

extern
bool duckdb_reader_get_statistics(const void *file,
                                  const void *bind,
                                  const char *column_name,
                                  size_t column_name_len,
                                  duckdb_column_statistics *stats_out);

extern bool duckdb_table_function_can_get_partition_stats(const void *bind);

extern
duckdb_vx_data duckdb_footer_get_cached(void *bind,
                                        const char *path,
                                        size_t len,
                                        uint64_t *row_count_out,
                                        duckdb_vx_error *error);

extern
bool duckdb_footer_get_statistics(const void *footer,
                                  size_t column_index,
                                  duckdb_column_statistics *stats_out);

extern bool duckdb_reader_initialize(const void *global, void *file, duckdb_vx_error *error);

extern duckdb_logical_type duckdb_reader_bind_column_type(const void *bind, size_t index);

extern bool duckdb_reader_is_aggregate(const void *bind);

extern bool duckdb_reader_try_initialize_scan(void *local, void *file);

extern
bool duckdb_reader_scan(const void *file,
                        const void *global,
                        void *local,
                        duckdb_data_chunk chunk,
                        duckdb_vx_error *error);

extern double duckdb_reader_get_progress_in_file(const void *file);

extern
bool duckdb_reader_finalize_scan(const void *global,
                                 duckdb_data_chunk chunk,
                                 duckdb_vx_error *error);

extern void duckdb_reader_finish_reading(const void *global, void *local);

extern duckdb_vx_data duckdb_table_function_bind_data_clone(const void *bind);

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
                                       const void *global_data,
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

extern duckdb_vx_data duckdb_copy_function_prepare_batch_new(void);

extern
void duckdb_copy_function_prepare_batch_push(const void *bind,
                                             void *batch,
                                             duckdb_data_chunk chunk,
                                             duckdb_vx_error *error);

extern
void duckdb_copy_function_flush_batch(const void *global,
                                      const void *batch,
                                      duckdb_vx_error *error);

extern
bool duckdb_copy_function_get_written_file_statistics(const void *global_data,
                                                      duckdb_vx_written_file_statistics *out);

extern
bool duckdb_copy_function_get_written_column_statistics(const void *global_data,
                                                        size_t column_index,
                                                        duckdb_vx_written_column_statistics *out,
                                                        duckdb_vx_error *error_out);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

// clang-format on
