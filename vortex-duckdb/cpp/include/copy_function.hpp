// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#pragma once
#include "data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"

using namespace duckdb;

struct VortexCopyBindData final : TableFunctionData {
    VortexCopyBindData(unique_ptr<CData> ffi_bind, vector<Identifier> column_names,
                       vector<LogicalType> column_types)
        : ffi_bind(std::move(ffi_bind)), column_names(std::move(column_names)),
          column_types(std::move(column_types)) {
    }
    unique_ptr<CData> ffi_bind;
    // Column names in write order, used to key WRITTEN_FILE_STATISTICS.
    vector<Identifier> column_names;
    // Column types in write order — needed to emit variant stats at leaf paths.
    vector<LogicalType> column_types;
    //! Optional DuckLake field_ids struct — serialized into Vortex `ducklake.field_ids` metadata.
    Value field_ids;
    //! Optional AES-GCM key (16 or 32 raw bytes) from encryption_config.footer_key_value.
    string encryption_key;
};

// GeometryStatsData seeded with the empty extent, like parquet's GeoStatisticsState.
struct VortexGeoStatsAccumulator {
    VortexGeoStatsAccumulator() {
        data.SetEmpty();
    }
    GeometryStatsData data;
};

// Per-VARIANT-column state for synthetic leaf statistics: the first pushed chunk
// is analyzed for its shredding type and a variant_to_parquet_variant transform
// expression is built that produces the parquet-shaped struct
// STRUCT(metadata BLOB, value BLOB, typed_value ...) used only for stats
// accumulation (physical storage stays unshredded). A null transform_expr means
// the transform function is unavailable or the column is unshredded - stats fall
// back to the metadata-only entry.
struct VortexVariantStatsState {
    bool analyzed = false;
    // The transform threw mid-COPY: leaf stats accumulated so far are partial
    // and must be discarded at finalize.
    bool poisoned = false;
    unique_ptr<Expression> transform_expr;
    LogicalType transformed_type;
    string variant_type_str;
};

struct VortexCopyGlobalState final : GlobalFunctionData {
    VortexCopyGlobalState(unique_ptr<CData> ffi_global) : ffi_global(std::move(ffi_global)) {
    }
    unique_ptr<CData> ffi_global;
    // Non-owning; null when the plan does not request statistics.
    CopyFunctionFileStatistics *written_stats = nullptr;
    // Geometry extent stats per quoted stats key ("col", "s"."a", "l"."element",
    // "m"."value"), accumulated as chunks are pushed and merged into
    // column_statistics at finalize, matching the parquet writer's bbox_*/geo_types.
    mutex geo_stats_lock;
    unordered_map<string, VortexGeoStatsAccumulator> geo_stats;
    // VARIANT transform state per column index, lazily analyzed on the first
    // pushed chunk. The transform output is accumulated as synthetic leaf stats
    // through the Rust leaf-stats walker.
    mutex variant_stats_lock;
    unordered_map<idx_t, VortexVariantStatsState> variant_stats;
};

struct VortexCopyPreparedBatchData final : PreparedBatchData {
    VortexCopyPreparedBatchData(unique_ptr<CData> ffi_copy_prepared)
        : ffi_copy_prepared(std::move(ffi_copy_prepared)) {
    }
    unique_ptr<CData> ffi_copy_prepared;
};
