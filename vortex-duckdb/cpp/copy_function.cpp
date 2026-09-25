// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "copy_function.hpp"
#include "data.hpp"
#include "error.hpp"
#include "vortex_duckdb.h"
#include "table_function.h"
#include "vortex.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include <algorithm>

unique_ptr<FunctionData> copy_to_bind(ClientContext &,
                                      CopyFunctionBindInput &input,
                                      const vector<Identifier> &names,
                                      const vector<LogicalType> &types) {
    // DuckLake integration: optional field_ids struct (schema identity for
    // add_files/managed tables) and footer encryption key.
    Value field_ids;
    string encryption_key;
    for (auto &opt : input.info.options) {
        auto name = StringUtil::Lower(opt.first.GetIdentifierName());
        if (name == "field_ids") {
            if (opt.second.empty()) {
                throw BinderException("Vortex field_ids option requires a value");
            }
            field_ids = opt.second.back();
            continue;
        }
        if (name == "encryption_config") {
            if (opt.second.empty()) {
                throw BinderException("Vortex encryption_config option requires a value");
            }
            auto &cfg = opt.second.back();
            if (cfg.type().id() != LogicalTypeId::STRUCT) {
                throw BinderException("Vortex encryption_config must be a STRUCT");
            }
            auto &children = StructValue::GetChildren(cfg);
            auto &child_types = StructType::GetChildTypes(cfg.type());
            for (idx_t i = 0; i < children.size(); i++) {
                if (StringUtil::CIEquals(child_types[i].first.GetIdentifierName(), "footer_key_value")) {
                    encryption_key = StringValue::Get(children[i]);
                }
            }
            if (encryption_key.empty()) {
                throw BinderException("Vortex encryption_config requires footer_key_value");
            }
            continue;
        }
        throw NotImplementedException("Unsupported Vortex COPY option \"%s\"",
                                      opt.first.GetIdentifierName());
    }

    vector<const char *> ffi_names(names.size());
    for (size_t i = 0; i < names.size(); ++i) {
        ffi_names[i] = names[i].c_str();
    }

    vector<duckdb_logical_type> ffi_types(types.size());
    for (size_t i = 0; i < types.size(); ++i) {
        // duckdb C api doesn't allow passing const LogicalTypes. We never
        // modify input in copy function.
        ffi_types[i] = reinterpret_cast<duckdb_logical_type>(const_cast<LogicalType *>(&types[i]));
    }

    duckdb_vx_error error_out = nullptr;
    const duckdb_vx_data ffi_bind_data = duckdb_copy_function_copy_to_bind(ffi_names.data(),
                                                                           ffi_names.size(),
                                                                           ffi_types.data(),
                                                                           ffi_types.size(),
                                                                           &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }
    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_bind_data));
    auto result = make_uniq<VortexCopyBindData>(std::move(cdata), names, types);
    result->field_ids = std::move(field_ids);
    result->encryption_key = std::move(encryption_key);
    return std::move(result);
}

unique_ptr<GlobalFunctionData>
copy_to_initialize_global(ClientContext &, FunctionData &bind_data, const string &file_path) {
    const VortexCopyBindData &bind = bind_data.Cast<VortexCopyBindData>();
    const void *const ffi_bind = bind.ffi_bind->DataPtr();

    // Serialize the DuckLake field_ids struct so the writer can store it as a
    // Vortex user metadata segment (`ducklake.field_ids`).
    string field_ids_blob;
    const uint8_t *field_ids_ptr = nullptr;
    size_t field_ids_len = 0;
    if (!bind.field_ids.IsNull()) {
        MemoryStream stream;
        BinarySerializer::Serialize(bind.field_ids, stream);
        field_ids_blob.assign(const_char_ptr_cast(stream.GetData()), stream.GetPosition());
        field_ids_ptr = reinterpret_cast<const uint8_t *>(field_ids_blob.data());
        field_ids_len = field_ids_blob.size();
    }

    duckdb_vx_error error_out = nullptr;
    const duckdb_vx_data ffi_global = duckdb_copy_function_copy_to_initialize_global(
        ffi_bind, file_path.c_str(), field_ids_ptr, field_ids_len,
        reinterpret_cast<const uint8_t *>(bind.encryption_key.data()), bind.encryption_key.size(),
        &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }

    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_global));
    return make_uniq<VortexCopyGlobalState>(std::move(cdata));
}

// Accumulate WKB extent stats for a GEOMETRY leaf over the rows in sel. The
// accumulator is only created once a valid value is seen, so all-NULL geometry
// leaves emit no stats.
static void AccumulateGeometryLeafStats(unordered_map<string, VortexGeoStatsAccumulator> &geo_stats,
                                        const string &path, const Vector &vector, const SelectionVector &sel,
                                        idx_t count) {
    auto values = vector.Values<string_t>();
    for (idx_t i = 0; i < count; i++) {
        auto entry = values[sel.get_index(i)];
        if (entry.IsValid()) {
            geo_stats[path].data.Update(entry.GetValue());
        }
    }
}

// Restrict sel to rows where the vector is non-NULL.
static SelectionVector ValidRows(const Vector &vector, const SelectionVector &sel, idx_t count,
                                 idx_t &valid_count) {
    UnifiedVectorFormat vdata;
    vector.ToUnifiedFormat(vdata);
    SelectionVector valid_sel(count);
    valid_count = 0;
    for (idx_t i = 0; i < count; i++) {
        auto idx = sel.get_index(i);
        if (vdata.validity.RowIsValid(vdata.sel->get_index(idx))) {
            valid_sel.set_index(valid_count++, idx);
        }
    }
    return valid_sel;
}

// Select the child rows belonging to non-NULL list entries, like ListStats::Verify.
// Works for MAP vectors as well, which share the list layout.
static SelectionVector ValidListChildRows(const Vector &vector, const SelectionVector &sel, idx_t count,
                                          idx_t &child_count) {
    auto entries = vector.Values<list_entry_t>();
    child_count = 0;
    for (idx_t i = 0; i < count; i++) {
        auto entry = entries[sel.get_index(i)];
        if (entry.IsValid()) {
            child_count += entry.GetValue().length;
        }
    }
    SelectionVector child_sel(child_count);
    idx_t child_idx = 0;
    for (idx_t i = 0; i < count; i++) {
        auto entry = entries[sel.get_index(i)];
        if (!entry.IsValid()) {
            continue;
        }
        auto list = entry.GetValue();
        for (idx_t j = 0; j < list.length; j++) {
            child_sel.set_index(child_idx++, list.offset + j);
        }
    }
    return child_sel;
}

// Select the child rows of non-NULL arrays, like ArrayStats::Verify.
static SelectionVector ValidArrayChildRows(const Vector &vector, const SelectionVector &sel, idx_t count,
                                           idx_t &child_count) {
    const auto array_size = ArrayType::GetSize(vector.GetType());
    UnifiedVectorFormat vdata;
    vector.ToUnifiedFormat(vdata);
    child_count = 0;
    for (idx_t i = 0; i < count; i++) {
        if (vdata.validity.RowIsValid(vdata.sel->get_index(sel.get_index(i)))) {
            child_count += array_size;
        }
    }
    SelectionVector child_sel(child_count);
    idx_t child_idx = 0;
    for (idx_t i = 0; i < count; i++) {
        auto index = vdata.sel->get_index(sel.get_index(i));
        if (!vdata.validity.RowIsValid(index)) {
            continue;
        }
        for (idx_t j = 0; j < array_size; j++) {
            child_sel.set_index(child_idx++, index * array_size + j);
        }
    }
    return child_sel;
}

// Recurse to GEOMETRY leaf vectors, accumulating extent stats under the quoted dot
// path the parquet writer and the Rust leaf-stats accumulator emit ("s"."a",
// "l"."element", "m"."value"). UNION and VARIANT carry no leaf geometry stats.
// Only leaf values under non-NULL container rows are counted, like parquet.
static void AccumulateGeometryStats(unordered_map<string, VortexGeoStatsAccumulator> &geo_stats,
                                    const string &path, const LogicalType &type, const Vector &vector,
                                    const SelectionVector &sel, idx_t count) {
    switch (type.id()) {
    case LogicalTypeId::GEOMETRY:
        AccumulateGeometryLeafStats(geo_stats, path, vector, sel, count);
        return;
    case LogicalTypeId::STRUCT: {
        idx_t valid_count = 0;
        auto valid_sel = ValidRows(vector, sel, count, valid_count);
        auto &entries = StructVector::GetEntries(vector);
        D_ASSERT(entries.size() == StructType::GetChildCount(type));
        for (idx_t i = 0; i < entries.size(); i++) {
            auto child_path = path + "." + SQLQuotedIdentifier::ToString(StructType::GetChildName(type, i));
            AccumulateGeometryStats(geo_stats, child_path, StructType::GetChildType(type, i), entries[i],
                                    valid_sel, valid_count);
        }
        return;
    }
    case LogicalTypeId::LIST: {
        idx_t child_count = 0;
        auto child_sel = ValidListChildRows(vector, sel, count, child_count);
        AccumulateGeometryStats(geo_stats, path + "." + SQLQuotedIdentifier::ToString("element"),
                                ListType::GetChildType(type), ListVector::GetChild(vector), child_sel,
                                child_count);
        return;
    }
    case LogicalTypeId::ARRAY: {
        idx_t child_count = 0;
        auto child_sel = ValidArrayChildRows(vector, sel, count, child_count);
        AccumulateGeometryStats(geo_stats, path + "." + SQLQuotedIdentifier::ToString("element"),
                                ArrayType::GetChildType(type), ArrayVector::GetChild(vector), child_sel,
                                child_count);
        return;
    }
    case LogicalTypeId::MAP: {
        idx_t child_count = 0;
        auto child_sel = ValidListChildRows(vector, sel, count, child_count);
        AccumulateGeometryStats(geo_stats, path + "." + SQLQuotedIdentifier::ToString("key"),
                                MapType::KeyType(type), MapVector::GetKeys(vector), child_sel, child_count);
        AccumulateGeometryStats(geo_stats, path + "." + SQLQuotedIdentifier::ToString("value"),
                                MapType::ValueType(type), MapVector::GetValues(vector), child_sel,
                                child_count);
        return;
    }
    default:
        return;
    }
}

// Accumulate extent stats for every GEOMETRY-typed leaf in the chunk. Only runs
// when the plan requests written statistics (DuckLake managed tables always do).
static void AccumulateChunkGeometryStats(VortexCopyGlobalState &global, const vector<Identifier> &names,
                                         DataChunk &input) {
    if (!global.written_stats) {
        return;
    }
    D_ASSERT(input.ColumnCount() == names.size());
    // Batch-mode prepare_batch can push chunks on the same state from multiple tasks.
    lock_guard<mutex> lock(global.geo_stats_lock);
    SelectionVector flat_sel;
    for (idx_t i = 0; i < input.ColumnCount(); i++) {
        AccumulateGeometryStats(global.geo_stats, SQLQuotedIdentifier::ToString(names[i]),
                                input.data[i].GetType(), input.data[i], flat_sel, input.size());
    }
}

/// Look up a scalar function in the system catalog, returning null when absent.
/// (Same helper as spatial_overrides.cpp.)
static optional_ptr<ScalarFunctionCatalogEntry> FindSystemScalarFunction(ClientContext &context,
                                                                         const char *name) {
    auto entry = Catalog::GetSystemCatalog(context).GetEntry(context,
                                                           CatalogType::SCALAR_FUNCTION_ENTRY,
                                                           Identifier::DefaultSchema(),
                                                           Identifier(name),
                                                           OnEntryNotFound::RETURN_NULL);
    if (!entry) {
        return nullptr;
    }
    return &entry->Cast<ScalarFunctionCatalogEntry>();
}

// Port of duckdb's ToStructuredType (variant_stats.cpp): unwrap the
// STRUCT(typed_value T [, untyped_value_index UINTEGER]) shredding envelope
// recursively down to the plain structured type.
static LogicalType VariantShreddingToStructuredType(const LogicalType &shredding) {
    if (shredding.id() != LogicalTypeId::STRUCT) {
        // not a struct - this is a primitive type
        return shredding;
    }
    auto &child_types = StructType::GetChildTypes(shredding);
    D_ASSERT(child_types.size() <= 2);
    auto &typed_value = child_types[VariantStats::TYPED_VALUE_INDEX].second;
    if (typed_value.id() == LogicalTypeId::STRUCT) {
        auto &struct_children = StructType::GetChildTypes(typed_value);
        child_list_t<LogicalType> structured_children;
        vector<idx_t> indices(struct_children.size());
        for (idx_t i = 0; i < indices.size(); i++) {
            indices[i] = i;
        }
        std::sort(indices.begin(), indices.end(), [&](const idx_t &lhs, const idx_t &rhs) {
            auto &a = struct_children[lhs].first;
            auto &b = struct_children[rhs].first;
            return a < b;
        });
        for (auto &index : indices) {
            auto &child = struct_children[index];
            structured_children.emplace_back(child.first, VariantShreddingToStructuredType(child.second));
        }
        return LogicalType::STRUCT(structured_children);
    }
    if (typed_value.id() == LogicalTypeId::LIST) {
        return LogicalType::LIST(VariantShreddingToStructuredType(ListType::GetChildType(typed_value)));
    }
    return typed_value;
}

// Port of VariantColumnWriter::TransformTypedValueRecursive (parquet
// convert_variant.cpp): struct fields and list elements become
// {value BLOB, typed_value <recursive>} shredding groups.
static LogicalType VariantTransformTypedValue(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::STRUCT: {
        auto &child_types = StructType::GetChildTypes(type);
        child_list_t<LogicalType> replaced_types;
        for (auto &entry : child_types) {
            child_list_t<LogicalType> child_children;
            child_children.emplace_back("value", LogicalType::BLOB);
            if (entry.second.id() != LogicalTypeId::VARIANT) {
                child_children.emplace_back("typed_value", VariantTransformTypedValue(entry.second));
            }
            replaced_types.emplace_back(entry.first, LogicalType::STRUCT(child_children));
        }
        return LogicalType::STRUCT(replaced_types);
    }
    case LogicalTypeId::LIST: {
        auto &child_type = ListType::GetChildType(type);
        child_list_t<LogicalType> replaced_types;
        replaced_types.emplace_back("value", LogicalType::BLOB);
        if (child_type.id() != LogicalTypeId::VARIANT) {
            replaced_types.emplace_back("typed_value", VariantTransformTypedValue(child_type));
        }
        return LogicalType::LIST(LogicalType::STRUCT(replaced_types));
    }
    case LogicalTypeId::UNION:
    case LogicalTypeId::MAP:
    case LogicalTypeId::VARIANT:
    case LogicalTypeId::ARRAY:
        // Cannot occur in a structured shredding type.
        throw InternalException("'%s' can't appear inside a 'typed_value' shredded type!", type.ToString());
    default:
        return type;
    }
}

// Analyze a VARIANT column's first chunk (mirroring the parquet writer's
// first-row-group analysis) and build the variant_to_parquet_variant transform
// expression whose output is the synthetic parquet-shaped struct used for leaf
// statistics. Without a shredded child or without the transform function
// (parquet extension not loaded) the column falls back to metadata-only stats.
static void AnalyzeVariantColumn(ClientContext &context, VortexVariantStatsState &state,
                                 const Vector &vec, const Identifier &name, idx_t index, idx_t count) {
    state.analyzed = true;
    VariantShreddingStats stats;
    stats.Update(vec, count);
    auto shredded = stats.GetShreddedType();
    optional_ptr<const LogicalType> shredding;
    for (auto &child : StructType::GetChildTypes(shredded)) {
        if (child.first.GetIdentifierName() == "shredded") {
            shredding = child.second;
        }
    }
    child_list_t<LogicalType> children;
    children.emplace_back("metadata", LogicalType::BLOB);
    children.emplace_back("value", LogicalType::BLOB);
    if (shredding) {
        children.emplace_back("typed_value",
                              VariantTransformTypedValue(VariantShreddingToStructuredType(*shredding)));
    }
    // No PARQUET_VARIANT alias - the alias would hide the struct type in ToString.
    state.transformed_type = LogicalType::STRUCT(std::move(children));
    state.variant_type_str = state.transformed_type.ToString();
    if (!shredding) {
        // Unshredded variants have no leaf stats to emit; skip the transform.
        return;
    }
    auto entry = FindSystemScalarFunction(context, "variant_to_parquet_variant");
    if (!entry || entry->functions.functions.empty()) {
        return;
    }
    BoundScalarFunction bound_func(entry->functions.functions[0]);
    bound_func.SetReturnType(state.transformed_type);
    vector<unique_ptr<Expression>> arguments;
    arguments.push_back(make_uniq<BoundReferenceExpression>(name, vec.GetType(), index));
    state.transform_expr =
        make_uniq<BoundFunctionExpression>(std::move(bound_func), std::move(arguments), nullptr);
}

// For each VARIANT column, evaluate the transform expression on the chunk and
// push the resulting single-column struct chunk into the Rust leaf-stats
// accumulator. Only runs when the plan requests written statistics.
static void AccumulateChunkVariantStats(ClientContext &context, VortexCopyGlobalState &global,
                                        const vector<Identifier> &names, const vector<LogicalType> &types,
                                        DataChunk &input) {
    if (!global.written_stats) {
        return;
    }
    D_ASSERT(input.ColumnCount() == names.size());
    lock_guard<mutex> lock(global.variant_stats_lock);
    for (idx_t i = 0; i < input.ColumnCount(); i++) {
        if (types[i].id() != LogicalTypeId::VARIANT) {
            continue;
        }
        auto &state = global.variant_stats[i];
        try {
            if (!state.analyzed) {
                AnalyzeVariantColumn(context, state, input.data[i], names[i], i, input.size());
            }
            if (!state.transform_expr || state.poisoned) {
                continue;
            }
            DataChunk transformed;
            transformed.Initialize(Allocator::Get(context), {state.transformed_type});
            ExpressionExecutor executor(context);
            executor.AddExpression(*state.transform_expr);
            executor.Execute(&input, transformed);

            duckdb_vx_error error_out = nullptr;
            duckdb_copy_function_accumulate_stats_chunk(global.ffi_global->DataPtr(), names[i].c_str(),
                                                        names[i].size(),
                                                        reinterpret_cast<duckdb_data_chunk>(&transformed),
                                                        &error_out);
            if (error_out) {
                throw ExecutorException(IntoErrString(error_out));
            }
        } catch (const std::exception &e) {
            // The synthetic stats path is advisory and must not fail the write:
            // the transform is fallible (e.g. values out of range for the
            // parquet variant encoding). Poison the column so finalize drops
            // the partial leaf stats accumulated so far.
            state.analyzed = true;
            state.transform_expr.reset();
            state.variant_type_str.clear();
            state.poisoned = true;
            DUCKDB_LOG_WARNING(context, StringUtil::Format("vortex COPY: disabling variant stats "
                                                           "for column %llu: %s", i, e.what()));
        }
    }
}

void copy_to_sink(ExecutionContext &context,
                  FunctionData &bind_data,
                  GlobalFunctionData &gstate,
                  LocalFunctionData &,
                  DataChunk &input) {
    const VortexCopyBindData &bind = bind_data.Cast<VortexCopyBindData>();
    VortexCopyGlobalState &global = gstate.Cast<VortexCopyGlobalState>();

    const void *const ffi_bind = bind.ffi_bind->DataPtr();
    const void *const ffi_global = global.ffi_global->DataPtr();

    duckdb_data_chunk ffi_chunk = reinterpret_cast<duckdb_data_chunk>(&input);
    duckdb_vx_error error_out = nullptr;
    duckdb_copy_function_copy_to_sink(ffi_bind, ffi_global, ffi_chunk, &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }
    AccumulateChunkGeometryStats(global, bind.column_names, input);
    AccumulateChunkVariantStats(context.client, global, bind.column_names, bind.column_types, input);
}

// CopyToFileInfo::file_stats is owned by the operator's sink state, which outlives gstate.
void copy_to_get_written_statistics(ClientContext &,
                                    FunctionData &,
                                    GlobalFunctionData &gstate,
                                    CopyFunctionFileStatistics &statistics) {
    gstate.Cast<VortexCopyGlobalState>().written_stats = &statistics;
}

// Nested containers carry statistics on their leaf paths only, like parquet.
static bool IsNestedContainerType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::STRUCT:
    case LogicalTypeId::LIST:
    case LogicalTypeId::MAP:
    case LogicalTypeId::ARRAY:
    case LogicalTypeId::UNION:
        return true;
    default:
        return false;
    }
}

// Render a stats bound as a string. BLOB bounds use bare uppercase hex (like
// parquet's BlobStatsUnifier) - DuckLake's variant parser checks the "value"
// leaf for min/max == "00" to detect fully shredded fields. BOOLEAN bounds use
// "1"/"0" like parquet's NumericStatsUnifier<int8_t>.
static string StatsBoundToString(const Value &value) {
    if (value.type().id() == LogicalTypeId::BOOLEAN) {
        return BooleanValue::Get(value) ? "1" : "0";
    }
    if (value.type().id() != LogicalTypeId::BLOB) {
        return value.ToString();
    }
    auto blob = StringValue::Get(value);
    auto data = const_data_ptr_cast(blob.c_str());
    string result;
    result.reserve(blob.size() * 2);
    for (idx_t i = 0; i < blob.size(); i++) {
        result += Blob::HEX_TABLE[data[i] >> 4];
        result += Blob::HEX_TABLE[data[i] & 0x0F];
    }
    return result;
}

// Move an FFI column-statistics struct into a DuckLake stat map, destroying owned values.
static void FillColumnStats(case_insensitive_map_t<Value> &column,
                            duckdb_vx_written_column_statistics &col_stats) {
    column["num_values"] = Value::UBIGINT(col_stats.num_values);
    if (col_stats.has_column_size) {
        column["column_size_bytes"] = Value::UBIGINT(col_stats.column_size_bytes);
    }
    if (col_stats.has_null_count) {
        column["null_count"] = Value::UBIGINT(col_stats.null_count);
    }
    if (col_stats.min) {
        column["min"] = Value(StatsBoundToString(*reinterpret_cast<Value *>(col_stats.min)));
        column["min_is_exact"] = Value::BOOLEAN(col_stats.min_is_exact);
        duckdb_destroy_value(&col_stats.min);
    }
    if (col_stats.max) {
        column["max"] = Value(StatsBoundToString(*reinterpret_cast<Value *>(col_stats.max)));
        column["max_is_exact"] = Value::BOOLEAN(col_stats.max_is_exact);
        duckdb_destroy_value(&col_stats.max);
    }
    if (col_stats.has_nan_stat) {
        column["has_nan"] = Value::BOOLEAN(col_stats.contains_nan);
    }
}

void copy_to_finalize(ClientContext &, FunctionData &bind_data, GlobalFunctionData &gstate) {
    auto &global = gstate.Cast<VortexCopyGlobalState>();
    void *const ffi_global = global.ffi_global->DataPtr();
    duckdb_vx_error error_out = nullptr;
    duckdb_copy_function_copy_to_finalize(ffi_global, &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }

    if (!global.written_stats) {
        return;
    }
    auto &bind = bind_data.Cast<VortexCopyBindData>();
    auto &names = bind.column_names;
    auto &types = bind.column_types;
    duckdb_vx_written_file_statistics file_stats;
    if (!duckdb_copy_function_get_written_file_statistics(ffi_global, &file_stats)) {
        // Statistics were requested (written_stats is set) but the finished write produced none;
        // that is an internal inconsistency, not a silently empty result.
        throw InternalException("vortex COPY: written statistics were requested but not produced");
    }
    if (file_stats.num_columns != names.size()) {
        throw InternalException("vortex COPY: %llu statistics columns for %llu written columns",
                                file_stats.num_columns,
                                names.size());
    }
    D_ASSERT(global.written_stats != nullptr);
    global.written_stats->row_count = file_stats.row_count;
    global.written_stats->file_size_bytes = file_stats.file_size_bytes;
    global.written_stats->footer_size_bytes = Value::UBIGINT(file_stats.footer_size_bytes);
    global.written_stats->extra_info["row_group_count"] = Value::UBIGINT(file_stats.row_group_count);
    for (idx_t i = 0; i < file_stats.num_columns; i++) {
        if (IsNestedContainerType(types[i])) {
            // Nested columns carry stats on leaf paths only, matching parquet.
            continue;
        }
        duckdb_vx_written_column_statistics col_stats {};
        duckdb_vx_error col_error = nullptr;
        if (!duckdb_copy_function_get_written_column_statistics(ffi_global, i, &col_stats, &col_error)) {
            if (col_error) {
                throw ExecutorException(IntoErrString(col_error));
            }
            throw InternalException("vortex COPY: no statistics for column %llu after finalize", i);
        }
        case_insensitive_map_t<Value> column;
        FillColumnStats(column, col_stats);
        // DuckLake keys column statistics by a quoted, dot-separated path (see
        // DuckLakeUtil::ParseQuotedList); match the parquet writer, which quotes each name.
        string stats_key = SQLQuotedIdentifier::ToString(names[i]);
        if (types[i].id() == LogicalTypeId::VARIANT) {
            // DuckLake expects variant stats under leaf paths (metadata/typed_value/value).
            // The metadata leaf carries the synthetic transform type when the
            // variant transform ran; otherwise emit the unshredded parquet shape.
            auto variant_state = global.variant_stats.find(i);
            if (variant_state != global.variant_stats.end() && !variant_state->second.variant_type_str.empty()) {
                column["variant_type"] = Value(variant_state->second.variant_type_str);
            } else {
                column["variant_type"] = Value("STRUCT(metadata BLOB, value BLOB)");
            }
            stats_key += "." + SQLQuotedIdentifier::ToString("metadata");
        }
        global.written_stats->column_statistics.emplace(std::move(stats_key), std::move(column));
    }
    // A poisoned variant column has partially accumulated leaf stats - drop all
    // paths nested under it.
    vector<string> poisoned_prefixes;
    for (auto &entry : global.variant_stats) {
        if (entry.second.poisoned) {
            poisoned_prefixes.push_back(SQLQuotedIdentifier::ToString(names[entry.first]) + ".");
        }
    }
    // Leaf-level statistics for nested columns (e.g. `"l"."element"`), accumulated in Rust at
    // push time since the Vortex footer only stores per-top-level-field stats.
    const idx_t leaf_count = duckdb_copy_function_get_written_leaf_statistics_count(ffi_global);
    for (idx_t i = 0; i < leaf_count; i++) {
        duckdb_vx_written_leaf_statistics leaf {};
        duckdb_vx_error leaf_error = nullptr;
        if (!duckdb_copy_function_get_written_leaf_statistics(ffi_global, i, &leaf, &leaf_error)) {
            if (leaf_error) {
                throw ExecutorException(IntoErrString(leaf_error));
            }
            throw InternalException("vortex COPY: no leaf statistics at index %llu after finalize", i);
        }
        case_insensitive_map_t<Value> column;
        FillColumnStats(column, leaf.stats);
        string stats_key = IntoErrString(leaf.path);
        bool poisoned = false;
        for (auto &prefix : poisoned_prefixes) {
            if (StringUtil::StartsWith(stats_key, prefix)) {
                poisoned = true;
                break;
            }
        }
        if (poisoned) {
            continue;
        }
        global.written_stats->column_statistics.emplace(std::move(stats_key), std::move(column));
    }
    // Geometry extent stats accumulated while pushing chunks, emitted under the
    // same bbox_*/geo_types keys the parquet writer produces for DuckLake.
    for (auto &entry : global.geo_stats) {
        const auto &stats = entry.second.data;
        const auto &bbox = stats.extent;
        const auto &types = stats.types;
        auto &column = global.written_stats->column_statistics[entry.first];
        if (bbox.HasXY()) {
            column["bbox_xmin"] = Value::DOUBLE(bbox.x_min);
            column["bbox_xmax"] = Value::DOUBLE(bbox.x_max);
            column["bbox_ymin"] = Value::DOUBLE(bbox.y_min);
            column["bbox_ymax"] = Value::DOUBLE(bbox.y_max);
            if (bbox.HasZ()) {
                column["bbox_zmin"] = Value::DOUBLE(bbox.z_min);
                column["bbox_zmax"] = Value::DOUBLE(bbox.z_max);
            }
            if (bbox.HasM()) {
                column["bbox_mmin"] = Value::DOUBLE(bbox.m_min);
                column["bbox_mmax"] = Value::DOUBLE(bbox.m_max);
            }
        }
        if (!types.IsEmpty()) {
            vector<Value> type_strings;
            for (const auto &geo_type : types.ToString(true)) {
                type_strings.push_back(Value(StringUtil::Lower(geo_type)));
            }
            column["geo_types"] = Value::LIST(type_strings);
        }
    }
}

unique_ptr<PreparedBatchData> copy_to_prepare_batch(ClientContext &context,
                                                    FunctionData &bind_data,
                                                    GlobalFunctionData &gstate,
                                                    unique_ptr<ColumnDataCollection> collection) {
    const VortexCopyBindData &bind = bind_data.Cast<VortexCopyBindData>();
    VortexCopyGlobalState &global = gstate.Cast<VortexCopyGlobalState>();

    const void *const ffi_bind = bind.ffi_bind->DataPtr();
    auto ffi_batch = unique_ptr<CData>(reinterpret_cast<CData *>(duckdb_copy_function_prepare_batch_new()));
    duckdb_vx_error error_out = nullptr;

    for (DataChunk &chunk : collection->Chunks()) {
        duckdb_data_chunk ffi_chunk = reinterpret_cast<duckdb_data_chunk>(&chunk);
        duckdb_copy_function_prepare_batch_push(ffi_bind, ffi_batch->DataPtr(), ffi_chunk, &error_out);
        if (error_out) {
            throw ExecutorException(IntoErrString(error_out));
        }
        AccumulateChunkGeometryStats(global, bind.column_names, chunk);
        AccumulateChunkVariantStats(context, global, bind.column_names, bind.column_types, chunk);
    }
    return make_uniq<VortexCopyPreparedBatchData>(std::move(ffi_batch));
}

void copy_to_flush_batch(ClientContext &,
                         FunctionData &,
                         GlobalFunctionData &gstate,
                         PreparedBatchData &batch_data) {
    const VortexCopyGlobalState &global = gstate.Cast<VortexCopyGlobalState>();
    const VortexCopyPreparedBatchData &batch = batch_data.Cast<VortexCopyPreparedBatchData>();

    const void *const ffi_global = global.ffi_global->DataPtr();
    const void *const ffi_batch = batch.ffi_copy_prepared->DataPtr();
    duckdb_vx_error error_out = nullptr;
    duckdb_copy_function_flush_batch(ffi_global, ffi_batch, &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }
}

// DuckLake passes per-file schema identity and the footer encryption key via
// COPY options rather than named function parameters.
void VortexListCopyOptions(ClientContext &, CopyOptionsInput &input) {
    auto &copy_options = input.options;
    copy_options["field_ids"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
    copy_options["encryption_config"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
}

extern "C" duckdb_state duckdb_vx_register_copy_function(duckdb_database ffi_db) {
    D_ASSERT(ffi_db);
    const DatabaseWrapper &wrapper = *reinterpret_cast<DatabaseWrapper *>(ffi_db);
    DatabaseInstance &db = *wrapper.database->instance;

    CopyFunction fn("vortex");
    fn.copy_to_bind = copy_to_bind;
    fn.copy_to_initialize_global = copy_to_initialize_global;
    // required by duckdb
    fn.copy_to_initialize_local = [](auto &, auto &) {
        return make_uniq<LocalFunctionData>();
    };
    fn.copy_to_sink = copy_to_sink;
    // required by duckdb for PARTITION_BY
    fn.copy_to_combine = [](ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
    };
    fn.copy_to_finalize = copy_to_finalize;
    fn.prepare_batch = copy_to_prepare_batch;
    fn.flush_batch = copy_to_flush_batch;
    fn.copy_to_get_written_statistics = copy_to_get_written_statistics;
    fn.file_size_bytes = [](GlobalFunctionData &gstate) -> idx_t {
        auto &global = gstate.Cast<VortexCopyGlobalState>();
        return duckdb_copy_function_file_size_bytes(global.ffi_global->DataPtr());
    };
    fn.extension = "vortex";
    fn.copy_options = VortexListCopyOptions;

    fn.execution_mode = [](bool preserve_insertion_order, bool supports_batch_index) {
        using enum CopyFunctionExecutionMode;
        if (!preserve_insertion_order) {
            return PARALLEL_COPY_TO_FILE;
        }
        if (supports_batch_index) {
            return BATCH_COPY_TO_FILE;
        }
        return REGULAR_COPY_TO_FILE;
    };

    try {
        Catalog &system_catalog = Catalog::GetSystemCatalog(db);
        CatalogTransaction data = CatalogTransaction::GetSystemTransaction(db);
        CreateCopyFunctionInfo copy_info(std::move(fn));
        system_catalog.CreateCopyFunction(data, copy_info);
    } catch (const std::exception &e) {
        ErrorData data(e);
        DUCKDB_LOG_ERROR(db, "Failed to create Vortex copy function:\t" + data.Message());
        return DuckDBError;
    }
    return DuckDBSuccess;
}
