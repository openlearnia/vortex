// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
#include "data.hpp"
#include "error.hpp"
#include "vortex_duckdb.h"
#include "table_function.h"
#include "vortex.h"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"

#include <array>
#include <cstdlib>
#include <optional>

using namespace duckdb;

struct VariantScalarStats {
    idx_t count = 0;
    bool compatible = true;
    bool has_min_max = false;
    Value min;
    Value max;

    void Add(Value value) {
        count++;
        if (!has_min_max) {
            min = value;
            max = std::move(value);
            has_min_max = true;
            return;
        }
        if (value.type() != min.type()) {
            compatible = false;
            return;
        }
        if (ValueOperations::LessThan(value, min)) {
            min = value;
        }
        if (ValueOperations::GreaterThan(value, max)) {
            max = std::move(value);
        }
    }
};

struct VariantNodeStats {
    static constexpr auto TYPE_COUNT = static_cast<uint8_t>(VariantLogicalType::ENUM_SIZE);

    idx_t total_count = 0;
    idx_t empty_array_count = 0;
    VariantLogicalType current_type = VariantLogicalType::VARIANT_NULL;
    array<idx_t, TYPE_COUNT> type_counts = {};
    array<VariantScalarStats, TYPE_COUNT> scalar_stats;
    map<string, VariantNodeStats> fields;
    unique_ptr<VariantNodeStats> element;

    void ObserveType(VariantLogicalType type) {
        // DuckDB variants encode boolean values as two logical tags, but Parquet shreds both as BOOLEAN.
        current_type = type == VariantLogicalType::BOOL_FALSE ? VariantLogicalType::BOOL_TRUE : type;
        type_counts[static_cast<uint8_t>(current_type)]++;
        total_count++;
    }

    void AddScalar(Value value) {
        scalar_stats[static_cast<uint8_t>(current_type)].Add(std::move(value));
    }
};

struct VariantColumnStats {
    idx_t row_count = 0;
    idx_t sql_null_count = 0;
    VariantNodeStats root;
};

struct VariantStatsVisitor {
    using result_type = void;

    static void VisitNull(VariantNodeStats &) {
    }
    static void VisitMetadata(VariantLogicalType type, VariantNodeStats &stats) {
        stats.ObserveType(type);
    }
    static void VisitBoolean(bool value, VariantNodeStats &stats) {
        stats.AddScalar(Value::BOOLEAN(value));
    }
    template <typename T>
    static void VisitInteger(T value, VariantNodeStats &stats) {
        stats.AddScalar(Value::CreateValue(value));
    }
    static void VisitFloat(float value, VariantNodeStats &stats) {
        stats.AddScalar(Value::FLOAT(value));
    }
    static void VisitDouble(double value, VariantNodeStats &stats) {
        stats.AddScalar(Value::DOUBLE(value));
    }
    static void VisitUUID(hugeint_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::UUID(value));
    }
    static void VisitDate(date_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::DATE(value));
    }
    static void VisitInterval(interval_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::INTERVAL(value));
    }
    static void VisitTime(dtime_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIME(value));
    }
    static void VisitTimeNanos(dtime_ns_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIME_NS(value));
    }
    static void VisitTimeTZ(dtime_tz_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIMETZ(value));
    }
    static void VisitTimestampSec(timestamp_sec_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIMESTAMPSEC(value));
    }
    static void VisitTimestampMs(timestamp_ms_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIMESTAMPMS(value));
    }
    static void VisitTimestamp(timestamp_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIMESTAMP(value));
    }
    static void VisitTimestampNanos(timestamp_ns_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIMESTAMPNS(value));
    }
    static void VisitTimestampTZ(timestamp_tz_t value, VariantNodeStats &stats) {
        stats.AddScalar(Value::TIMESTAMPTZ(value));
    }
    static void VisitString(const string_t &value, VariantNodeStats &stats) {
        stats.AddScalar(Value(value.GetString()));
    }
    static void VisitBlob(const string_t &value, VariantNodeStats &stats) {
        stats.AddScalar(Value::BLOB(const_data_ptr_cast(value.GetData()), value.GetSize()));
    }
    static void VisitBignum(const string_t &value, VariantNodeStats &stats) {
        stats.AddScalar(Value::BIGNUM(const_data_ptr_cast(value.GetData()), value.GetSize()));
    }
    static void VisitGeometry(const string_t &value, VariantNodeStats &stats) {
        stats.AddScalar(Value::GEOMETRY(const_data_ptr_cast(value.GetData()), value.GetSize()));
    }
    static void VisitBitstring(const string_t &value, VariantNodeStats &stats) {
        stats.AddScalar(Value::BIT(const_data_ptr_cast(value.GetData()), value.GetSize()));
    }
    template <typename T>
    static void VisitDecimal(T value, uint32_t width, uint32_t scale, VariantNodeStats &stats) {
        stats.AddScalar(Value::DECIMAL(value, static_cast<uint8_t>(width), static_cast<uint8_t>(scale)));
    }
    static void VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
                           VariantNodeStats &stats) {
        if (!stats.element) {
            stats.element = make_uniq<VariantNodeStats>();
        }
        if (nested_data.child_count == 0) {
            stats.empty_array_count++;
        }
        VariantVisitor<VariantStatsVisitor>::VisitArrayItems(variant, row, nested_data, *stats.element);
    }
    static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
                            VariantNodeStats &stats) {
        for (idx_t i = 0; i < nested_data.child_count; i++) {
            const auto source_index = nested_data.children_idx + i;
            const auto key_index = variant.GetKeysIndex(row, source_index);
            const auto value_index = variant.GetValuesIndex(row, source_index);
            auto &child = stats.fields[variant.GetKey(row, key_index).GetString()];
            VariantVisitor<VariantStatsVisitor>::Visit(variant, row, value_index, child);
        }
    }
    static void VisitDefault(VariantLogicalType type, const_data_ptr_t, VariantNodeStats &) {
        throw NotImplementedException("Vortex VARIANT stats do not support VariantLogicalType::%s",
                                      EnumUtil::ToString(type));
    }
};

struct CopyBindData final : TableFunctionData {
    CopyBindData(unique_ptr<CData> ffi_data, vector<string> column_names, vector<LogicalType> column_types)
        : ffi_data(std::move(ffi_data)), column_names(std::move(column_names)), column_types(std::move(column_types)) {
    }
    unique_ptr<CData> ffi_data;
    vector<string> column_names;
    vector<LogicalType> column_types;
    //! Optional DuckLake field_ids struct — serialized into Vortex `ducklake.field_ids` metadata.
    Value field_ids;
    //! Optional AES-GCM key (16 or 32 raw bytes) from encryption_config.footer_key_value.
    string encryption_key;
};

struct CopyGlobalData final : GlobalFunctionData {
    CopyGlobalData(unique_ptr<CData> ffi_data) : ffi_data(std::move(ffi_data)) {
    }

    unique_ptr<CData> ffi_data;
    optional_ptr<CopyFunctionFileStatistics> written_statistics;
    //! Approximate uncompressed bytes seen by the writer; used for target-size rotation.
    // ponytail: ceiling = in-memory chunk allocation size, not compressed file bytes;
    // upgrade = Writer::bytes_written() once the stream path exposes it.
    idx_t bytes_written = 0;
    case_insensitive_map_t<GeometryStatsData> geometry_stats;
    case_insensitive_map_t<VariantColumnStats> variant_stats;
};

static string JoinStatsPath(const string &prefix, const string &name) {
    const auto segment = KeywordHelper::WriteQuoted(name, '"');
    return prefix.empty() ? segment : prefix + "." + segment;
}

static bool ContainsGeometry(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::GEOMETRY:
        return true;
    case LogicalTypeId::STRUCT:
        for (const auto &child : StructType::GetChildTypes(type)) {
            if (ContainsGeometry(child.second)) {
                return true;
            }
        }
        return false;
    case LogicalTypeId::LIST:
        return ContainsGeometry(ListType::GetChildType(type));
    case LogicalTypeId::ARRAY:
        return ContainsGeometry(ArrayType::GetChildType(type));
    case LogicalTypeId::MAP:
        return ContainsGeometry(MapType::KeyType(type)) || ContainsGeometry(MapType::ValueType(type));
    default:
        return false;
    }
}

static void AccumulateGeometryStats(const LogicalType &type, const Value &value, const string &path,
                                    case_insensitive_map_t<GeometryStatsData> &stats) {
    if (value.IsNull()) {
        return;
    }
    switch (type.id()) {
    case LogicalTypeId::GEOMETRY: {
        auto entry = stats.find(path);
        if (entry == stats.end()) {
            GeometryStatsData data;
            data.SetEmpty();
            entry = stats.emplace(path, std::move(data)).first;
        }
        entry->second.Update(StringValue::Get(value));
        return;
    }
    case LogicalTypeId::STRUCT: {
        const auto &types = StructType::GetChildTypes(type);
        const auto &children = StructValue::GetChildren(value);
        for (idx_t i = 0; i < children.size(); i++) {
            AccumulateGeometryStats(types[i].second, children[i], JoinStatsPath(path, types[i].first), stats);
        }
        return;
    }
    case LogicalTypeId::LIST: {
        const auto &child_type = ListType::GetChildType(type);
        const auto child_path = JoinStatsPath(path, "element");
        for (const auto &child : ListValue::GetChildren(value)) {
            AccumulateGeometryStats(child_type, child, child_path, stats);
        }
        return;
    }
    case LogicalTypeId::ARRAY: {
        const auto &child_type = ArrayType::GetChildType(type);
        const auto child_path = JoinStatsPath(path, "element");
        for (const auto &child : ArrayValue::GetChildren(value)) {
            AccumulateGeometryStats(child_type, child, child_path, stats);
        }
        return;
    }
    case LogicalTypeId::MAP: {
        const auto &key_type = MapType::KeyType(type);
        const auto &value_type = MapType::ValueType(type);
        const auto key_path = JoinStatsPath(path, "key");
        const auto value_path = JoinStatsPath(path, "value");
        for (const auto &entry : MapValue::GetChildren(value)) {
            const auto &children = StructValue::GetChildren(entry);
            AccumulateGeometryStats(key_type, children[0], key_path, stats);
            AccumulateGeometryStats(value_type, children[1], value_path, stats);
        }
        return;
    }
    default:
        return;
    }
}

static LogicalType VariantPrimitiveType(VariantLogicalType type, const VariantScalarStats &stats) {
    switch (type) {
    case VariantLogicalType::BOOL_TRUE:
        return LogicalType::BOOLEAN;
    case VariantLogicalType::INT8:
        return LogicalType::TINYINT;
    case VariantLogicalType::INT16:
        return LogicalType::SMALLINT;
    case VariantLogicalType::INT32:
        return LogicalType::INTEGER;
    case VariantLogicalType::INT64:
        return LogicalType::BIGINT;
    case VariantLogicalType::UINT8:
        return LogicalType::SMALLINT;
    case VariantLogicalType::UINT16:
        return LogicalType::INTEGER;
    case VariantLogicalType::UINT32:
    case VariantLogicalType::UINT64:
    case VariantLogicalType::UINT128:
    case VariantLogicalType::INT128:
        return LogicalType::BIGINT;
    case VariantLogicalType::FLOAT:
        return LogicalType::FLOAT;
    case VariantLogicalType::DOUBLE:
        return LogicalType::DOUBLE;
    case VariantLogicalType::DECIMAL:
        return stats.has_min_max ? stats.min.type() : LogicalType::INTEGER;
    case VariantLogicalType::VARCHAR:
        return LogicalType::VARCHAR;
    case VariantLogicalType::BLOB:
        return LogicalType::BLOB;
    case VariantLogicalType::UUID:
        return LogicalType::UUID;
    case VariantLogicalType::DATE:
        return LogicalType::DATE;
    case VariantLogicalType::TIME_MICROS:
        return LogicalType::TIME;
    case VariantLogicalType::TIME_NANOS:
        return LogicalType::TIME_NS;
    case VariantLogicalType::TIME_MICROS_TZ:
        return LogicalType::TIME_TZ;
    case VariantLogicalType::TIMESTAMP_SEC:
        return LogicalType::TIMESTAMP_S;
    case VariantLogicalType::TIMESTAMP_MILIS:
        return LogicalType::TIMESTAMP_MS;
    case VariantLogicalType::TIMESTAMP_MICROS:
        return LogicalType::TIMESTAMP;
    case VariantLogicalType::TIMESTAMP_NANOS:
        return LogicalType::TIMESTAMP_NS;
    case VariantLogicalType::TIMESTAMP_MICROS_TZ:
        return LogicalType::TIMESTAMP_TZ;
    case VariantLogicalType::INTERVAL:
        return LogicalType::INTERVAL;
    case VariantLogicalType::BIGNUM:
        return LogicalType::BIGNUM;
    case VariantLogicalType::BITSTRING:
        return LogicalType::BIT;
    case VariantLogicalType::GEOMETRY:
        return LogicalType::GEOMETRY();
    default:
        throw NotImplementedException("Cannot synthesize a shredded type for VariantLogicalType::%s",
                                      EnumUtil::ToString(type));
    }
}

static std::optional<VariantLogicalType> SelectVariantType(const VariantNodeStats &stats) {
    idx_t max_count = 0;
    std::optional<VariantLogicalType> selected;
    for (uint8_t i = 1; i < VariantNodeStats::TYPE_COUNT; i++) {
        const auto type = static_cast<VariantLogicalType>(i);
        if (type == VariantLogicalType::BOOL_FALSE) {
            continue;
        }
        const auto count = stats.type_counts[i];
        if (count > max_count) {
            max_count = count;
            selected = type;
        }
    }
    if (!selected && stats.type_counts[static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL)] > 0) {
        return VariantLogicalType::INT32;
    }
    return selected;
}

struct SynthesizedVariantStats {
    std::optional<LogicalType> typed_type;
    case_insensitive_map_t<case_insensitive_map_t<Value>> columns;
};

static string VariantStatsString(const Value &value) {
    if (value.type().id() == LogicalTypeId::BOOLEAN) {
        return value.GetValue<bool>() ? "1" : "0";
    }
    return value.ToString();
}

static case_insensitive_map_t<Value> CountStats(idx_t null_count, idx_t num_values) {
    case_insensitive_map_t<Value> result;
    result["null_count"] = Value(to_string(null_count));
    result["num_values"] = Value(to_string(num_values));
    return result;
}

static std::optional<LogicalType> SynthesizeVariantNode(const VariantNodeStats &stats, idx_t definition_count,
                                                        const string &entry_path, SynthesizedVariantStats &result) {
    auto selected = SelectVariantType(stats);
    if (!selected) {
        return std::optional<LogicalType>();
    }
    const auto type = *selected;
    const auto selected_count = stats.type_counts[static_cast<uint8_t>(type)];

    if (type == VariantLogicalType::OBJECT) {
        child_list_t<LogicalType> fields;
        for (const auto &entry : stats.fields) {
            const auto child_path = JoinStatsPath(JoinStatsPath(entry_path, "typed_value"), entry.first);
            auto child_type = SynthesizeVariantNode(entry.second, definition_count, child_path, result);
            if (child_type) {
                fields.emplace_back(entry.first,
                                    LogicalType::STRUCT({{"value", LogicalType::BLOB},
                                                         {"typed_value", std::move(*child_type)}}));
            }
        }
        if (fields.empty()) {
            return std::optional<LogicalType>();
        }
        return LogicalType::STRUCT(std::move(fields));
    }
    if (type == VariantLogicalType::ARRAY) {
        if (!stats.element) {
            return std::optional<LogicalType>();
        }
        const auto element_definitions =
            definition_count - selected_count + stats.element->total_count + stats.empty_array_count;
        const auto element_path = JoinStatsPath(JoinStatsPath(entry_path, "typed_value"), "element");
        auto element_type = SynthesizeVariantNode(*stats.element, element_definitions, element_path, result);
        if (!element_type) {
            return std::optional<LogicalType>();
        }
        return LogicalType::LIST(LogicalType::STRUCT(
            {{"value", LogicalType::BLOB}, {"typed_value", std::move(*element_type)}}));
    }

    const auto null_count = stats.type_counts[static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL)];
    const auto &scalar = stats.scalar_stats[static_cast<uint8_t>(type)];
    const auto typed_type = VariantPrimitiveType(type, scalar);
    if (!scalar.compatible || selected_count + null_count != stats.total_count) {
        return typed_type;
    }

    auto typed_stats = CountStats(definition_count - selected_count, definition_count);
    if (scalar.has_min_max) {
        typed_stats["min"] = Value(VariantStatsString(scalar.min));
        typed_stats["max"] = Value(VariantStatsString(scalar.max));
    }
    result.columns[JoinStatsPath(entry_path, "typed_value")] = std::move(typed_stats);
    result.columns[JoinStatsPath(entry_path, "value")] = CountStats(definition_count, definition_count);
    return typed_type;
}

static SynthesizedVariantStats SynthesizeVariantColumn(const string &path, const VariantColumnStats &stats) {
    SynthesizedVariantStats result;
    result.typed_type = SynthesizeVariantNode(stats.root, stats.row_count, path, result);
    if (!result.typed_type) {
        return result;
    }

    const auto selected = SelectVariantType(stats.root);
    const auto selected_count =
        selected ? stats.root.type_counts[static_cast<uint8_t>(*selected)] : static_cast<idx_t>(0);
    const auto encoded_nulls =
        stats.root.type_counts[static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL)];
    result.columns[JoinStatsPath(path, "value")] =
        CountStats(stats.sql_null_count + encoded_nulls + selected_count, stats.row_count);

    auto metadata_stats = CountStats(0, stats.row_count);
    const auto variant_type =
        LogicalType::STRUCT({{"metadata", LogicalType::BLOB},
                             {"value", LogicalType::BLOB},
                             {"typed_value", result.typed_type.value()}})
            .ToString();
    metadata_stats["variant_type"] = Value(variant_type);
    result.columns[JoinStatsPath(path, "metadata")] = std::move(metadata_stats);
    return result;
}

void VortexListCopyOptions(ClientContext &, CopyOptionsInput &input) {
    auto &copy_options = input.options;
    copy_options["field_ids"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
    copy_options["encryption_config"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
}

unique_ptr<FunctionData> copy_to_bind(ClientContext &,
                                      CopyFunctionBindInput &input,
                                      const vector<string> &column_names,
                                      const vector<LogicalType> &column_types) {
    Value field_ids;
    string encryption_key;
    for (auto &opt : input.info.options) {
        auto name = StringUtil::Lower(opt.first);
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
                if (StringUtil::CIEquals(child_types[i].first, "footer_key_value")) {
                    encryption_key = StringValue::Get(children[i]);
                }
            }
            if (encryption_key.empty()) {
                throw BinderException("Vortex encryption_config requires footer_key_value");
            }
            continue;
        }
        throw NotImplementedException("Unsupported Vortex COPY option \"%s\"", opt.first);
    }

    vector<const char *> ffi_column_names(column_names.size());
    for (size_t i = 0; i < column_names.size(); ++i) {
        ffi_column_names[i] = column_names[i].c_str();
    }

    vector<duckdb_logical_type> ffi_column_types(column_types.size());
    for (size_t i = 0; i < column_types.size(); ++i) {
        // duckdb C api doesn't allow passing const LogicalTypes. We never
        // modify input in copy function.
        ffi_column_types[i] =
            reinterpret_cast<duckdb_logical_type>(const_cast<LogicalType *>(&column_types[i]));
    }

    duckdb_vx_error error_out = nullptr;
    const duckdb_vx_data ffi_bind_data = duckdb_copy_function_copy_to_bind(ffi_column_names.data(),
                                                                           ffi_column_names.size(),
                                                                           ffi_column_types.data(),
                                                                           ffi_column_types.size(),
                                                                           &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }
    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_bind_data));
    auto result = make_uniq<CopyBindData>(std::move(cdata), column_names, column_types);
    result->field_ids = std::move(field_ids);
    result->encryption_key = std::move(encryption_key);
    return std::move(result);
}

unique_ptr<GlobalFunctionData>
copy_to_initialize_global(ClientContext &, FunctionData &bind_data, const string &file_path) {
    auto &bind = bind_data.Cast<CopyBindData>();
    void *const ffi_bind = bind.ffi_data->DataPtr();

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
    return make_uniq<CopyGlobalData>(std::move(cdata));
}

void copy_to_get_written_statistics(ClientContext &, FunctionData &, GlobalFunctionData &gstate,
                                    CopyFunctionFileStatistics &statistics) {
    gstate.Cast<CopyGlobalData>().written_statistics = statistics;
}

void copy_to_sink(ExecutionContext &,
                  FunctionData &bind_data,
                  GlobalFunctionData &gstate,
                  LocalFunctionData &,
                  DataChunk &input) {
    void *const ffi_bind = bind_data.Cast<CopyBindData>().ffi_data->DataPtr();
    auto &global = gstate.Cast<CopyGlobalData>();
    void *const ffi_global = global.ffi_data->DataPtr();
    auto ffi_chunk = reinterpret_cast<duckdb_data_chunk>(&input);
    duckdb_vx_error error_out = nullptr;
    duckdb_copy_function_copy_to_sink(ffi_bind, ffi_global, ffi_chunk, &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }
    global.bytes_written += input.GetAllocationSize();

    const auto &bind = bind_data.Cast<CopyBindData>();
    // ponytail: Value traversal favors one small generic path over a second geometry vector walker;
    // upgrade if geometry-heavy COPY profiles show materialization dominating writer time.
    for (idx_t column_idx = 0; column_idx < input.ColumnCount(); column_idx++) {
        const auto path = KeywordHelper::WriteQuoted(bind.column_names[column_idx], '"');
        if (ContainsGeometry(bind.column_types[column_idx])) {
            for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
                AccumulateGeometryStats(bind.column_types[column_idx], input.GetValue(column_idx, row_idx), path,
                                        global.geometry_stats);
            }
        }
        if (bind.column_types[column_idx].id() == LogicalTypeId::VARIANT) {
            auto &stats = global.variant_stats[path];
            stats.row_count += input.size();
            RecursiveUnifiedVectorFormat recursive_format;
            Vector::RecursiveToUnifiedFormat(input.data[column_idx], input.size(), recursive_format);
            UnifiedVariantVectorData variant(recursive_format);
            for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
                if (!variant.RowIsValid(row_idx)) {
                    stats.sql_null_count++;
                    continue;
                }
                VariantVisitor<VariantStatsVisitor>::Visit(variant, row_idx, 0, stats.root);
            }
        }
    }
}

void copy_to_combine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
    // Vortex writers are single-file and not batched; DuckDB may still call combine for
    // partitioned COPY. No-op is enough for the DuckLake MVP.
}

bool copy_rotate_files(FunctionData &, const optional_idx &file_size_bytes) {
    return file_size_bytes.IsValid();
}

bool copy_rotate_next_file(GlobalFunctionData &gstate, FunctionData &, const optional_idx &file_size_bytes) {
    if (!file_size_bytes.IsValid()) {
        return false;
    }
    return gstate.Cast<CopyGlobalData>().bytes_written >= file_size_bytes.GetIndex();
}

void copy_to_finalize(ClientContext &, FunctionData &, GlobalFunctionData &gstate) {
    auto &global = gstate.Cast<CopyGlobalData>();
    void *const ffi_global = global.ffi_data->DataPtr();
    duckdb_vx_error error_out = nullptr;
    uint64_t row_count = 0;
    uint64_t file_size = 0;
    duckdb_copy_function_copy_to_finalize(ffi_global, &row_count, &file_size, &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }
    if (global.written_statistics) {
        global.written_statistics->row_count = static_cast<idx_t>(row_count);
        global.written_statistics->file_size_bytes = static_cast<idx_t>(file_size);
        // Vortex WriteSummary has no discrete footer-size field; DuckLake requires non-null.
        global.written_statistics->footer_size_bytes = Value::UBIGINT(0);

        const auto stats_count = duckdb_copy_function_exported_stats_count(ffi_global);
        for (uint64_t i = 0; i < stats_count; i++) {
            char *name = nullptr;
            char *min_str = nullptr;
            char *max_str = nullptr;
            uint64_t null_count = 0;
            uint64_t num_values = 0;
            uint64_t column_size = 0;
            bool has_null_count = false;
            bool has_num_values = false;
            bool has_column_size = false;
            bool has_nan = false;
            bool has_has_nan = false;
            if (!duckdb_copy_function_exported_stat_at(ffi_global, i, &name, &null_count, &has_null_count,
                                                       &num_values, &has_num_values, &column_size, &has_column_size,
                                                       &min_str, &max_str, &has_nan, &has_has_nan)) {
                continue;
            }
            case_insensitive_map_t<Value> col_stats;
            if (has_null_count) {
                col_stats["null_count"] = Value(to_string(null_count));
            }
            if (has_num_values) {
                col_stats["num_values"] = Value(to_string(num_values));
            }
            if (has_column_size) {
                col_stats["column_size_bytes"] = Value(to_string(column_size));
            }
            if (min_str) {
                col_stats["min"] = Value(min_str);
            }
            if (max_str) {
                col_stats["max"] = Value(max_str);
            }
            if (has_has_nan) {
                col_stats["has_nan"] = Value(has_nan ? "true" : "false");
            }
            if (name) {
                // Nested leaves arrive pre-quoted as "col"."element"; top-level names are bare.
                string key = name;
                if (key.empty() || key.front() != '"') {
                    key = KeywordHelper::WriteQuoted(name, '"');
                }
                global.written_statistics->column_statistics[key] = std::move(col_stats);
                free(name);
            }
            if (min_str) {
                free(min_str);
            }
            if (max_str) {
                free(max_str);
            }
        }

        for (const auto &entry : global.geometry_stats) {
            auto &col_stats = global.written_statistics->column_statistics[entry.first];
            const auto &bbox = entry.second.extent;
            if (bbox.HasXY()) {
                col_stats["bbox_xmin"] = Value::DOUBLE(bbox.x_min);
                col_stats["bbox_xmax"] = Value::DOUBLE(bbox.x_max);
                col_stats["bbox_ymin"] = Value::DOUBLE(bbox.y_min);
                col_stats["bbox_ymax"] = Value::DOUBLE(bbox.y_max);
                if (bbox.HasZ()) {
                    col_stats["bbox_zmin"] = Value::DOUBLE(bbox.z_min);
                    col_stats["bbox_zmax"] = Value::DOUBLE(bbox.z_max);
                }
                if (bbox.HasM()) {
                    col_stats["bbox_mmin"] = Value::DOUBLE(bbox.m_min);
                    col_stats["bbox_mmax"] = Value::DOUBLE(bbox.m_max);
                }
            }
            if (!entry.second.types.IsEmpty()) {
                vector<Value> type_values;
                for (const auto &type_name : entry.second.types.ToString(true)) {
                    type_values.emplace_back(type_name);
                }
                col_stats["geo_types"] = Value::LIST(std::move(type_values));
            }
        }
        for (const auto &entry : global.variant_stats) {
            auto synthesized = SynthesizeVariantColumn(entry.first, entry.second);
            for (auto &column : synthesized.columns) {
                global.written_statistics->column_statistics[column.first] = std::move(column.second);
            }
        }
    }
}

extern "C" duckdb_state duckdb_vx_register_copy_function(duckdb_database ffi_db) {
    D_ASSERT(ffi_db);
    const DatabaseWrapper &wrapper = *reinterpret_cast<DatabaseWrapper *>(ffi_db);
    DatabaseInstance &db = *wrapper.database->instance;

    CopyFunction fn("vortex");
    fn.copy_to_bind = copy_to_bind;
    fn.copy_to_initialize_global = copy_to_initialize_global;
    fn.copy_to_initialize_local = [](auto &, auto &) {
        return make_uniq<LocalFunctionData>();
    };
    fn.copy_to_get_written_statistics = copy_to_get_written_statistics;
    fn.copy_to_sink = copy_to_sink;
    fn.copy_to_combine = copy_to_combine;
    fn.rotate_files = copy_rotate_files;
    fn.rotate_next_file = copy_rotate_next_file;
    fn.copy_to_finalize = copy_to_finalize;
    fn.extension = "vortex";
    fn.copy_options = VortexListCopyOptions;

    // TODO(joe): expose this via c our api
    fn.execution_mode = [](bool, bool) {
        return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
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
