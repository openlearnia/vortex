// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "error.hpp"
#include "data.hpp"
#include "vortex_duckdb.h"
#include "table_function.h"
#include "table_function.hpp"
#include "vortex.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

namespace {

LogicalType FileMetadataStructType() {
	child_list_t<LogicalType> children;
	children.emplace_back("file_name", LogicalType::VARCHAR);
	children.emplace_back("num_rows", LogicalType::BIGINT);
	children.emplace_back("file_size_bytes", LogicalType::UBIGINT);
	children.emplace_back("footer_size", LogicalType::UBIGINT);
	return LogicalType::STRUCT(std::move(children));
}

LogicalType SchemaStructType() {
	child_list_t<LogicalType> children;
	children.emplace_back("name", LogicalType::VARCHAR);
	children.emplace_back("duckdb_type", LogicalType::VARCHAR);
	children.emplace_back("num_children", LogicalType::BIGINT);
	children.emplace_back("field_id", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
}

LogicalType ColumnStatsStructType() {
	child_list_t<LogicalType> children;
	children.emplace_back("column_id", LogicalType::BIGINT);
	children.emplace_back("stats_min", LogicalType::VARCHAR);
	children.emplace_back("stats_max", LogicalType::VARCHAR);
	children.emplace_back("stats_null_count", LogicalType::BIGINT);
	children.emplace_back("stats_num_values", LogicalType::BIGINT);
	children.emplace_back("total_compressed_size", LogicalType::BIGINT);
	children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	return LogicalType::STRUCT(std::move(children));
}

struct VortexFullMetadataBindData : public TableFunctionData {
	shared_ptr<MultiFileList> files;
};

struct VortexFullMetadataGlobalState : public GlobalTableFunctionState {
	explicit VortexFullMetadataGlobalState(shared_ptr<MultiFileList> files_p) : files(std::move(files_p)) {
		files->InitializeScan(scan);
	}

	shared_ptr<MultiFileList> files;
	MultiFileListScanData scan;
	mutex lock;
};

unique_ptr<FunctionData> VortexFullMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto multi_file_reader = MultiFileReader::CreateDefault("VortexFullMetadata");
	auto result = make_uniq<VortexFullMetadataBindData>();
	result->files =
	    multi_file_reader->CreateFileList(context, input.inputs[0], FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "vortex"));

	names = {Identifier("vortex_file_metadata"), Identifier("vortex_schema"), Identifier("vortex_column_stats")};
	return_types = {LogicalType::LIST(FileMetadataStructType()), LogicalType::LIST(SchemaStructType()),
	                LogicalType::LIST(ColumnStatsStructType())};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> VortexFullMetadataInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<VortexFullMetadataBindData>();
	return make_uniq<VortexFullMetadataGlobalState>(bind.files);
}

string StringFromFFI(const char *ptr, size_t len) {
	if (!ptr || len == 0) {
		return string();
	}
	return string(ptr, len);
}

Value BuildFileMetadataValue(const string &path, void *meta) {
	child_list_t<Value> children;
	children.emplace_back("file_name", Value(path));
	children.emplace_back("num_rows", Value::BIGINT(NumericCast<int64_t>(duckdb_vortex_full_metadata_row_count(meta))));
	children.emplace_back("file_size_bytes", Value::UBIGINT(duckdb_vortex_full_metadata_file_size(meta)));
	// Managed Vortex COPY persists footer_size = 0; keep externals consistent.
	children.emplace_back("footer_size", Value::UBIGINT(0));
	vector<Value> list_vals;
	list_vals.push_back(Value::STRUCT(std::move(children)));
	return Value::LIST(FileMetadataStructType(), std::move(list_vals));
}

Value BuildSchemaValue(void *meta) {
	vector<Value> rows;
	const auto count = duckdb_vortex_full_metadata_schema_count(meta);
	for (size_t i = 0; i < count; i++) {
		duckdb_vx_schema_node node;
		if (!duckdb_vortex_full_metadata_schema_at(meta, i, &node)) {
			break;
		}
		child_list_t<Value> children;
		children.emplace_back("name", Value(StringFromFFI(node.name, node.name_len)));
		children.emplace_back("duckdb_type", Value(StringFromFFI(node.duckdb_type, node.duckdb_type_len)));
		children.emplace_back("num_children", Value::BIGINT(NumericCast<int64_t>(node.num_children)));
		children.emplace_back("field_id", Value(LogicalType::BIGINT)); // NULL — name mapping does not need field ids
		rows.push_back(Value::STRUCT(std::move(children)));
	}
	return Value::LIST(SchemaStructType(), std::move(rows));
}

Value BuildColumnStatsValue(void *meta) {
	vector<Value> rows;
	const auto count = duckdb_vortex_full_metadata_stats_count(meta);
	for (size_t i = 0; i < count; i++) {
		duckdb_vx_column_stat stat;
		if (!duckdb_vortex_full_metadata_stat_at(meta, i, &stat)) {
			break;
		}
		child_list_t<Value> children;
		children.emplace_back("column_id", Value::BIGINT(NumericCast<int64_t>(stat.column_id)));
		children.emplace_back("stats_min",
		                      stat.has_stats_min ? Value(StringFromFFI(stat.stats_min, stat.stats_min_len))
		                                         : Value(LogicalType::VARCHAR));
		children.emplace_back("stats_max",
		                      stat.has_stats_max ? Value(StringFromFFI(stat.stats_max, stat.stats_max_len))
		                                         : Value(LogicalType::VARCHAR));
		children.emplace_back("stats_null_count",
		                      stat.has_null_count ? Value::BIGINT(NumericCast<int64_t>(stat.stats_null_count))
		                                          : Value(LogicalType::BIGINT));
		children.emplace_back("stats_num_values",
		                      stat.has_num_values ? Value::BIGINT(NumericCast<int64_t>(stat.stats_num_values))
		                                          : Value(LogicalType::BIGINT));
		children.emplace_back("total_compressed_size",
		                      stat.has_compressed_size
		                          ? Value::BIGINT(NumericCast<int64_t>(stat.total_compressed_size))
		                          : Value(LogicalType::BIGINT));
		children.emplace_back("contains_nan",
		                      stat.has_contains_nan ? Value::BOOLEAN(stat.contains_nan) : Value(LogicalType::BOOLEAN));
		rows.push_back(Value::STRUCT(std::move(children)));
	}
	return Value::LIST(ColumnStatsStructType(), std::move(rows));
}

void VortexFullMetadataExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &global = input.global_state->Cast<VortexFullMetadataGlobalState>();
	idx_t row = 0;
	while (row < STANDARD_VECTOR_SIZE) {
		OpenFileInfo file;
		{
			lock_guard<mutex> guard(global.lock);
			if (!global.files->Scan(global.scan, file)) {
				break;
			}
		}

		duckdb_vx_error error = nullptr;
		auto raw = duckdb_vortex_full_metadata_open(file.path.c_str(), &error);
		if (error) {
			throw InvalidInputException(IntoErrString(error));
		}
		if (!raw) {
			throw InvalidInputException("Failed to open Vortex metadata for file \"%s\"", file.path);
		}
		unique_ptr<CData> metadata(reinterpret_cast<CData *>(raw));
		void *meta = metadata->DataPtr();

		output.data[0].SetValue(row, BuildFileMetadataValue(file.path, meta));
		output.data[1].SetValue(row, BuildSchemaValue(meta));
		output.data[2].SetValue(row, BuildColumnStatsValue(meta));
		row++;
	}
	output.SetCardinalityUnsafe(row);
}

} // namespace

duckdb_state register_vortex_full_metadata(DatabaseInstance &db) {
	try {
		TableFunction fn("vortex_full_metadata", {LogicalType::VARCHAR}, VortexFullMetadataExecute,
		                 VortexFullMetadataBind, VortexFullMetadataInitGlobal);
		auto function_set = MultiFileReader::CreateFunctionSet(std::move(fn));
		auto &system_catalog = Catalog::GetSystemCatalog(db);
		auto data = CatalogTransaction::GetSystemTransaction(db);
		CreateTableFunctionInfo tf_info(std::move(function_set));
		tf_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		system_catalog.CreateFunction(data, tf_info);
	} catch (const std::exception &e) {
		ErrorData data(e);
		DUCKDB_LOG_ERROR(db, "Failed to create vortex_full_metadata:\t" + data.Message());
		return DuckDBError;
	}
	return DuckDBSuccess;
}
