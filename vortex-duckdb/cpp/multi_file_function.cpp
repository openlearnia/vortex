// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "multi_file_function.hpp"
#include "multi_file_reader.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include "error.hpp"
#include "vortex_duckdb.h"

#include <cstdlib>

extern "C" {
uint8_t *duckdb_vortex_read_ducklake_field_ids(const char *file_path, size_t *len_out,
                                               duckdb_vx_error *error_out);
}

namespace duckdb {

namespace {

static constexpr const char *DUCKDB_FIELD_ID_KEY = "__duckdb_field_id";

//! Build nested MultiFileColumnDefinition children the way Parquet does.
//! DuckDB's CreateFromNameAndType only expands STRUCT; MAP/LIST mapping expects
//! Parquet-shaped children (`element` / `key_value.{key,value}`).
MultiFileColumnDefinition CreateVortexColumn(const string &name, const LogicalType &type) {
	MultiFileColumnDefinition result(name, type);
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		for (auto &child_entry : StructType::GetChildTypes(type)) {
			result.children.push_back(CreateVortexColumn(child_entry.first, child_entry.second));
		}
		break;
	case LogicalTypeId::LIST:
		result.children.push_back(CreateVortexColumn("element", ListType::GetChildType(type)));
		break;
	case LogicalTypeId::ARRAY:
		result.children.push_back(CreateVortexColumn("element", ArrayType::GetChildType(type)));
		break;
	case LogicalTypeId::MAP: {
		child_list_t<LogicalType> key_value;
		key_value.emplace_back("key", MapType::KeyType(type));
		key_value.emplace_back("value", MapType::ValueType(type));
		result.children.push_back(CreateVortexColumn("key_value", LogicalType::STRUCT(std::move(key_value))));
		break;
	}
	default:
		break;
	}
	return result;
}

vector<MultiFileColumnDefinition> ColumnsFromVortexNamesAndTypes(const vector<string> &names,
                                                                 const vector<LogicalType> &types) {
	vector<MultiFileColumnDefinition> columns;
	D_ASSERT(names.size() == types.size());
	for (idx_t i = 0; i < names.size(); i++) {
		columns.push_back(CreateVortexColumn(names[i], types[i]));
	}
	return columns;
}

void ApplyFieldIdValue(MultiFileColumnDefinition &col, const Value &v) {
	if (v.IsNull()) {
		return;
	}
	if (v.type().id() == LogicalTypeId::BIGINT || v.type().id() == LogicalTypeId::INTEGER) {
		col.identifier = Value::INTEGER(v.GetValue<int32_t>());
		return;
	}
	if (v.type().id() != LogicalTypeId::STRUCT) {
		return;
	}
	auto &children = StructValue::GetChildren(v);
	auto &child_types = StructType::GetChildTypes(v.type());
	for (idx_t i = 0; i < children.size(); i++) {
		auto &child_name = child_types[i].first;
		if (StringUtil::CIEquals(child_name, DUCKDB_FIELD_ID_KEY)) {
			col.identifier = Value::INTEGER(children[i].GetValue<int32_t>());
			continue;
		}
		// DuckLake MAP field_ids are keyed as {key, value}, while the Vortex/Parquet
		// local MultiFile schema nests them under a synthetic `key_value` struct.
		if (col.type.id() == LogicalTypeId::MAP && col.children.size() == 1 &&
		    StringUtil::CIEquals(col.children[0].name, "key_value")) {
			for (auto &kv_child : col.children[0].children) {
				if (StringUtil::CIEquals(kv_child.name, child_name)) {
					ApplyFieldIdValue(kv_child, children[i]);
					break;
				}
			}
			continue;
		}
		for (auto &child_col : col.children) {
			if (StringUtil::CIEquals(child_col.name, child_name)) {
				ApplyFieldIdValue(child_col, children[i]);
				break;
			}
		}
	}
}

void ApplyFieldIdsStruct(vector<MultiFileColumnDefinition> &columns, const Value &field_ids) {
	if (field_ids.IsNull() || field_ids.type().id() != LogicalTypeId::STRUCT) {
		return;
	}
	auto &children = StructValue::GetChildren(field_ids);
	auto &names = StructType::GetChildTypes(field_ids.type());
	for (idx_t i = 0; i < children.size(); i++) {
		for (auto &col : columns) {
			if (StringUtil::CIEquals(col.name, names[i].first)) {
				ApplyFieldIdValue(col, children[i]);
				break;
			}
		}
	}
}

void TryApplyDuckLakeFieldIds(const string &path, vector<MultiFileColumnDefinition> &columns) {
	size_t len = 0;
	duckdb_vx_error error_out = nullptr;
	uint8_t *bytes = duckdb_vortex_read_ducklake_field_ids(path.c_str(), &len, &error_out);
	if (error_out) {
		// Legacy files / open failures: fall back to name matching.
		duckdb_vx_error_free(error_out);
		return;
	}
	if (!bytes || len == 0) {
		if (bytes) {
			free(bytes);
		}
		return;
	}
	try {
		MemoryStream stream(data_ptr_cast(bytes), len);
		BinaryDeserializer deserializer(stream);
		deserializer.Begin();
		auto field_ids = Value::Deserialize(deserializer);
		deserializer.End();
		ApplyFieldIdsStruct(columns, field_ids);
	} catch (...) {
		free(bytes);
		throw;
	}
	free(bytes);
}

unique_ptr<CData> OpenVortexFile(const string &path, const string &encryption_key) {
	duckdb_vx_error error = nullptr;
	unique_ptr<CData> ffi_file;
	if (encryption_key.empty()) {
		ffi_file.reset(reinterpret_cast<CData *>(duckdb_reader_open(path.c_str(), path.size(), &error)));
	} else {
		ffi_file.reset(reinterpret_cast<CData *>(duckdb_reader_open_with_key(
		    path.c_str(), path.size(),
		    reinterpret_cast<const uint8_t *>(encryption_key.data()), encryption_key.size(), &error)));
	}
	if (error) {
		throw IOException(IntoErrString(error));
	}
	return ffi_file;
}

void BindVortexFile(ClientContext &context, const string &path, vector<LogicalType> &types,
                    vector<string> &names, const string &encryption_key, unique_ptr<CData> &ffi_file,
                    unique_ptr<CData> &ffi_bind) {
	ffi_file = OpenVortexFile(path, encryption_key);

	// Bind the scan schema from the opened file, the way read_vortex does.
	VortexBindResult result {types, names};
	duckdb_vx_error error = nullptr;
	duckdb_bind_result ffi_result = reinterpret_cast<duckdb_bind_result>(&result);
	const duckdb_vx_data ffi_bind_data = duckdb_reader_bind(ffi_file->DataPtr(), ffi_result, &error);
	if (error) {
		throw BinderException(IntoErrString(error));
	}
	ffi_bind.reset(reinterpret_cast<CData *>(ffi_bind_data));
}

string EncryptionKeyFromOpenFile(const OpenFileInfo &file) {
	if (!file.extended_info) {
		return string();
	}
	auto &open_options = file.extended_info->options;
	auto encryption_entry = open_options.find("encryption_key");
	if (encryption_entry == open_options.end()) {
		return string();
	}
	return StringValue::Get(encryption_entry->second);
}

string EncryptionKeyFromConfigValue(const Value &cfg) {
	if (cfg.IsNull() || cfg.type().id() != LogicalTypeId::STRUCT) {
		return string();
	}
	auto &children = StructValue::GetChildren(cfg);
	auto &child_types = StructType::GetChildTypes(cfg.type());
	for (idx_t i = 0; i < children.size(); i++) {
		if (StringUtil::CIEquals(child_types[i].first, "footer_key_value")) {
			return StringValue::Get(children[i]);
		}
	}
	return string();
}

} // namespace

VortexFileReader::VortexFileReader(ClientContext &context_p, OpenFileInfo file_p, string encryption_key_p)
    : BaseFileReader(std::move(file_p)), context(context_p) {
	vector<LogicalType> types;
	vector<string> names;
	string encryption_key = std::move(encryption_key_p);
	if (encryption_key.empty()) {
		encryption_key = EncryptionKeyFromOpenFile(file);
	}
	BindVortexFile(context, file.path, types, names, encryption_key, ffi_file, ffi_bind);
	schema_column_count = names.size();
	columns = ColumnsFromVortexNamesAndTypes(names, types);
	TryApplyDuckLakeFieldIds(file.path, columns);
}

unique_ptr<BaseStatistics> VortexFileReader::GetStatistics(ClientContext &, const string &name) {
	for (idx_t column_index = 0; column_index < schema_column_count; column_index++) {
		if (columns[column_index].name == name) {
			if (columns[column_index].type.id() == LogicalTypeId::VARIANT) {
				return nullptr;
			}
			duckdb_column_statistics statistics = {};
			if (!duckdb_reader_get_statistics(ffi_file->DataPtr(), ffi_bind->DataPtr(), name.c_str(), name.size(),
			                                  &statistics)) {
				return nullptr;
			}
			return to_duckdb_statistics(statistics);
		}
	}
	return nullptr;
}

void VortexFileReader::AddVirtualColumn(column_t virtual_column_id) {
	if (virtual_column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		// The same source virtual can be requested more than once for derived DuckLake columns.
		file_row_number_cols.insert(columns.size() - 1);
		return;
	}
	throw NotImplementedException("Vortex reader does not support virtual column id %llu", virtual_column_id);
}

vector<column_t> VortexFileReader::BuildVortexColumnIds(VortexMultiFileLocalState &lstate,
                                                        vector<LogicalType> &types) {
	vector<column_t> result;
	map<column_t, idx_t> positions;
	lstate.output_to_vortex.clear();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto local_id = column_ids[MultiFileLocalIndex(i)].GetId();
		column_t vortex_id;
		if (file_row_number_cols.find(local_id) != file_row_number_cols.end()) {
			vortex_id = MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;
		} else if (local_id < schema_column_count) {
			vortex_id = local_id;
		} else {
			throw InternalException("Unexpected Vortex local column id %llu (schema has %llu columns)", local_id,
			                        schema_column_count);
		}
		auto entry = positions.find(vortex_id);
		if (entry == positions.end()) {
			auto position = result.size();
			positions.emplace(vortex_id, position);
			result.push_back(vortex_id);
			types.push_back(columns[local_id].type);
			lstate.output_to_vortex.push_back(position);
		} else {
			lstate.output_to_vortex.push_back(entry->second);
		}
	}
	return result;
}

bool VortexFileReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                         LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<VortexMultiFileGlobalState>();
	auto &lstate = lstate_p.Cast<VortexMultiFileLocalState>();
	if (lstate.assigned_reader == this) {
		return false;
	}
	if (gstate.scans_assigned >= gstate.max_scans && gstate.ffi_global) {
		return false;
	}

	lstate.file_row_offset = 0;
	lstate.output_to_vortex.clear();
	lstate.ffi_local.reset();

	vector<LogicalType> vortex_types;
	auto vortex_column_ids = BuildVortexColumnIds(lstate, vortex_types);
	lstate.vortex_chunk.Destroy();
	lstate.vortex_chunk.Initialize(Allocator::Get(context), vortex_types);
	static const vector<idx_t> empty_projection;
	// When DuckLake injects a deletion filter, physical row positions must stay contiguous.
	// Derived virtual columns likewise need their expressions evaluated before filtering.
	optional_ptr<TableFilterSet> filters_for_vortex =
	    deletion_filter || !expression_map.empty() ? nullptr : filters.get();

	if (!gstate.ffi_global) {
		// First reader of this scan creates the shared Rust global state.
		const duckdb_vx_tfunc_init_input ffi_input = {
		    .bind_data = ffi_bind->DataPtr(),
		    .column_ids = vortex_column_ids.data(),
		    .column_ids_count = vortex_column_ids.size(),
		    .projection_ids = empty_projection.data(),
		    .projection_ids_count = empty_projection.size(),
		    .filters = reinterpret_cast<duckdb_vx_table_filter_set>(filters_for_vortex.get()),
		    .client_context = reinterpret_cast<duckdb_client_context>(&context),
		};
		duckdb_vx_error error = nullptr;
		gstate.ffi_global.reset(reinterpret_cast<CData *>(duckdb_table_function_init_global(&ffi_input, &error)));
		if (error) {
			throw BinderException(IntoErrString(error));
		}
		gstate.ffi_bind = ffi_bind->DataPtr();
		gstate.max_scans =
		    deletion_filter ? 1 : NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	}
	if (!lstate.ffi_local) {
		lstate.ffi_local.reset(reinterpret_cast<CData *>(
		    duckdb_table_function_init_local(gstate.ffi_bind, gstate.ffi_global->DataPtr())));
	}
	lstate.assigned_reader = this;
	lstate.prepared = true;
	// Attach this file to the thread's Rust local state; a false return means
	// the file is pruned (e.g. by footer statistics) and has no work here.
	if (!duckdb_reader_try_initialize_scan(lstate.ffi_local->DataPtr(), ffi_file->DataPtr())) {
		lstate.assigned_reader = nullptr;
		lstate.prepared = false;
		return false;
	}
	gstate.scans_assigned++;
	return true;
}

void VortexFileReader::PrepareScan(ClientContext &, GlobalTableFunctionState &,
                                   LocalTableFunctionState &lstate_p) {
	D_ASSERT(lstate_p.Cast<VortexMultiFileLocalState>().prepared);
}

AsyncResult VortexFileReader::Scan(ClientContext &context_p, GlobalTableFunctionState &gstate_p,
                                   LocalTableFunctionState &lstate_p, DataChunk &chunk) {
	auto &gstate = gstate_p.Cast<VortexMultiFileGlobalState>();
	auto &lstate = lstate_p.Cast<VortexMultiFileLocalState>();
	D_ASSERT(lstate.prepared);
	D_ASSERT(lstate.ffi_local);

	duckdb_vx_error error = nullptr;
	lstate.vortex_chunk.Reset();
	const bool has_more_data =
	    duckdb_reader_scan(ffi_file->DataPtr(), gstate.ffi_global->DataPtr(), lstate.ffi_local->DataPtr(),
	                       reinterpret_cast<duckdb_data_chunk>(&lstate.vortex_chunk), &error);
	if (error) {
		throw InvalidInputException(IntoErrString(error));
	}
	if (lstate.vortex_chunk.size() == 0) {
		return SourceResultType::FINISHED;
	}
	chunk.SetCardinality(lstate.vortex_chunk.size());
	for (idx_t i = 0; i < lstate.output_to_vortex.size(); i++) {
		chunk.data[i].Reference(lstate.vortex_chunk.data[lstate.output_to_vortex[i]]);
	}

	const auto scanned = chunk.size();
	if (deletion_filter || !expression_map.empty()) {
		lstate.deletion_sel.Initialize(nullptr);
		auto kept = deletion_filter
		                ? deletion_filter->Filter(UnsafeNumericCast<row_t>(lstate.file_row_offset), scanned,
		                                          lstate.deletion_sel)
		                : scanned;
		if (filters) {
			for (auto &entry : filters->filters) {
				if (entry.second->filter_type == TableFilterType::OPTIONAL_FILTER) {
					continue;
				}
				auto &vec = chunk.data[entry.first];
				unique_ptr<Vector> evaluated;
				auto filter_vec = &vec;
				const auto local_id = column_ids[MultiFileLocalIndex(entry.first)].GetId();
				auto expression = expression_map.find(local_id);
				if (expression != expression_map.end()) {
					DataChunk expression_input;
					expression_input.Initialize(Allocator::Get(context_p), {vec.GetType()});
					expression_input.data[0].Reference(vec);
					expression_input.SetCardinality(scanned);
					evaluated = make_uniq<Vector>(expression->second->return_type);
					ExpressionExecutor executor(context_p, *expression->second);
					executor.ExecuteExpression(expression_input, *evaluated);
					filter_vec = evaluated.get();
				}
				UnifiedVectorFormat vdata;
				filter_vec->ToUnifiedFormat(scanned, vdata);
				auto &filter = *entry.second;
				auto filter_state = TableFilterState::Initialize(context_p, filter);
				kept = ColumnSegment::FilterSelection(lstate.deletion_sel, *filter_vec, vdata, filter, *filter_state,
				                                      scanned, kept);
			}
		}
		if (kept != scanned) {
			chunk.Slice(lstate.deletion_sel, kept);
		}
	}
	for (idx_t i = 0; i < lstate.output_to_vortex.size(); i++) {
		const auto local_id = column_ids[MultiFileLocalIndex(i)].GetId();
		auto expression = expression_map.find(local_id);
		if (expression == expression_map.end()) {
			continue;
		}
		DataChunk expression_input;
		expression_input.Initialize(Allocator::Get(context_p), {chunk.data[i].GetType()});
		expression_input.data[0].Reference(chunk.data[i]);
		expression_input.SetCardinality(chunk.size());
		Vector evaluated(expression->second->return_type);
		ExpressionExecutor executor(context_p, *expression->second);
		executor.ExecuteExpression(expression_input, evaluated);
		chunk.data[i].Reference(evaluated);
	}
	lstate.file_row_offset += scanned;
	return has_more_data ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

void VortexFileReader::FinishFile(ClientContext &, GlobalTableFunctionState &gstate_p) {
	auto &gstate = gstate_p.Cast<VortexMultiFileGlobalState>();
	gstate.ffi_global.reset();
	gstate.ffi_bind = nullptr;
	gstate.scans_assigned = 0;
	gstate.max_scans = 1;
}

unique_ptr<MultiFileReaderInterface> VortexMultiFileInfo::CreateInterface(ClientContext &) {
	return make_uniq<VortexMultiFileInfo>();
}

unique_ptr<BaseFileReaderOptions> VortexMultiFileInfo::InitializeOptions(ClientContext &,
                                                                         optional_ptr<TableFunctionInfo>) {
	return make_uniq<VortexFileReaderOptions>();
}

bool VortexMultiFileInfo::ParseCopyOption(ClientContext &, const string &, const vector<Value> &,
                                          BaseFileReaderOptions &, vector<string> &, vector<LogicalType> &) {
	return false;
}

bool VortexMultiFileInfo::ParseOption(ClientContext &, const string &key, const Value &val, MultiFileOptions &,
                                      BaseFileReaderOptions &options_p) {
	auto &options = options_p.Cast<VortexFileReaderOptions>();
	auto lkey = StringUtil::Lower(key);
	if (lkey == "encryption_config") {
		options.encryption_key = EncryptionKeyFromConfigValue(val);
		return true;
	}
	return false;
}

unique_ptr<TableFunctionData> VortexMultiFileInfo::InitializeBindData(MultiFileBindData &,
                                                                      unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<VortexMultiFileBindData>();
	if (options_p) {
		result->encryption_key = options_p->Cast<VortexFileReaderOptions>().encryption_key;
	}
	return std::move(result);
}

void VortexMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                     MultiFileBindData &bind_data) {
	auto options = make_uniq<VortexFileReaderOptions>();
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
	    context, return_types, names, *bind_data.file_list, bind_data, *options, bind_data.file_options);
}

optional_idx VortexMultiFileInfo::MaxThreads(const MultiFileBindData &, const MultiFileGlobalState &,
                                             FileExpandResult) {
	return optional_idx();
}

unique_ptr<GlobalTableFunctionState> VortexMultiFileInfo::InitializeGlobalState(ClientContext &, MultiFileBindData &,
                                                                                MultiFileGlobalState &) {
	return make_uniq<VortexMultiFileGlobalState>();
}

unique_ptr<LocalTableFunctionState> VortexMultiFileInfo::InitializeLocalState(ExecutionContext &,
                                                                              GlobalTableFunctionState &) {
	return make_uniq<VortexMultiFileLocalState>();
}

shared_ptr<BaseFileReader> VortexMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &,
                                                             BaseUnionData &union_data, const MultiFileBindData &bind_data) {
	string key;
	if (bind_data.bind_data) {
		key = bind_data.bind_data->Cast<VortexMultiFileBindData>().encryption_key;
	}
	return make_shared_ptr<VortexFileReader>(context, union_data.file, std::move(key));
}

shared_ptr<BaseFileReader> VortexMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &,
                                                             const OpenFileInfo &file, idx_t,
                                                             const MultiFileBindData &bind_data) {
	string key;
	if (bind_data.bind_data) {
		key = bind_data.bind_data->Cast<VortexMultiFileBindData>().encryption_key;
	}
	return make_shared_ptr<VortexFileReader>(context, file, std::move(key));
}

shared_ptr<BaseFileReader> VortexMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                             BaseFileReaderOptions &options_p,
                                                             const MultiFileOptions &) {
	auto &options = options_p.Cast<VortexFileReaderOptions>();
	return make_shared_ptr<VortexFileReader>(context, file, options.encryption_key);
}

void VortexMultiFileInfo::GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &result) {
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
}

unique_ptr<MultiFileReaderInterface> VortexMultiFileInfo::Copy() {
	return make_uniq<VortexMultiFileInfo>();
}

FileGlobInput VortexMultiFileInfo::GetGlobInput() {
	return FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "vortex");
}

duckdb_state register_vortex_multi_file_scan(DatabaseInstance &db) {
	try {
		MultiFileFunction<VortexMultiFileInfo> table_function("vortex_multi_file_scan");
		table_function.filter_pushdown = true;
		table_function.named_parameters["encryption_config"] = LogicalTypeId::ANY;
		table_function.filter_prune = true;
		auto function_set = MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(table_function));

		auto &system_catalog = Catalog::GetSystemCatalog(db);
		auto data = CatalogTransaction::GetSystemTransaction(db);
		CreateTableFunctionInfo tf_info(std::move(function_set));
		tf_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		system_catalog.CreateFunction(data, tf_info);
	} catch (const std::exception &e) {
		ErrorData data(e);
		DUCKDB_LOG_ERROR(db, "Failed to create vortex_multi_file_scan:\t" + data.Message());
		return DuckDBError;
	}
	return DuckDBSuccess;
}

} // namespace duckdb
