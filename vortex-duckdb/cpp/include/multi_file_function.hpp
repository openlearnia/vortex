// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "table_function.hpp"

namespace duckdb {

struct VortexFileReaderOptions : public BaseFileReaderOptions {
	//! AES-GCM key from named `encryption_config` (ParquetFileScanner / COPY FROM).
	string encryption_key;
};

//! Placeholder bind data — MultiFileGetBindInfo always dereferences interface bind_data.
struct VortexMultiFileBindData : public TableFunctionData {
	string encryption_key;
};

struct VortexMultiFileGlobalState : public GlobalTableFunctionState {
	//! Rust scan GlobalState shared by all Vortex locals of this multi file scan.
	unique_ptr<CData> ffi_global;
	//! First file's Rust BindState; needed to create the per-thread local states.
	const void *ffi_bind = nullptr;
	//! Vortex locals share one thread-safe iterator for the current file.
	idx_t scans_assigned = 0;
	idx_t max_scans = 1;
};

struct VortexMultiFileLocalState : public LocalTableFunctionState {
	//! Per-thread Rust scan LocalState.
	unique_ptr<CData> ffi_local;
	idx_t file_row_offset = 0;
	SelectionVector deletion_sel;
	DataChunk vortex_chunk;
	vector<idx_t> output_to_vortex;
	const BaseFileReader *assigned_reader = nullptr;
	bool prepared = false;
};

class VortexFileReader : public BaseFileReader {
public:
	VortexFileReader(ClientContext &context, OpenFileInfo file_p, string encryption_key = string());

	string GetReaderType() const override {
		return "VORTEX";
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, const string &name) override;
	void AddVirtualColumn(column_t virtual_column_id) override;
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	void PrepareScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                 LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate,
	                 DataChunk &chunk) override;
	void FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) override;

private:
	vector<column_t> BuildVortexColumnIds(VortexMultiFileLocalState &lstate, vector<LogicalType> &types);

	ClientContext &context;
	//! Opened Vortex file (Rust OpenFileReader).
	unique_ptr<CData> ffi_file;
	//! Rust BindState created from the file's footer/schema.
	unique_ptr<CData> ffi_bind;
	idx_t schema_column_count = 0;
	set<idx_t> file_row_number_cols;
};

struct VortexMultiFileInfo : MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext &context);

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<string> &expected_names,
	                     vector<LogicalType> &expected_types) override;
	bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;
	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                MultiFileBindData &bind_data) override;
	optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
	                        FileExpandResult expand_result) override;
	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data,
	                                                           MultiFileGlobalState &global_state) override;
	unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &context,
	                                                         GlobalTableFunctionState &global_state) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        BaseUnionData &union_data, const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        const OpenFileInfo &file, idx_t file_idx,
	                                        const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                        BaseFileReaderOptions &options,
	                                        const MultiFileOptions &file_options) override;
	void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result) override;
	unique_ptr<MultiFileReaderInterface> Copy() override;
	FileGlobInput GetGlobInput() override;
};

duckdb_state register_vortex_multi_file_scan(DatabaseInstance &db);

} // namespace duckdb
