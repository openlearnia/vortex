// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
#pragma once
#include "duckdb/optimizer/type_pushdown.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

using namespace duckdb;

// Push UNGROUPED_AGGREGATE's of form agg(T) and count_star() into GET.
unique_ptr<LogicalOperator> TryPushdownAggregateFunctions(ClientContext &context,
                                                        unique_ptr<LogicalOperator> plan);

unique_ptr<LogicalOperator> RewriteAggregates(ClientContext &context,
                                              unique_ptr<LogicalOperator> op,
                                              Analyses &analyses,
                                              const Projections &projections);

unique_ptr<LogicalOperator> TryReplaceAggregate(ClientContext &context,
                                                unique_ptr<LogicalOperator> op,
                                                Analyses &analyses,
                                                const Projections &projections);

// return GET for UNGROUPED_AGGREGATE -> [GET] or for UNGROUPED_AGGREGATE ->
// PROJECTION -> [GET], nullptr if not found.
LogicalGet *GetChildGet(const LogicalAggregate &agg);
