// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
#include "aggregate_fn_pushdown.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "table_function.hpp"

using enum LogicalOperatorType;

unique_ptr<LogicalOperator> TryPushdownAggregateFunctions(ClientContext &context,
                                                        unique_ptr<LogicalOperator> plan) {
    Analyses analyses;
    Projections projections;
    FindGetsAndProjections(*plan, analyses, projections);
    if (analyses.empty()) {
        return plan;
    }
    return RewriteAggregates(context, std::move(plan), analyses, projections);
}

unique_ptr<LogicalOperator> RewriteAggregates(ClientContext &context,
                                              unique_ptr<LogicalOperator> op,
                                              Analyses &analyses,
                                              const Projections &projections) {
    for (auto &child : op->children) {
        child = RewriteAggregates(context, std::move(child), analyses, projections);
    }
    if (op->type == LOGICAL_AGGREGATE_AND_GROUP_BY) {
        return TryReplaceAggregate(context, std::move(op), analyses, projections);
    }
    return op;
}

static bool IsUngrouped(const LogicalAggregate &agg) {
    return agg.groups.empty() && agg.grouping_sets.empty() && agg.grouping_functions.empty() &&
           !agg.expressions.empty();
}



unique_ptr<LogicalOperator> TryReplaceAggregate(ClientContext &context,
                                                unique_ptr<LogicalOperator> op,
                                                Analyses &analyses,
                                                const Projections &projections) {
    LogicalAggregate &agg = op->Cast<LogicalAggregate>();
    if (!IsUngrouped(agg)) {
        return op;
    }

    LogicalGet *const get = GetChildGet(agg);
    if (get == nullptr) {
        return op;
    }

    vector<std::pair<ProjectionIndex, const Expression &>> input;
    const idx_t aggregates_len = agg.expressions.size();
    input.reserve(aggregates_len);

    for (const auto &expr : agg.expressions) {
        if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
            return op;
        }
        const auto &bound_aggr = expr->Cast<BoundAggregateExpression>();
        if (bound_aggr.IsDistinct() || bound_aggr.GetFilter() != nullptr || bound_aggr.GetOrderBys() != nullptr) {
            return op;
        }

        if (bound_aggr.Function().GetName() == "count_star") {
            input.emplace_back(COUNT_STAR_PROJ_IDX, *expr);
            continue;
        }

        if (bound_aggr.GetChildren().size() != 1 ||
            bound_aggr.GetChildren()[0]->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
            return op;
        }
        const auto &bound_col = bound_aggr.GetChildren()[0]->Cast<BoundColumnRefExpression>();
        const auto binding = Resolve(bound_col.Binding(), analyses, projections);
        if (!binding || &binding->analysis.get != get) {
            return op;
        }
        input.emplace_back(binding->column_index, *expr);
    }

    if (!aggregate_pushdown(context, {*get, input})) {
        return op;
    }

    vector<Identifier> names(aggregates_len); // need a copy because we reference original names

    const vector<ColumnIndex> original_column_ids = get->GetColumnIds();
    for (idx_t i = 0; i < aggregates_len; i++) {
        const auto &[column_index, expr] = input[i];
        if (column_index == COUNT_STAR_PROJ_IDX) {
            names[i] = "count_star()";
        } else {
            const idx_t storage_index = original_column_ids[column_index].GetPrimaryIndex();
            names[i] = get->names[storage_index];
        }
    }

    // GET now returns one column per aggregate.
    auto &column_ids = get->GetMutableColumnIds();
    get->types.resize(aggregates_len);
    get->returned_types.resize(aggregates_len);
    column_ids.resize(aggregates_len);
    for (idx_t i = 0; i < aggregates_len; i++) {
        const auto &[_, expr] = input[i];
        get->types[i] = expr.GetReturnType();
        get->returned_types[i] = expr.GetReturnType();
        column_ids[i] = ColumnIndex {i};
    }
    get->names = std::move(names);
    get->projection_ids.clear();
    get->table_index = agg.aggregate_index;

    unique_ptr<LogicalOperator> &child = agg.children[0];
    if (child->type == LOGICAL_GET) {
        return std::move(child);
    }
    D_ASSERT(child->type == LOGICAL_PROJECTION);
    D_ASSERT(child->children.size() == 1);
    D_ASSERT(child->children[0]->type == LOGICAL_GET);
    return std::move(child->children[0]);
}

LogicalGet *GetChildGet(const LogicalAggregate &agg) {
    if (agg.children.size() != 1) {
        return nullptr;
    }
    LogicalOperator &child = *agg.children[0];
    LogicalOperator *op;
    if (child.type == LOGICAL_GET) {
        op = &child;
    } else if (child.type == LOGICAL_PROJECTION && child.children.size() == 1 &&
               child.children[0]->type == LOGICAL_GET) {
        op = child.children[0].get();
    } else {
        return nullptr;
    }
    LogicalGet &get = op->Cast<LogicalGet>();
    return is_vortex_scan(get.function) ? &get : nullptr;
}
