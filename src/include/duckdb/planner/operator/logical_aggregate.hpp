//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/parser/group_by_node.hpp"

namespace duckdb
{
//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalAggregate : public LogicalOperator
{
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY;

public:
	LogicalAggregate(idx_t group_index, idx_t aggregate_index, vector<unique_ptr<Expression>> select_list);

	//! The table index for the groups of the LogicalAggregate
	idx_t group_index;
	//! The table index for the aggregates of the LogicalAggregate
	idx_t aggregate_index;
	//! The table index for the GROUPING function calls of the LogicalAggregate
	idx_t groupings_index;
	//! The set of groups (optional).
	vector<unique_ptr<Expression>> groups;
	//! The set of grouping sets (optional).
	vector<GroupingSet> grouping_sets;
	//! The list of grouping function calls (optional)
	vector<vector<idx_t>> grouping_functions;
	//! Group statistics (optional)
	vector<unique_ptr<BaseStatistics>> group_stats;

public:
	string ParamsToString() const override;

	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	idx_t EstimateCardinality(ClientContext &context) override;
	vector<idx_t> GetTableIndex() const override;

	unique_ptr<Operator> Copy() override;
	
	unique_ptr<Operator> CopyWithNewGroupExpression(CGroupExpression *pgexpr) override;

	unique_ptr<Operator> CopyWithNewChildren(CGroupExpression *pgexpr,
                                        duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                        double cost) override;
	
	void CE() override;

	CXform_set *XformCandidates() const;
	
protected:
	void ResolveTypes() override;
};
} // namespace duckdb