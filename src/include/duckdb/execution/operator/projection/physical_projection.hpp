//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PROJECTION;

public:
	PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality);
	
	vector<unique_ptr<Expression>> select_list;
	vector<ColumnBinding> v_column_binding;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override
	{
		return true;
	}

	string ParamsToString() const override;

	static unique_ptr<PhysicalOperator> CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types, const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map, const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality);

public:
	// Rehydrate expression from a given cost context and child expressions
	duckdb::unique_ptr<Operator>
	SelfRehydrate(duckdb::unique_ptr<CCostContext> pcc,
				  duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
				  duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan) override;
	
	unique_ptr<Operator> Copy() override;

	unique_ptr<Operator>
	CopyWithNewGroupExpression(unique_ptr<CGroupExpression> pgexpr) override;
	
	unique_ptr<Operator>
	CopyWithNewChildren(duckdb::unique_ptr<CGroupExpression> pgexpr,
						duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
						double cost) override;

	vector<ColumnBinding> GetColumnBindings() override {
		return v_column_binding;
	}

	void CE() override;
};
} // namespace duckdb