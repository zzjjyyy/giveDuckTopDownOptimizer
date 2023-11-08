//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace duckdb {

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_FILTER;

public:
	LogicalFilter();

	explicit LogicalFilter(unique_ptr<Expression> expression);
	
	vector<idx_t> projection_map;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	bool SplitPredicates()
	{
		return SplitPredicates(expressions);
	}
	//! Splits up the predicates of the LogicalFilter into a set of predicates
	//! separated by AND Returns whether or not any splits were made
	static bool SplitPredicates(vector<unique_ptr<Expression>> &expressions);

protected:
	void ResolveTypes() override;

public:
	duckdb::unique_ptr<CKeyCollection> DeriveKeyCollection(CExpressionHandle &exprhdl) override;
	
	duckdb::unique_ptr<CPropConstraint> DerivePropertyConstraint(CExpressionHandle &exprhdl) override;

	// Rehydrate expression from a given cost context and child expressions
	duckdb::unique_ptr<Operator>
	SelfRehydrate(duckdb::unique_ptr<CCostContext> pcc,
				  duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
				  duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan) override;
	
	unique_ptr<Operator> Copy() override;
	
	unique_ptr<Operator>
	CopyWithNewGroupExpression(unique_ptr<CGroupExpression> pgexpr) override;

	unique_ptr<Operator>
	CopyWithNewChildren(unique_ptr<CGroupExpression> pgexpr,
                        duckdb::vector<unique_ptr<Operator>> pdrgpexpr,
                        double cost) override;
	
	void CE() override;

public:
	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------
	// candidate set of xforms
	virtual duckdb::unique_ptr<CXform_set> XformCandidates() const override;
};

} // namespace duckdb
