//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_empty_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalEmptyResult returns an empty result. This is created by the optimizer if it can reason that certain parts of
//! the tree will always return an empty result.
class LogicalEmptyResult : public LogicalOperator
{
	LogicalEmptyResult();

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EMPTY_RESULT;

public:
	explicit LogicalEmptyResult(unique_ptr<LogicalOperator> op);

	//! The set of return types of the empty result
	vector<LogicalType> return_types;
	//! The columns that would be bound at this location (if the subtree was not optimized away)
	vector<ColumnBinding> bindings;

public:
	vector<ColumnBinding> GetColumnBindings() override
	{
		return bindings;
	}
	
	void Serialize(FieldWriter &writer) const override;
	
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	
	idx_t EstimateCardinality(ClientContext &context) override
	{
		return 0;
	}

protected:
	void ResolveTypes() override
	{
		this->types = return_types;
	}

public:
	duckdb::unique_ptr<CKeyCollection> DeriveKeyCollection(CExpressionHandle &exprhdl) override;
	
	duckdb::unique_ptr<CPropConstraint> DerivePropertyConstraint(CExpressionHandle &exprhdl) override;

	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override;

	duckdb::unique_ptr<Operator>
	SelfRehydrate(duckdb::unique_ptr<CCostContext> pcc,
				  duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
				  duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan) override;

public:
	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------
	// candidate set of xforms
	duckdb::unique_ptr<CXform_set> XformCandidates() const override;
};
} // namespace duckdb
