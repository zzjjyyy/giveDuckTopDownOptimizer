//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDummyScan represents a dummy scan returning a single row
class LogicalDummyScan : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_DUMMY_SCAN;

public:
	explicit LogicalDummyScan(idx_t table_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DUMMY_SCAN), table_index(table_index) {
		logical_type = LogicalOperatorType::LOGICAL_DUMMY_SCAN;
		m_derived_logical_property = make_uniq<CDerivedLogicalProp>();
		m_group_expression = nullptr;
		m_derived_physical_property = nullptr;
		m_required_physical_property = nullptr;
	}

	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(table_index, 0)};
	}

	idx_t EstimateCardinality(ClientContext &context) override {
		return 1;
	}
	
	void Serialize(FieldWriter &writer) const override;

	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	vector<idx_t> GetTableIndex() const override;

protected:
	void ResolveTypes() override {
		if (types.empty()) {
			types.emplace_back(LogicalType::INTEGER);
		}
	}

public:
	// ----------------- ORCA -------------------------

	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override {
		return 1;
	}

	duckdb::unique_ptr<CXform_set> XformCandidates() const override;

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
};
} // namespace duckdb
