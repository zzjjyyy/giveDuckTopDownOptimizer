//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::DUMMY_SCAN;

	vector<ColumnBinding> v_column_binding;

public:
	explicit PhysicalDummyScan(vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN, std::move(types), estimated_cardinality) {
	}

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// ------------------------------ ORCA ---------------------------------
	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override;

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

	// ------------------------------ DuckDB ---------------------------------
	vector<ColumnBinding> GetColumnBindings() override {
		return v_column_binding;	
	}
};
} // namespace duckdb
