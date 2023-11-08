#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace duckdb {

void LogicalTopN::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
	writer.WriteField(offset);
	writer.WriteField(limit);
}

unique_ptr<LogicalOperator> LogicalTopN::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	auto offset = reader.ReadRequired<idx_t>();
	auto limit = reader.ReadRequired<idx_t>();
	return make_uniq<LogicalTopN>(std::move(orders), limit, offset);
}

idx_t LogicalTopN::EstimateCardinality(ClientContext &context)
{
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if (limit >= 0 && child_cardinality < idx_t(limit))
	{
		return limit;
	}
	return child_cardinality;
}

duckdb::unique_ptr<CKeyCollection>
LogicalTopN::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	const ULONG arity = exprhdl.Arity();
	return PkcDeriveKeysPassThru(exprhdl, arity - 1);
}
	
duckdb::unique_ptr<CPropConstraint>
LogicalTopN::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return PpcDeriveConstraintPassThru(exprhdl, exprhdl.Arity() - 1);
}

duckdb::unique_ptr<Operator>
LogicalTopN::SelfRehydrate(duckdb::unique_ptr<CCostContext> pcc,
						   duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
						   duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan)
{
	auto pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	LogicalTopN* tmp = (LogicalTopN*)pgexpr->m_operator.get();
	auto pexpr = make_uniq<LogicalTopN>(tmp->orders, tmp->limit, tmp->offset);
	// Need to delete
	// for (auto &child : pdrgpexpr)
	for (auto child : pdrgpexpr)
	{
		pexpr->AddChild(child);
	}
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::XformCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CXform_set>
LogicalTopN::XformCandidates() const
{
	auto xform_set = make_uniq<CXform_set>();
	(void) xform_set->set(CXform::ExfImplementLimit);
	(void) xform_set->set(CXform::ExfSplitLimit);
	return xform_set;
}
} // namespace duckdb
