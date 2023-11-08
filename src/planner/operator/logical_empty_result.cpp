#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

namespace duckdb
{
LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT)
{
	this->bindings = op->GetColumnBindings();
	op->ResolveOperatorTypes();
	this->return_types = op->types;
}

LogicalEmptyResult::LogicalEmptyResult()
	: LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT)
{
}

void LogicalEmptyResult::Serialize(FieldWriter &writer) const
{
	writer.WriteRegularSerializableList(return_types);
	writer.WriteList<ColumnBinding>(bindings);
}

unique_ptr<LogicalOperator> LogicalEmptyResult::Deserialize(LogicalDeserializationState &state, FieldReader &reader)
{
	auto return_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto bindings = reader.ReadRequiredList<ColumnBinding>();
	auto result = unique_ptr<LogicalEmptyResult>(new LogicalEmptyResult());
	result->return_types = return_types;
	result->bindings = bindings;
	return std::move(result);
}

duckdb::unique_ptr<CPropConstraint>
LogicalEmptyResult::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return nullptr;
}

ULONG LogicalEmptyResult::DeriveJoinDepth(CExpressionHandle &exprhdl)
{
	return 0;
}
	
// Rehydrate expression from a given cost context and child expressions
duckdb::unique_ptr<Operator>
LogicalEmptyResult::SelfRehydrate(duckdb::unique_ptr<CCostContext> pcc,
								  duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
								  duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan)
{
	auto pexpr = make_uniq<LogicalEmptyResult>(unique_ptr_cast<Operator, LogicalOperator>(pdrgpexpr[0]->Copy()));
	pexpr->m_cost = pcc->m_cost;
	pexpr->m_group_expression = pcc->m_group_expression;
	return pexpr;
}

duckdb::unique_ptr<CKeyCollection> LogicalEmptyResult::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	return nullptr;
}

//-------------------------------------------------------------------------------------
// Transformations
//-------------------------------------------------------------------------------------
// candidate set of xforms
duckdb::unique_ptr<CXform_set>
LogicalEmptyResult::XformCandidates() const
{
	auto xform_set = make_uniq<CXform_set>();
	return xform_set;
}
} // namespace duckdb
