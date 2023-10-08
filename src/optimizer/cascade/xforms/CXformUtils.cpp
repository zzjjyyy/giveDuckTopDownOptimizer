#include "duckdb/optimizer/cascade/xforms/CXformUtils.h"

#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace gpopt;

duckdb::unique_ptr<Operator> CXformUtils::PexprPushGbBelowJoin(Operator *m_operator) {
	LogicalAggregate *pexprGb = (LogicalAggregate*)m_operator;
	LogicalComparisonJoin* pexprJoin = (LogicalComparisonJoin*)m_operator->children[0].get();
	Operator *pexprOuter = pexprJoin->children[0].get();
	Operator *pexprInner = pexprJoin->children[1].get();
	duckdb::vector<ColumnBinding> pcrsOuterOutput = pexprOuter->GetColumnBindings();
	duckdb::vector<ColumnBinding> pcrsAggOutput = pexprGb->GetColumnBindings();
	duckdb::vector<ColumnBinding> pcrsUsed;
	for(auto &child : pexprGb->expressions) {
		duckdb::vector<ColumnBinding> v = child->GetColumnBinding();
		pcrsUsed.insert(pcrsUsed.end(), v.begin(), v.end());
	}
	duckdb::vector<ColumnBinding> pcrsFKey;
	for(auto& child : pexprJoin->conditions) {
		auto left = child.left->GetColumnBinding();
		auto right = child.right->GetColumnBinding();
		if(IsPK(right, pexprInner)) {
			pcrsFKey.insert(pcrsFKey.end(), left.begin(), left.end());
		}
	}
	duckdb::vector<ColumnBinding> pcrsScalarFromOuter;
	for(auto& child : pexprJoin->conditions) {
		auto left = child.left->GetColumnBinding();
		pcrsScalarFromOuter.insert(pcrsFKey.end(), left.begin(), left.end());
	}
	duckdb::vector<ColumnBinding> pcrsGrpCols;
	for(auto &child : pexprGb->groups) {
		duckdb::vector<ColumnBinding> v = child->GetColumnBinding();
		pcrsGrpCols.insert(pcrsUsed.end(), v.begin(), v.end());
	}
	bool fCanPush = FCanPushGbAggBelowJoin(pcrsGrpCols, pcrsOuterOutput, pcrsScalarFromOuter, pcrsAggOutput, pcrsUsed, pcrsFKey);
	if (!fCanPush)
	{
		return nullptr;
	}
	// here, we know that grouping columns include FK and all used columns by Gb
	// come only from the outer child of the join;
	// we can safely push Gb to be on top of join's outer child
	duckdb::unique_ptr<LogicalAggregate> popGbAggNew = PopGbAggPushableBelowJoin(pexprGb, pcrsOuterOutput, pcrsGrpCols);
	duckdb::unique_ptr<Operator> new_operator = pexprJoin->Copy();
	new_operator->children.clear();
	new_operator->AddChild(std::move(popGbAggNew));
	new_operator->AddChild(std::move(pexprInner->Copy()));
	return new_operator;
}

bool CXformUtils::IsPK(duckdb::vector<ColumnBinding> v, Operator *m_operator) {
	if(m_operator->logical_type == LogicalOperatorType::LOGICAL_GET) {
		LogicalGet *logical_get = (LogicalGet*)m_operator;
		/* Is it the table that column binds? */
		if(logical_get->table_index == v[0].table_index) {
			/* The first column is the primary key */
			if(logical_get->column_ids[v[0].column_index] == 0) {
				return true;
			}
		}
		return false;
	}
	if(IsPK(v, m_operator->children[0].get())) {
		return true;
	}
	if(IsPK(v, m_operator->children[1].get())) {
		return true;
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FCanPushGbAggBelowJoin
//
//	@doc:
//		Check if the preconditions for pushing down Group by through join are
//		satisfied
//---------------------------------------------------------------------------
bool CXformUtils::FCanPushGbAggBelowJoin(duckdb::vector<ColumnBinding> pcrsGrpCols,
										 duckdb::vector<ColumnBinding> pcrsJoinOuterChildOutput,
										 duckdb::vector<ColumnBinding> pcrsJoinScalarUsedFromOuter,
										 duckdb::vector<ColumnBinding> pcrsGrpByOutput,
										 duckdb::vector<ColumnBinding> pcrsGrpByUsed,
										 duckdb::vector<ColumnBinding> pcrsFKey)
{
	bool fGrpByProvidesUsedColumns = CUtils::ContainsAll(pcrsGrpByOutput, pcrsJoinScalarUsedFromOuter);
	bool fHasFK = (0 != pcrsFKey.size());
	bool fGrpColsContainFK = (fHasFK && CUtils::ContainsAll(pcrsGrpCols, pcrsFKey));
	bool fOutputColsContainUsedCols = CUtils::ContainsAll(pcrsJoinOuterChildOutput, pcrsGrpByUsed);
	if (!fHasFK || !fGrpColsContainFK || !fOutputColsContainUsedCols || !fGrpByProvidesUsedColumns)
	{
		// GrpBy cannot be pushed through join because
		// (1) no FK exists, or
		// (2) FK exists but grouping columns do not include it, or
		// (3) Gb uses columns from both join children, or
		// (4) Gb hides columns required for the join scalar child
		return false;
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PopGbAggPushableBelowJoin
//
//	@doc:
//		Create the Gb operator to be pushed below a join given the original Gb
//		operator, output columns of the join's outer child and grouping cols
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<LogicalAggregate> CXformUtils::PopGbAggPushableBelowJoin(LogicalAggregate *popGbAggOld,
									   										duckdb::vector<ColumnBinding> pcrsOutputOuter,
									   										duckdb::vector<ColumnBinding> pcrsGrpCols)
{
	duckdb::unique_ptr<Operator> popNew = popGbAggOld->Copy();
	duckdb::unique_ptr<LogicalAggregate> popGbAggNew = unique_ptr_cast<Operator, LogicalAggregate>(std::move(popNew));
	if (!CUtils::ContainsAll(pcrsOutputOuter, pcrsGrpCols))
	{
		popGbAggNew->groups.clear();
		// we have grouping columns from both join children;
		// we can drop grouping columns from the inner join child since
		// we already have a FK in grouping columns
		for(auto &child : popGbAggOld->groups) {
			auto v = child->GetColumnBinding();
			if(CUtils::ContainsAll(pcrsOutputOuter, v)) {
				popGbAggNew->groups.push_back(child->Copy());
			}
		}
	}
	return popGbAggNew;
}