//---------------------------------------------------------------------------
//	@filename:
//		COptimizationContext.cpp
//
//	@doc:
//		Implementation of optimization context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"

#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"
#include "duckdb/optimizer/cascade/base/COrderProperty.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropRelational.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

namespace gpopt {
// invalid optimization context
const COptimizationContext COptimizationContext::M_INVALID_OPT_CONTEXT;

// invalid optimization context pointer
const OPTCTXT_PTR COptimizationContext::M_INVALID_OPT_CONTEXT_PTR = nullptr;

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::~COptimizationContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COptimizationContext::~COptimizationContext() {
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::BestExpression
//
//	@doc:
//		Best group expression accessor
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CGroupExpression>
COptimizationContext::BestExpression() const {
	if (nullptr == m_best_cost_context) {
		return nullptr;
	}
	return m_best_cost_context->m_group_expression;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::SetBest
//
//	@doc:
//		 Set best cost context
//
//---------------------------------------------------------------------------
void COptimizationContext::SetBest(duckdb::unique_ptr<CCostContext> pcc) {
	m_best_cost_context = pcc;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::Matches
//
//	@doc:
//		Match against another context
//
//---------------------------------------------------------------------------
bool COptimizationContext::Matches(const duckdb::unique_ptr<COptimizationContext> poc) const {
	if (m_group != poc->m_group || m_search_stage != poc->UlSearchStageIndex()) {
		return false;
	}
	auto prppFst = this->m_required_plan_properties;
	auto prppSnd = poc->m_required_plan_properties;
	// make sure we are not comparing to invalid context
	if (nullptr == prppFst || nullptr == prppSnd) {
		return nullptr == prppFst && nullptr == prppSnd;
	}
	return prppFst->Equals(prppSnd);
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FEqualForStats
//
//	@doc:
//		Equality function used for computing stats during costing
//
//---------------------------------------------------------------------------
bool COptimizationContext::FEqualForStats(const duckdb::unique_ptr<COptimizationContext> pocLeft,
										  const duckdb::unique_ptr<COptimizationContext> pocRight) {
	return CUtils::Equals(pocLeft->m_required_relational_properties->PcrsStat(),
	                      pocRight->m_required_relational_properties->PcrsStat());
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimize
//
//	@doc:
//		Return true if given group expression should be optimized under
//		given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimize(duckdb::unique_ptr<CGroupExpression> pgexprParent,
									 duckdb::unique_ptr<CGroupExpression> pgexprChild,
                                     duckdb::unique_ptr<COptimizationContext> pocChild,
									 ULONG ulSearchStages) {
	if (PhysicalOperatorType::ORDER_BY == pgexprChild->m_operator->physical_type) {
		return FOptimizeSort(pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}
	if (PhysicalOperatorType::NESTED_LOOP_JOIN == pgexprChild->m_operator->physical_type) {
		return FOptimizeNLJoin(pgexprParent, pgexprChild, pocChild, ulSearchStages);
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FEqualIds
//
//	@doc:
//		Compare array of optimization contexts based on context ids
//
//---------------------------------------------------------------------------
bool COptimizationContext::FEqualContextIds(duckdb::vector<duckdb::unique_ptr<COptimizationContext>> pdrgpocFst,
                                            duckdb::vector<duckdb::unique_ptr<COptimizationContext>> pdrgpocSnd) {
	if (0 == pdrgpocFst.size() || 0 == pdrgpocSnd.size()) {
		return (0 == pdrgpocFst.size() && 0 == pdrgpocSnd.size());
	}
	const ULONG ulCtxts = pdrgpocFst.size();
	if (ulCtxts != pdrgpocSnd.size()) {
		return false;
	}
	bool fEqual = true;
	for (ULONG ul = 0; fEqual && ul < ulCtxts; ul++) {
		fEqual = pdrgpocFst[ul]->m_id == pdrgpocSnd[ul]->m_id;
	}
	return fEqual;
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeSort
//
//	@doc:
//		Check if a Sort node should be optimized for the given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimizeSort(duckdb::unique_ptr<CGroupExpression> pgexprParent,
										 duckdb::unique_ptr<CGroupExpression> pgexprSort,
                                         duckdb::unique_ptr<COptimizationContext> poc,
										 ULONG ulSearchStages) {
	return poc->m_required_plan_properties
		->m_sort_order->FCompatible(((PhysicalOrder *)pgexprSort->m_operator.get())->OrderSpec());
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizationContext::FOptimizeAgg
//
//	@doc:
//		Check if Agg node should be optimized for the given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimizeAgg(duckdb::unique_ptr<CGroupExpression> pgexprParent,
										duckdb::unique_ptr<CGroupExpression> pgexprAgg,
                                        duckdb::unique_ptr<COptimizationContext> poc,
										ULONG ulSearchStages) {
	// otherwise, we need to avoid optimizing node unless it is a multi-stage agg
	// COptimizationContext* pocFound = pgexprAgg->m_group->PocLookupBest(ulSearchStages,
	// poc->m_required_physical_property); if (nullptr != pocFound && pocFound->FHasMultiStageAggPlan())
	// {
	//  	// context already has a multi-stage agg plan, optimize child only if it is also a multi-stage agg
	// 	    return CPhysicalAgg::PopConvert(pgexprAgg->Pop())->FMultiStage();
	// }
	// child context has no plan yet, return true
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::FOptimizeNLJoin
//
//	@doc:
//		Check if NL join node should be optimized for the given context
//
//---------------------------------------------------------------------------
bool COptimizationContext::FOptimizeNLJoin(duckdb::unique_ptr<CGroupExpression> pgexprParent,
										   duckdb::unique_ptr<CGroupExpression> pgexprJoin,
                                           duckdb::unique_ptr<COptimizationContext> poc,
										   ULONG ulSearchStages) {
	// For correlated join, the requested columns must be covered by outer child
	// columns and columns to be generated from inner child
	duckdb::vector<ColumnBinding> pcrs;
	auto pcrsOuterChild =
	    CDerivedLogicalProp::GetRelationalProperties((*pgexprJoin)[0]->m_derived_properties)->GetOutputColumns();
	pcrs.insert(pcrsOuterChild.begin(), pcrsOuterChild.end(), pcrs.end());
	bool fIncluded = CUtils::ContainsAll(pcrs, poc->m_required_plan_properties->m_cols);
	return fIncluded;
}
} // namespace gpopt