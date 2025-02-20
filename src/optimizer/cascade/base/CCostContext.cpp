//---------------------------------------------------------------------------
//	@filename:
//		CCostContext.cpp
//
//	@doc:
//		Implementation of cost context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CCostContext.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropPlan.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtRelational.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"

#include <cstdlib>

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::CCostContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostContext::CCostContext(COptimizationContext *poc, ULONG ulOptReq, CGroupExpression *pgexpr)
    : m_cost(GPOPT_INVALID_COST), m_estate(estUncosted), m_group_expression(pgexpr), m_group_expr_for_stats(nullptr),
      m_derived_prop_plan(nullptr), m_optimization_request_num(ulOptReq), m_fPruned(false), m_poc(poc) {
	if (m_group_expression != nullptr) {
		CGroupExpression *pgexprForStats = m_group_expression->m_group->BestPromiseGroupExpr(m_group_expression);
		if (nullptr != pgexprForStats) {
			m_group_expr_for_stats = pgexprForStats;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::~CCostContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostContext::~CCostContext() {
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FNeedsNewStats
//
//	@doc:
//		Check if we need to derive new stats for this context,
//		by default a cost context inherits stats from the owner group,
//		the only current exception is when part of the plan below cost
//		context is affected by partition elimination done by partition
//		selection in some other part of the plan
//
//---------------------------------------------------------------------------
bool CCostContext::FNeedsNewStats() const {
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::DerivePlanProps
//
//	@doc:
//		Derive properties of the plan carried by cost context
//
//---------------------------------------------------------------------------
void CCostContext::DerivePlanProps() {
	if (nullptr == m_derived_prop_plan) {
		// derive properties of the plan carried by cost context
		CExpressionHandle handle;
		handle.Attach(this);
		handle.DerivePlanPropsForCostContext();
		CDerivedPhysicalProp *plan_property = CDerivedPhysicalProp::DrvdPlanProperty(handle.DerivedProperty());
		m_derived_prop_plan = plan_property;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::operator ==
//
//	@doc:
//		Comparison operator
//
//---------------------------------------------------------------------------
bool CCostContext::operator==(const CCostContext &cc) const {
	return Equals(cc, *this);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::IsValid
//
//	@doc:
//		Check validity by comparing derived and required properties
//
//---------------------------------------------------------------------------
bool CCostContext::IsValid() {
	// obtain relational properties from group
	CDerivedLogicalProp *prop_relation =
	    CDerivedLogicalProp::GetRelationalProperties(m_group_expression->m_group->m_derived_properties);
	// derive plan properties
	DerivePlanProps();
	// checking for required properties satisfaction
	bool is_valid = m_poc->m_required_plan_properties->FSatisfied(prop_relation, m_derived_prop_plan);
	return is_valid;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBreakCostTiesForJoinPlan
//
//	@doc:
//		For two cost contexts with join plans of the same cost, break the
//		tie in cost values based on join depth,
//		if tie-resolution succeeded, store a pointer to preferred cost
//		context in output argument
//
//---------------------------------------------------------------------------
void CCostContext::BreakCostTiesForJoinPlans(CCostContext *pccFst, CCostContext *pccSnd, CCostContext **ppccPrefered,
                                             bool *pfTiesResolved) {
	// for two join plans with the same estimated rows in both children,
	// prefer the plan that has smaller tree depth on the inner side,
	// this is because a smaller tree depth means that row estimation
	// errors are not grossly amplified,
	// since we build a hash table/broadcast the inner side, we need
	// to have more reliable statistics on this side
	*pfTiesResolved = false;
	*ppccPrefered = nullptr;
	double dRowsOuterFst = pccFst->m_optimization_contexts[0]->m_best_cost_context->m_cost;
	double dRowsInnerFst = pccFst->m_optimization_contexts[1]->m_best_cost_context->m_cost;
	if (dRowsOuterFst != dRowsInnerFst) {
		// two children of first plan have different row estimates
		return;
	}
	double dRowsOuterSnd = pccSnd->m_optimization_contexts[0]->m_best_cost_context->m_cost;
	double dRowsInnerSnd = pccSnd->m_optimization_contexts[1]->m_best_cost_context->m_cost;
	if (dRowsOuterSnd != dRowsInnerSnd) {
		// two children of second plan have different row estimates
		return;
	}
	if (dRowsInnerFst != dRowsInnerSnd) {
		// children of first plan have different row estimates compared to second plan
		return;
	}
	// both plans have equal estimated rows for both children, break tie based on join depth
	*pfTiesResolved = true;
	ULONG ulOuterJoinDepthFst =
	    CDerivedLogicalProp::GetRelationalProperties((*pccFst->m_group_expression)[0]->m_derived_properties)
	        ->GetJoinDepth();
	ULONG ulInnerJoinDepthFst =
	    CDerivedLogicalProp::GetRelationalProperties((*pccFst->m_group_expression)[1]->m_derived_properties)
	        ->GetJoinDepth();
	if (ulInnerJoinDepthFst < ulOuterJoinDepthFst) {
		*ppccPrefered = pccFst;
	} else {
		*ppccPrefered = pccSnd;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::FBetterThan
//
//	@doc:
//		Is current context better than the given equivalent context
//		based on cost?
//
//---------------------------------------------------------------------------
bool CCostContext::FBetterThan(CCostContext *pcc) const {
	double dCostDiff = (this->m_cost - pcc->m_cost);
	if (dCostDiff < 0.0) {
		// if current context has a strictly smaller cost, then it is preferred
		return true;
	}

	if (dCostDiff > 0.0) {
		// if current context has a strictly larger cost, then it is not preferred
		return false;
	}
	// otherwise, we need to break tie in cost values
	// RULE 1: break ties in cost of join plans,
	// if both plans have the same estimated rows for both children, prefer
	// the plan with deeper outer child
	if (CUtils::FPhysicalJoin(this->m_group_expression->m_operator.get())
		&& CUtils::FPhysicalJoin(pcc->m_group_expression->m_operator.get()))
	{
		if(this->m_group_expression->m_operator->children[0]->estimated_cardinality >
		   this->m_group_expression->m_operator->children[1]->estimated_cardinality) {
			return true;
		} else if (pcc->m_group_expression->m_operator->children[0]->estimated_cardinality >
		   pcc->m_group_expression->m_operator->children[1]->estimated_cardinality){
			return false;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostContext::CostCompute
//
//	@doc:
//		Compute cost of current context,
//
//		the function extracts cardinality and row width of owner operator
//		and child operators, and then adjusts row estimate obtained from
//		statistics based on data distribution obtained from plan properties,
//
//		statistics row estimate is computed on logical expressions by
//		estimating the size of the whole relation regardless data
//		distribution, on the other hand, optimizer's cost model computes
//		the cost of a plan instance on some segment,
//
//		when a plan produces tuples distributed to multiple segments, we
//		need to divide statistics row estimate by the number segments to
//		provide a per-segment row estimate for cost computation,
//
//		Note that this scaling of row estimate cannot happen during
//		statistics derivation since plans are not created yet at this point
//
// 		this function also extracts number of rebinds of owner operator child
//		operators, if statistics are computed using predicates with external
//		parameters (outer references), number of rebinds is the total number
//		of external parameters' values
//
//---------------------------------------------------------------------------
double CCostContext::CostCompute(duckdb::vector<double> pdrgpcostChildren) {
	if (!this->m_group_expression->m_operator->has_estimated_cardinality) {
		this->m_group_expression->m_operator->CE();
	}
	if (m_optimization_contexts.size() == 0) {
		/* Scan */
		return 0;
	} else if (m_optimization_contexts.size() == 1) {
		/* Filter or Sort */
		if(this->m_group_expression->m_operator->logical_type == LogicalOperatorType::LOGICAL_FILTER
		|| this->m_group_expression->m_operator->physical_type == PhysicalOperatorType::FILTER) {
			return 0;
		} else {
			return pdrgpcostChildren[0] +
				   this->m_group_expression->m_operator->estimated_cardinality;
		}
	} else {
		return pdrgpcostChildren[0] + pdrgpcostChildren[1] +
		       this->m_group_expression->m_operator->estimated_cardinality;
	}
}