//---------------------------------------------------------------------------
//	@filename:
//		COptimizationContext.h
//
//	@doc:
//		Optimization context object stores properties required to hold
//		on the plan generated by the optimizer
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CRequiredPhysicalProp.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"

#define GPOPT_INVALID_OPTCTXT_ID gpos::ulong_max

namespace gpopt {
using namespace gpos;

// forward declarations
class CGroup;
class CGroupExpression;
class CCostContext;
class COptimizationContext;
class CRequiredLogicalProp;

// optimization context pointer definition
typedef COptimizationContext *OPTCTXT_PTR;

//---------------------------------------------------------------------------
//	@class:
//		COptimizationContext
//
//	@doc:
//		Optimization context
//
//---------------------------------------------------------------------------
class COptimizationContext {
public:
	// states of optimization context
	enum EState { estUnoptimized, estOptimizing, estOptimized, estSentinel };

public:
	// dummy ctor; used for creating invalid context
	COptimizationContext()
	    : m_id(GPOPT_INVALID_OPTCTXT_ID), m_group(nullptr), m_required_plan_properties(nullptr),
	      m_required_relational_properties(nullptr), m_search_stage(0), m_best_cost_context(nullptr),
	      m_estate(estUnoptimized), m_has_multi_stage_agg_plan(false) {};

	//---------------------------------------------------------------------------
	// ctor
	// @inputs:
	//	 CRequiredLogicalProp* prprel: required relational props -- used during stats derivation
	//	 IStatisticsArray* stats_ctxt: stats of previously optimized expressions
	//---------------------------------------------------------------------------
	COptimizationContext(CGroup *pgroup, CRequiredPhysicalProp *prpp, CRequiredLogicalProp *prprel,
	                     ULONG search_stage_index)
	    : m_id(GPOPT_INVALID_OPTCTXT_ID), m_group(pgroup), m_required_plan_properties(prpp),
	      m_required_relational_properties(prprel), m_search_stage(search_stage_index), m_best_cost_context(nullptr),
	      m_estate(estUnoptimized), m_has_multi_stage_agg_plan(false) {
	}
	COptimizationContext(const COptimizationContext &) = delete;
	virtual ~COptimizationContext();

	// unique id within owner group, used for debugging
	ULONG m_id;
	// back pointer to owner group, used for debugging
	CGroup *m_group;
	// required plan properties
	CRequiredPhysicalProp *m_required_plan_properties;
	// required relational properties -- used for stats computation during costing
	CRequiredLogicalProp *m_required_relational_properties;
	// index of search stage where context is generated
	ULONG m_search_stage;
	// best cost context under the optimization context
	CCostContext *m_best_cost_context;
	// optimization context state
	EState m_estate;
	// is there a multi-stage Agg plan satisfying required properties
	bool m_has_multi_stage_agg_plan;
	// context's optimization job queue
	CJobQueue m_opt_job_queue;
	// link for optimization context hash table in CGroup
	SLink m_link;
	// invalid optimization context, needed for hash table iteration
	static const COptimizationContext M_INVALID_OPT_CONTEXT;
	// invalid optimization context pointer, needed for cost contexts hash table iteration
	static const OPTCTXT_PTR M_INVALID_OPT_CONTEXT_PTR;

public:
	// internal matching function
	bool FMatchSortColumns(COptimizationContext *poc) const;
	// best group expression accessor
	CGroupExpression *BestExpression() const;
	// match optimization contexts
	bool Matches(const COptimizationContext *poc) const;
	// search stage index accessor
	ULONG UlSearchStageIndex() const {
		return m_search_stage;
	}
	// optimization job queue accessor
	CJobQueue *PjqOptimization() {
		return &m_opt_job_queue;
	}
	// set optimization context id
	void SetId(ULONG id) {
		m_id = id;
	}
	// set optimization context state
	void SetState(EState estNewState) {
		m_estate = estNewState;
	}
	// set best cost context
	void SetBest(CCostContext *pcc);
	// comparison operator for hashtables
	bool operator==(const COptimizationContext &oc) const {
		return oc.Matches(this);
	}
	// check equality of optimization contexts
	static bool Equals(const COptimizationContext &ocLeft, const COptimizationContext &ocRight) {
		return ocLeft == ocRight;
	}
	size_t HashValue() {
		return m_required_plan_properties->HashValue();
	}
	// hash function for optimization context
	static size_t HashValue(const COptimizationContext &oc) {
		return oc.m_required_plan_properties->HashValue();
	}
	// equality function for cost contexts hash table
	static bool Equals(const OPTCTXT_PTR &pocLeft, const OPTCTXT_PTR &pocRight) {
		if (pocLeft == M_INVALID_OPT_CONTEXT_PTR || pocRight == M_INVALID_OPT_CONTEXT_PTR) {
			return pocLeft == M_INVALID_OPT_CONTEXT_PTR && pocRight == M_INVALID_OPT_CONTEXT_PTR;
		}
		return *pocLeft == *pocRight;
	}
	// hash function for cost contexts hash table
	static size_t HashValue(const OPTCTXT_PTR &poc) {
		return HashValue(*poc);
	}
	// hash function used for computing stats during costing
	static ULONG UlHashForStats(const COptimizationContext *poc) {
		return HashValue(*poc);
	}
	// equality function used for computing stats during costing
	static bool FEqualForStats(const COptimizationContext *pocLeft, const COptimizationContext *pocRight);
	// check if Agg node should be optimized for the given context
	static bool FOptimizeAgg(CGroupExpression *pgexprParent, CGroupExpression *pgexprAgg, COptimizationContext *poc,
	                         ULONG ulSearchStages);
	// check if Sort node should be optimized for the given context
	static bool FOptimizeSort(CGroupExpression *pgexprParent, CGroupExpression *pgexprSort, COptimizationContext *poc,
	                          ULONG ulSearchStages);
	// check if Motion node should be optimized for the given context
	static bool FOptimizeMotion(CGroupExpression *pgexprParent, CGroupExpression *pgexprMotion,
	                            COptimizationContext *poc, ULONG ulSearchStages);
	// check if NL join node should be optimized for the given context
	static bool FOptimizeNLJoin(CGroupExpression *pgexprParent, CGroupExpression *pgexprMotion,
	                            COptimizationContext *poc, ULONG ulSearchStages);
	// return true if given group expression should be optimized under given context
	static bool FOptimize(CGroupExpression *pgexprParent, CGroupExpression *pgexprChild, COptimizationContext *pocChild,
	                      ULONG ulSearchStages);
	// compare array of contexts based on context ids
	static bool FEqualContextIds(duckdb::vector<COptimizationContext *> pdrgpocFst,
	                             duckdb::vector<COptimizationContext *> pdrgpocSnd);
	// compute required properties to CTE producer based on plan properties of CTE consumer
	// static CRequiredPhysicalProp *PrppCTEProducer(COptimizationContext *poc, ULONG ulSearchStages);
}; // class COptimizationContext
} // namespace gpopt