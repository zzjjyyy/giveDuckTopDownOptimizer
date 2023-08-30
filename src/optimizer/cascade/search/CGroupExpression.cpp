//---------------------------------------------------------------------------
//	@filename:
//		CGroupExpression.cpp
//
//	@doc:
//		Implementation of group expressions
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/xforms/CXformExploration.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

#define GPOPT_COSTCTXT_HT_BUCKETS 100

namespace gpopt {
using namespace gpos;

// invalid group expression
const CGroupExpression CGroupExpression::M_INVALID_GROUP_EXPR;

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CGroupExpression
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CGroupExpression::CGroupExpression(duckdb::unique_ptr<Operator> op, duckdb::vector<CGroup *> groups,
                                   CXform::EXformId xform_id, CGroupExpression *group_expr_origin, bool is_intermediate)
    : m_id(GPOPT_INVALID_GEXPR_ID), m_duplicate_group_expr(nullptr), m_operator(std::move(op)), m_child_groups(groups),
      m_group(nullptr), m_xform_id_origin(xform_id), m_group_expr_origin(group_expr_origin),
      m_intermediate(is_intermediate), m_estate(estUnexplored), m_eol(EolLow), m_circular_dependency(ecdDefault) {
	// store sorted array of children for faster comparison
	if (1 < groups.size() && !m_operator->FInputOrderSensitive()) {
		m_child_groups_sorted.insert(m_child_groups_sorted.end(), groups.begin(), groups.end());
		sort(m_child_groups_sorted.begin(), m_child_groups_sorted.end(), CUtils::PtrCmp);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::~CGroupExpression
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroupExpression::~CGroupExpression() {
	if (this != &(CGroupExpression::M_INVALID_GROUP_EXPR)) {
		CleanupContexts();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CleanupContexts
//
//	@doc:
//		 Destroy stored cost contexts in hash table
//
//---------------------------------------------------------------------------
void CGroupExpression::CleanupContexts() {
	// need to suspend cancellation while cleaning up
	{ m_cost_context_map.clear(); }
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Init
//
//	@doc:
//		Init group expression
//
//
//---------------------------------------------------------------------------
void CGroupExpression::Init(CGroup *pgroup, ULONG id) {
	SetGroup(pgroup);
	SetId(id);
	SetOptimizationLevel();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetOptimizationLevel
//
//	@doc:
//		Set optimization level of group expression
////
//---------------------------------------------------------------------------
void CGroupExpression::SetOptimizationLevel() {
	/* I commenet here */
	if (m_operator->physical_type == PhysicalOperatorType::HASH_JOIN) {
		// optimize hash join first to minimize plan cost quickly
		m_eol = EolHigh;
	}
	/* I have deleted a lot if code here to fit duckdb */
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FValidContext
//
//	@doc:
//		Check if group expression is valid with respect to given child contexts
//
//		This is called during cost computation phase in group expression
//		optimization after enforcement is complete. Since it is called bottom-up,
//		for the given physical group expression, all the derived properties are
//		already computed.
//
//		Since property enforcement in CEngine::FCheckEnforceableProps() only determines
//		whether or not an enforcer is added to the group, it is possible for the
//		enforcer group expression to select a child group expression that did not
//		create the enforcer. This could lead to invalid plans that could not have
//		been prevented earlier because derived physical properties weren't
//		available. For example, a Motion group expression may select as a child a
//		DynamicTableScan that has unresolved part propagators, instead of picking
//		the PartitionSelector enforcer which would resolve it.
//
//		This method can be used to reject such plans.
//
//---------------------------------------------------------------------------
bool CGroupExpression::FValidContext(COptimizationContext *poc,
                                     duckdb::vector<COptimizationContext *> child_optimization_contexts) {
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetId
//
//	@doc:
//		Set id of expression
//
//---------------------------------------------------------------------------
void CGroupExpression::SetId(ULONG id) {
	m_id = id;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetGroup
//
//	@doc:
//		Set group pointer of expression
//
//---------------------------------------------------------------------------
void CGroupExpression::SetGroup(CGroup *pgroup) {
	m_group = pgroup;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FCostContextExists
//
//	@doc:
//		Check if cost context already exists in group expression hash table
//
//---------------------------------------------------------------------------
bool CGroupExpression::FCostContextExists(COptimizationContext *poc,
                                          duckdb::vector<COptimizationContext *> optimization_contexts) {
	// lookup context based on required properties
	CCostContext *pccFound;
	CostContextMap::iterator itr;
	{ itr = m_cost_context_map.find(poc->HashValue()); }
	while (m_cost_context_map.end() != itr) {
		pccFound = itr->second;
		if (COptimizationContext::FEqualContextIds(optimization_contexts, pccFound->m_optimization_contexts)) {
			// a cost context, matching required properties and child contexts, was already created
			return true;
		}
		{ ++itr; }
	}
	return false;
}

//---------------------------------------------------------------------------
//     @function:
//			CGroupExpression::CostContextRemove
//
//     @doc:
//			Remove cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *CGroupExpression::CostContextRemove(COptimizationContext *poc, ULONG id) {
	auto pccFound_iter = m_cost_context_map.find(poc->HashValue());
	while (m_cost_context_map.end() != pccFound_iter) {
		if (id == pccFound_iter->second->m_optimization_request_num) {
			CCostContext *pccFound = pccFound_iter->second;
			m_cost_context_map.erase(pccFound_iter);
			return pccFound;
		}
		++pccFound_iter;
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//     @function:
//			CGroupExpression::CostContextInsertBest
//
//     @doc:
//			Insert given context in hash table only if a better context
//			does not already exist,
//			return the context that is kept in hash table
//
//---------------------------------------------------------------------------
CCostContext *CGroupExpression::CostContextInsertBest(CCostContext *pcc) {
	COptimizationContext *poc = pcc->m_poc;
	const ULONG optimization_request_num = pcc->m_optimization_request_num;
	// remove existing cost context, if any
	CCostContext *pccExisting = CostContextRemove(poc, optimization_request_num);
	CCostContext *pccKept = nullptr;
	// compare existing context with given context
	if (nullptr == pccExisting || pcc->FBetterThan(pccExisting)) {
		// insert new context
		pccKept = CostContextInsert(pcc);
		if (nullptr != pccExisting) {
			if (pccExisting == poc->m_best_cost_context) {
				// change best cost context of the corresponding optimization context
				poc->SetBest(pcc);
			}
		}
	} else {
		// re-insert existing context
		pccKept = CostContextInsert(pccExisting);
	}
	return pccKept;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccComputeCost
//
//	@doc:
//		Compute and store expression's cost under a given context;
//		the function returns the cost context containing the computed cost
//
//---------------------------------------------------------------------------
CCostContext *CGroupExpression::PccComputeCost(COptimizationContext *opt_context, ULONG opt_request_num,
                                               duckdb::vector<COptimizationContext *> opt_contexts, bool is_pruned,
                                               double cost_lower_bound) {
	if (!is_pruned && !FValidContext(opt_context, opt_contexts)) {
		return nullptr;
	}
	// check if the same cost context is already created for current group expression
	if (FCostContextExists(opt_context, opt_contexts)) {
		return nullptr;
	}
	CCostContext *cost_context = new CCostContext(opt_context, opt_request_num, this);
	bool is_valid = true;
	// computing cost
	cost_context->SetState(CCostContext::estCosting);
	if (!is_pruned) {
		cost_context->SetChildContexts(opt_contexts);
		is_valid = cost_context->IsValid();
		if (is_valid) {
			double cost = CostCompute(cost_context);
			cost_context->SetCost(cost);
		}
	} else {
		cost_context->SetPruned();
		cost_context->SetCost(cost_lower_bound);
	}
	cost_context->SetState(CCostContext::estCosted);
	if (is_valid) {
		return CostContextInsertBest(cost_context);
	}
	// invalid cost context
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostLowerBound
//
//	@doc:
//		Compute a lower bound on plans rooted by current group expression for
//		the given required properties
//
//---------------------------------------------------------------------------
double CGroupExpression::CostLowerBound(CRequiredPropPlan *input_required_prop_plan, CCostContext *child_cost_context,
                                        ULONG child_index) {
	CPartialPlan *ppp = new CPartialPlan(this, input_required_prop_plan, child_cost_context, child_index);
	auto itr = m_partial_plan_cost_map.find(ppp->HashValue());
	if (itr != m_partial_plan_cost_map.end()) {
		return itr->second;
	}
	// compute partial plan cost
	double cost = ppp->CostCompute();
	m_partial_plan_cost_map.insert(make_pair(ppp->HashValue(), cost));
	return cost;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetState
//
//	@doc:
//		Set group expression state;
//
//---------------------------------------------------------------------------
void CGroupExpression::SetState(EState state) {
	m_estate = state;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::ResetState
//
//	@doc:
//		Reset group expression state;
//
//---------------------------------------------------------------------------
void CGroupExpression::ResetState() {
	m_estate = estUnexplored;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostCompute
//
//	@doc:
//		Costing scheme.
//
//---------------------------------------------------------------------------
double CGroupExpression::CostCompute(CCostContext *pcc) const {
	// prepare cost array
	duckdb::vector<COptimizationContext *> pdrgpoc = pcc->m_optimization_contexts;
	duckdb::vector<double> pdrgpcostChildren;
	const ULONG length = pdrgpoc.size();
	for (ULONG ul = 0; ul < length; ul++) {
		COptimizationContext *pocChild = pdrgpoc[ul];
		pdrgpcostChildren.emplace_back(pocChild->m_best_cost_context->m_cost);
	}
	double cost = pcc->CostCompute(pdrgpcostChildren);
	return cost;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FTransitioned
//
//	@doc:
//		Check if transition to the given state is completed;
//
//---------------------------------------------------------------------------
bool CGroupExpression::FTransitioned(EState estate) const {
	return !m_operator->FLogical() || (estate == estExplored && FExplored()) ||
	       (estate == estImplemented && FImplemented());
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostContextLookup
//
//	@doc:
//		Lookup cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *CGroupExpression::CostContextLookup(COptimizationContext *poc, ULONG optimization_request_num) {
	auto pccFound_iter = m_cost_context_map.find(poc->HashValue());
	while (m_cost_context_map.end() != pccFound_iter) {
		if (optimization_request_num == pccFound_iter->second->m_optimization_request_num) {
			CCostContext *pccFound = pccFound_iter->second;
			return pccFound;
		}
		++pccFound_iter;
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccLookupAll
//
//	@doc:
//		Lookup all valid cost contexts matching given optimization context
//
//---------------------------------------------------------------------------
duckdb::vector<CCostContext *> CGroupExpression::LookupAllMatchedCostContexts(COptimizationContext *poc) {
	duckdb::vector<CCostContext *> pdrgpcc;
	CostContextMap::iterator itr;
	CCostContext *pccFound = nullptr;
	bool fValid = false;
	{
		itr = m_cost_context_map.find(poc->HashValue());
		fValid = (m_cost_context_map.end() != itr && (itr->second)->m_cost != GPOPT_INVALID_COST &&
		          !(itr->second)->m_fPruned);
	}
	while (m_cost_context_map.end() != itr) {
		pccFound = itr->second;
		if (fValid) {
			pdrgpcc.emplace_back(pccFound);
		}
		{
			++itr;
			fValid = (m_cost_context_map.end() != itr && (itr->second)->m_cost != GPOPT_INVALID_COST &&
			          !(itr->second)->m_fPruned);
		}
	}
	return pdrgpcc;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostContextInsert
//
//	@doc:
//		Insert a cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *CGroupExpression::CostContextInsert(CCostContext *pcc) {
	// HERE BE DRAGONS
	// See comment in CCache::InsertEntry
	auto pccFound_iter = m_cost_context_map.find(pcc->m_poc->HashValue());
	while (m_cost_context_map.end() != pccFound_iter) {
		if (CCostContext::Equals(*pcc, *pccFound_iter->second)) {
			return pccFound_iter->second;
		}
		++pccFound_iter;
	}
	m_cost_context_map.insert(make_pair(pcc->m_poc->HashValue(), pcc));
	return pcc;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PreprocessTransform
//
//	@doc:
//		Pre-processing before applying transformation
//
//---------------------------------------------------------------------------
void CGroupExpression::PreprocessTransform(CXform *pxform) {
	if (pxform->FExploration() && CXformExploration::Pxformexp(pxform)->FNeedsStats()) {
		// derive stats on container group before applying xform
		CExpressionHandle exprhdl;
		exprhdl.Attach(this);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PostprocessTransform
//
//	@doc:
//		Post-processing after applying transformation
//
//---------------------------------------------------------------------------
void CGroupExpression::PostprocessTransform(CXform *pxform) {
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Transform
//
//	@doc:
//		Transform group expression using the given xform
//
//---------------------------------------------------------------------------
void CGroupExpression::Transform(CXform *pxform, CXformResult *results, ULONG *elapsed_time, ULONG *num_bindings) {
	// check xform promise
	CExpressionHandle exprhdl;
	exprhdl.Attach(this);
	exprhdl.DeriveProps(nullptr);
	if (CXform::ExfpNone == pxform->XformPromise(exprhdl)) {
		return;
	}
	// pre-processing before applying xform to group expression
	PreprocessTransform(pxform);
	// extract memo bindings to apply xform
	CBinding binding;
	CXformContext *pxfctxt = new CXformContext();
	COptimizerConfig *optconfig = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	ULONG bindThreshold = optconfig->m_hint->UlXformBindThreshold();
	Operator *pexprPattern = pxform->m_operator.get();
	Operator *pexpr = binding.PexprExtract(this, pexprPattern, nullptr);
	while (nullptr != pexpr) {
		++(*num_bindings);
		ULONG ulNumResults = results->m_alternative_expressions.size();
		pxform->Transform(pxfctxt, results, pexpr);
		ulNumResults = results->m_alternative_expressions.size() - ulNumResults;
		if ((bindThreshold != 0 && (*num_bindings) > bindThreshold) || pxform->IsApplyOnce() ||
		    (0 < results->m_alternative_expressions.size())) {
			// do not apply xform to other possible patterns
			break;
		}
		Operator *pexprLast = pexpr;
		pexpr = binding.PexprExtract(this, pexprPattern, pexprLast);
	}
	// post-processing before applying xform to group expression
	PostprocessTransform(pxform);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FMatchNonScalarChildren
//
//	@doc:
//		Match children of group expression against given children of
//		passed expression
//
//---------------------------------------------------------------------------
bool CGroupExpression::FMatchNonScalarChildren(CGroupExpression *group_expr) const {
	if (0 == Arity()) {
		return (group_expr->Arity() == 0);
	}
	return CGroup::FMatchNonScalarGroups(m_child_groups, group_expr->m_child_groups);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Matches
//
//	@doc:
//		Match group expression against given operator and its children
//
//---------------------------------------------------------------------------
bool CGroupExpression::Matches(const CGroupExpression *group_expr) const {
	// make sure we are not comparing to invalid group expression
	if (nullptr == this->m_operator || nullptr == group_expr->m_operator) {
		return nullptr == this->m_operator && nullptr == group_expr->m_operator;
	}
	// have same arity
	if (Arity() != group_expr->Arity()) {
		return false;
	}
	// match operators
	if (!m_operator->Matches(group_expr->m_operator.get())) {
		return false;
	}
	// compare inputs
	if (0 == Arity()) {
		return true;
	} else {
		if (1 == Arity() || m_operator->FInputOrderSensitive()) {
			return CGroup::FMatchGroups(m_child_groups, group_expr->m_child_groups);
		} else {
			return CGroup::FMatchGroups(m_child_groups_sorted, group_expr->m_child_groups_sorted);
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::HashValue
//
//	@doc:
//		static hash function for operator and group references
//
//---------------------------------------------------------------------------
ULONG CGroupExpression::HashValue(Operator *pop, duckdb::vector<CGroup *> groups) {
	ULONG ulHash = Operator::HashValue(pop);
	// ULONG ulHash = pop->HashValue();
	ULONG arity = groups.size();
	for (ULONG i = 0; i < arity; i++) {
		ulHash = CombineHashes(ulHash, groups[i]->HashValue());
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::HashValue
//
//	@doc:
//		static hash function for group expressions
//
//---------------------------------------------------------------------------
ULONG CGroupExpression::HashValue(const CGroupExpression &gexpr) {
	return gexpr.HashValue();
}

// Consider the group expression CLogicalSelect [ 0 3 ]
// it has the 0th child coming from Group 0, where Group 0 has Duplicate Group 4
// While deriving Stats, this will cause a circular loop as follows
// 1. CLogicalSelect child 0 -> Will ask the stats to be derived on Group 0
// 2. Group 0 will ask Group 4 to give the stats (as its duplicate),
// which will then again ask CLogicalSelect [0 4] to derive stats resulting in a loop.
// Such Group Expression can be ignored for deriving stats and implementation.
// Group 4 (#GExprs: 5):
// 0: CLogicalSelect [ 0 3 ]
// 1: CLogicalNAryJoin [ 6 7 8 ] Origin: (xform: CXformInlineCTEConsumerUnderSelect, Grp: 4, GrpExpr: 0)
// 2: CLogicalCTEConsumer (0), Columns: ["a" (18), "b" (19), "a" (20), "b" (21)] [ ]
// 3: CLogicalNAryJoin [ 6 7 3 ] Origin: (xform: CXformInlineCTEConsumer, Grp: 4, GrpExpr: 2)
// 4: CLogicalInnerJoin [ 6 7 3 ] Origin: (xform: CXformExpandNAryJoinGreedy, Grp: 4, GrpExpr: 3)
//
// Group 0 (#GExprs: 0, Duplicate Group: 4):
bool CGroupExpression::ContainsCircularDependencies() {
	// if it's already marked to contain circular dependency, return early
	if (m_circular_dependency == CGroupExpression::ecdCircularDependency) {
		return true;
	}
	// if exploration is completed, then the group expression does not have
	// any circular dependency
	if (m_group->FExplored()) {
		return false;
	}
	// we are still in exploration phase, check if there are any circular dependencies
	duckdb::vector<CGroup *> child_groups = m_child_groups;
	for (ULONG ul = 0; ul < child_groups.size(); ul++) {
		CGroup *child_group = child_groups[ul];
		if (child_group->m_is_scalar)
			continue;
		CGroup *child_duplicate_group = child_group->m_group_for_duplicate_groups;
		if (child_duplicate_group != nullptr) {
			ULONG child_duplicate_group_id = child_duplicate_group->m_id;
			ULONG current_group_id = m_group->m_id;
			if (child_duplicate_group_id == current_group_id) {
				m_circular_dependency = CGroupExpression::ecdCircularDependency;
				break;
			}
		}
	}
	return m_circular_dependency == CGroupExpression::ecdCircularDependency;
}
} // namespace gpopt