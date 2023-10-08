//---------------------------------------------------------------------------
//	@filename:
//		CGroup.cpp
//
//	@doc:
//		Implementation of Memo groups; database agnostic
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CGroup.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedProperty.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtRelational.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropRelational.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/common/types/hash.hpp"

using namespace gpopt;

#define GPOPT_OPTCTXT_HT_BUCKETS 100

//---------------------------------------------------------------------------
//	@function:
//		SContextLink::SContextLink
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
SContextLink::SContextLink(CCostContext *pccParent, ULONG child_index, COptimizationContext *poc)
    : m_parent_cost_context(pccParent), m_ulChildIndex(child_index), m_poc(poc) {
}

//---------------------------------------------------------------------------
//	@function:
//		ContextLink::~SContextLink
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
SContextLink::~SContextLink() {
}

//---------------------------------------------------------------------------
//	@function:
//		SContextLink::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool SContextLink::operator==(const SContextLink &pclink2) const {
	bool fEqualChildIndexes = (this->m_ulChildIndex == pclink2.m_ulChildIndex);
	bool fEqual = false;
	if (fEqualChildIndexes) {
		if (nullptr == this->m_parent_cost_context || nullptr == pclink2.m_parent_cost_context) {
			fEqual = (nullptr == this->m_parent_cost_context && nullptr == pclink2.m_parent_cost_context);
		} else {
			fEqual = (*this->m_parent_cost_context == *pclink2.m_parent_cost_context);
		}
	}
	if (fEqual) {
		if (nullptr == this->m_poc || nullptr == pclink2.m_poc) {
			return (nullptr == this->m_poc && nullptr == pclink2.m_poc);
		}
		return COptimizationContext::Equals(*this->m_poc, *pclink2.m_poc);
	}
	return fEqual;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::CGroup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroup::CGroup(bool fScalar)
    : m_id(GPOPT_INVALID_GROUP_ID), m_is_scalar(fScalar), m_derived_properties(nullptr), m_scalar_expr(nullptr),
      m_is_scalar_expr_exact(false), m_dummy_cost_context(nullptr), m_group_for_duplicate_groups(nullptr),
      m_num_exprs(0), m_num_opt_contexts(0), m_estate(estUnexplored), m_max_opt_level(EolLow),
      m_has_new_logical_operators(false) {
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::~CGroup
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroup::~CGroup() {
	// cleaning-up group expressions
	list<CGroupExpression *>::iterator pgexpr_iter = m_group_exprs.begin();
	CGroupExpression *pgexpr = *pgexpr_iter;
	while (nullptr != pgexpr) {
		pgexpr_iter++;
		CGroupExpression *pgexprNext = *pgexpr_iter;
		pgexpr->CleanupContexts();
		pgexpr = pgexprNext;
	}
	// cleaning-up duplicate expressions
	pgexpr_iter = m_duplicate_group_exprs.begin();
	pgexpr = *pgexpr_iter;
	while (nullptr != pgexpr) {
		pgexpr_iter++;
		CGroupExpression *pgexprNext = *pgexpr_iter;
		pgexpr->CleanupContexts();
		pgexpr = pgexprNext;
	}
	// cleanup optimization contexts
	m_sht.clear();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::UpdateBestCost
//
//	@doc:
//		 Update the group expression with best cost under the given
//		 optimization context
//
//---------------------------------------------------------------------------
void CGroup::UpdateBestCost(COptimizationContext *poc, CCostContext *pcc) {
	CGroup::opt_context_hashmap_t::iterator itr;
	COptimizationContext *pocFound = nullptr;
	{
		// scope for accessor
		itr = m_sht.find(poc->HashValue());
		pocFound = itr->second;
	}
	// update best cost context
	CCostContext *pccBest = pocFound->m_best_cost_context;
	if (GPOPT_INVALID_COST != pcc->m_cost && (nullptr == pccBest || pcc->FBetterThan(pccBest))) {
		pocFound->SetBest(pcc);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookup
//
//	@doc:
//		Lookup a given context in contexts hash table
//
//---------------------------------------------------------------------------
COptimizationContext *CGroup::PocLookup(CRequiredPhysicalProp *prpp, ULONG search_stage_index) {
	duckdb::vector<ColumnBinding> v;
	COptimizationContext *poc =
	    new COptimizationContext(this, prpp, new CRequiredLogicalProp(v), search_stage_index);
	COptimizationContext *poc_found = nullptr;
	{
		auto itr = m_sht.find(poc->HashValue());
		poc_found = itr->second;
	}
	return poc_found;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookupBest
//
//	@doc:
//		Lookup the best context across all stages for the given required
//		properties
//
//---------------------------------------------------------------------------
COptimizationContext *CGroup::PocLookupBest(ULONG ul_search_stages, CRequiredPhysicalProp *required_properties) {
	COptimizationContext *poc_best = nullptr;
	CCostContext *pcc_best = nullptr;
	for (ULONG ul = 0; ul < ul_search_stages; ul++) {
		COptimizationContext *poc_current = PocLookup(required_properties, ul);
		if (nullptr == poc_current) {
			continue;
		}
		CCostContext *pcc_current = poc_current->m_best_cost_context;
		if (nullptr == pcc_best || (nullptr != pcc_current && pcc_current->FBetterThan(pcc_best))) {
			poc_best = poc_current;
			pcc_best = pcc_current;
		}
	}
	return poc_best;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::Ppoc
//
//	@doc:
//		Lookup a context by id
//
//---------------------------------------------------------------------------
COptimizationContext *CGroup::Ppoc(ULONG id) const {
	COptimizationContext *poc = nullptr;
	auto iter = m_sht.begin();
	while (iter != m_sht.end()) {
		{
			poc = iter->second;
			if (poc->m_id == id) {
				return poc;
			}
			++iter;
		}
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocInsert
//
//	@doc:
//		Insert a given context into contexts hash table only if a matching
//		context does not already exist;
//		return either the inserted or the existing matching context
//
//---------------------------------------------------------------------------
COptimizationContext *CGroup::PocInsert(COptimizationContext *poc) {
	auto itr = m_sht.find(poc->HashValue());
	if (m_sht.end() == itr) {
		poc->SetId((ULONG)IncreaseOptContextsNumber());
		m_sht.insert(make_pair(poc->HashValue(), poc));
		return poc;
	}
	COptimizationContext *pocFound = itr->second;
	return pocFound;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::BestExpression
//
//	@doc:
//		Lookup best group expression under optimization context
//
//---------------------------------------------------------------------------
CGroupExpression *CGroup::BestExpression(COptimizationContext *poc) {
	auto itr = m_sht.find(poc->HashValue());
	COptimizationContext *poc_found = itr->second;
	if (nullptr != poc_found) {
		return poc_found->BestExpression();
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetId
//
//	@doc:
//		Set group id;
//		separated from constructor to avoid synchronization issues
//
//---------------------------------------------------------------------------
void CGroup::SetId(ULONG id) {
	m_id = id;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::InitProperties
//
//	@doc:
//		Initialize group's properties
//
//---------------------------------------------------------------------------
void CGroup::InitProperties(CDerivedProperty *pdp) {
	m_derived_properties = pdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetState
//
//	@doc:
//		Set group state;
//
//---------------------------------------------------------------------------
void CGroup::SetState(EState estNewState) {
	m_estate = estNewState;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::HashValue
//
//	@doc:
//		Hash function for group identification
//
//---------------------------------------------------------------------------
size_t CGroup::HashValue() const {
	size_t id = m_id;
	if (FDuplicateGroup() && 0 == m_num_exprs) {
		// group has been merged into another group
		id = m_group_for_duplicate_groups->m_id;
	}
	return duckdb::Hash<size_t>(id);
	// return gpos::HashValue<ULONG>(&id);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::Insert
//
//	@doc:
//		Insert group expression
//
//---------------------------------------------------------------------------
void CGroup::Insert(CGroupExpression *pgexpr) {
	m_group_exprs.emplace_back(pgexpr);
	if (pgexpr->m_operator->FLogical()) {
		m_has_new_logical_operators = true;
	}
	if (pgexpr->OptimizationLevel() > m_max_opt_level) {
		m_max_opt_level = pgexpr->OptimizationLevel();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::MoveDuplicateGExpr
//
//	@doc:
//		Move duplicate group expression to duplicates list
//
//---------------------------------------------------------------------------
void CGroup::MoveDuplicateGExpr(CGroupExpression *pgexpr) {
	m_group_exprs.remove(pgexpr);
	m_num_exprs--;
	m_duplicate_group_exprs.emplace_back(pgexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FirstGroupExpr
//
//	@doc:
//		Retrieve first expression in group
//
//---------------------------------------------------------------------------
list<CGroupExpression *>::iterator CGroup::FirstGroupExpr() {
	return m_group_exprs.begin();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::NextGroupExpr
//
//	@doc:
//		Retrieve next expression in group
//
//---------------------------------------------------------------------------
list<CGroupExpression *>::iterator CGroup::NextGroupExpr(list<CGroupExpression *>::iterator pgexpr_iter) {
	return pgexpr_iter++;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchGroups
//
//	@doc:
//		Determine whether two arrays of groups are equivalent
//
//---------------------------------------------------------------------------
bool CGroup::FMatchGroups(duckdb::vector<CGroup *> pdrgpgroupFst, duckdb::vector<CGroup *> pdrgpgroupSnd) {
	ULONG arity = pdrgpgroupFst.size();
	for (ULONG i = 0; i < arity; i++) {
		CGroup *pgroupFst = pdrgpgroupFst[i];
		CGroup *pgroupSnd = pdrgpgroupSnd[i];
		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd)) {
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchNonScalarGroups
//
//	@doc:
//		 Matching of pairs of arrays of groups while skipping scalar groups
//
//---------------------------------------------------------------------------
bool CGroup::FMatchNonScalarGroups(duckdb::vector<CGroup *> pdrgpgroupFst, duckdb::vector<CGroup *> pdrgpgroupSnd) {
	if (pdrgpgroupFst.size() != pdrgpgroupSnd.size()) {
		return false;
	}
	ULONG arity = pdrgpgroupFst.size();
	for (ULONG i = 0; i < arity; i++) {
		CGroup *pgroupFst = pdrgpgroupFst[i];
		CGroup *pgroupSnd = pdrgpgroupSnd[i];
		if (pgroupFst->m_is_scalar) {
			// skip scalar groups
			continue;
		}
		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd)) {
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FDuplicateGroups
//
//	@doc:
//		Determine whether two groups are equivalent
//
//---------------------------------------------------------------------------
bool CGroup::FDuplicateGroups(CGroup *pgroupFst, CGroup *pgroupSnd) {
	CGroup *pgroupFstDup = pgroupFst->m_group_for_duplicate_groups;
	CGroup *pgroupSndDup = pgroupSnd->m_group_for_duplicate_groups;
	return (pgroupFst == pgroupSnd) || (pgroupFst == pgroupSndDup) || (pgroupSnd == pgroupFstDup) ||
	       (nullptr != pgroupFstDup && nullptr != pgroupSndDup && pgroupFstDup == pgroupSndDup);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::AddDuplicateGrp
//
//	@doc:
//		Add duplicate group
//
//---------------------------------------------------------------------------
void CGroup::AddDuplicateGrp(CGroup *pgroup) {
	// add link following monotonic ordering of group IDs
	CGroup *pgroupSrc = this;
	CGroup *pgroupDest = pgroup;
	if (this->m_id > pgroup->m_id) {
		std::swap(pgroupSrc, pgroupDest);
	}
	// keep looping until we add link
	while (pgroupSrc->m_group_for_duplicate_groups != pgroupDest) {
		if (nullptr == pgroupSrc->m_group_for_duplicate_groups) {
			pgroupSrc->m_group_for_duplicate_groups = pgroupDest;
		} else {
			pgroupSrc = pgroupSrc->m_group_for_duplicate_groups;
			if (pgroupSrc->m_id > pgroupDest->m_id) {
				std::swap(pgroupSrc, pgroupDest);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResolveDuplicateMaster
//
//	@doc:
//		Resolve master duplicate group
//
//---------------------------------------------------------------------------
void CGroup::ResolveDuplicateMaster() {
	if (!FDuplicateGroup()) {
		return;
	}
	CGroup *pgroupTarget = m_group_for_duplicate_groups;
	while (nullptr != pgroupTarget->m_group_for_duplicate_groups) {
		pgroupTarget = pgroupTarget->m_group_for_duplicate_groups;
	}
	// update reference to target group
	m_group_for_duplicate_groups = pgroupTarget;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::MergeGroup
//
//	@doc:
//		Merge group with its duplicate - not thread-safe
//
//---------------------------------------------------------------------------
void CGroup::MergeGroup() {
	if (!FDuplicateGroup()) {
		return;
	}
	// resolve target group
	ResolveDuplicateMaster();
	CGroup *pgroupTarget = m_group_for_duplicate_groups;
	// move group expressions from this group to target
	while (!m_group_exprs.empty()) {
		CGroupExpression *pgexpr = m_group_exprs.front();
		m_group_exprs.pop_front();
		m_num_exprs--;
		pgexpr->Reset(pgroupTarget, pgroupTarget->m_num_exprs++);
		pgroupTarget->Insert(pgexpr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::CreateDummyCostContext
//
//	@doc:
//		Create a dummy cost context attached to the first group expression,
//		used for plan enumeration for scalar groups
//
//
//---------------------------------------------------------------------------
void CGroup::CreateDummyCostContext() {
	CGroupExpression *pgexprFirst;
	{
		CGroupProxy gp(this);
		pgexprFirst = *(gp.PgexprFirst());
	}
	duckdb::vector<ColumnBinding> v;
	COptimizationContext *poc =
	    new COptimizationContext(this, CRequiredPhysicalProp::PrppEmpty(), new CRequiredLogicalProp(v), 0);
	m_dummy_cost_context = new CCostContext(poc, 0, pgexprFirst);
	m_dummy_cost_context->SetState(CCostContext::estCosting);
	m_dummy_cost_context->SetCost(0.0);
	m_dummy_cost_context->SetState(CCostContext::estCosted);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::RecursiveBuildTreeMap
//
//	@doc:
//		Find all cost contexts of current group expression that carry valid
//		implementation of the given optimization context,
//		for all such cost contexts, introduce a link from parent cost context
//		to child cost context and then process child groups recursively
//
//
//---------------------------------------------------------------------------
void CGroup::RecursiveBuildTreeMap(
    COptimizationContext *poc, CCostContext *pccParent, CGroupExpression *pgexprCurrent, ULONG child_index,
    CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals> *ptmap) {
	duckdb::vector<CCostContext *> pdrgpcc = pgexprCurrent->LookupAllMatchedCostContexts(poc);
	const ULONG ulCCSize = pdrgpcc.size();
	if (0 == ulCCSize) {
		// current group expression has no valid implementations of optimization context
		return;
	}
	// iterate over all valid implementations of given optimization context
	for (ULONG ulCC = 0; ulCC < ulCCSize; ulCC++) {
		CCostContext *pccCurrent = pdrgpcc[ulCC];
		if (nullptr != pccParent) {
			// link parent cost context to child cost context
			ptmap->Insert(pccParent, child_index, pccCurrent);
		}
		duckdb::vector<COptimizationContext *> pdrgpoc = pccCurrent->m_optimization_contexts;
		if (0 != pdrgpoc.size()) {
			// process children recursively
			const ULONG arity = pgexprCurrent->Arity();
			for (ULONG ul = 0; ul < arity; ul++) {
				CGroup *pgroupChild = (*pgexprCurrent)[ul];
				COptimizationContext *pocChild = nullptr;
				if (!pgroupChild->m_is_scalar) {
					pocChild = pdrgpoc[ul];
				}
				pgroupChild->BuildTreeMap(pocChild, pccCurrent, ul, ptmap);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::BuildTreeMap
//
//	@doc:
//		Given a parent cost context and an optimization context,
//		link parent cost context to all cost contexts in current group
//		that carry valid implementation of the given optimization context
//
//
//---------------------------------------------------------------------------
void CGroup::BuildTreeMap(
    COptimizationContext *poc, CCostContext *pccParent, ULONG child_index,
    CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals> *ptmap) {
	// check if link has been processed before,
	// this is crucial to eliminate unnecessary recursive calls
	SContextLink pclink(pccParent, child_index, poc);
	if (m_link_map.find(pclink.HashValue()) != m_link_map.end()) {
		// link is already processed
		return;
	}
	list<CGroupExpression *>::iterator itr;
	// start with first non-logical group expression
	CGroupExpression *pgexprCurrent = nullptr;
	{
		CGroupProxy gp(this);
		itr = gp.m_pgroup->m_group_exprs.begin();
		itr = gp.PgexprSkipLogical(itr);
		pgexprCurrent = *itr;
	}
	while (m_group_exprs.end() != itr) {
		if (pgexprCurrent->m_operator->FPhysical()) {
			// create links recursively
			RecursiveBuildTreeMap(poc, pccParent, pgexprCurrent, child_index, ptmap);
		} else {
			// this is a scalar group, link parent cost context to group's dummy context
			ptmap->Insert(pccParent, child_index, m_dummy_cost_context);
			// recursively link group's dummy context to child contexts
			const ULONG arity = pgexprCurrent->Arity();
			for (ULONG ul = 0; ul < arity; ul++) {
				CGroup *pgroupChild = (*pgexprCurrent)[ul];
				pgroupChild->BuildTreeMap(nullptr, m_dummy_cost_context, ul, ptmap);
			}
		}
		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			itr = gp.PgexprSkipLogical(itr);
			pgexprCurrent = *itr;
		}
	}
	// remember processed links to avoid re-processing them later
	m_link_map.insert(make_pair(pclink.HashValue(), true));
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::BestPromiseGroupExpr
//
//	@doc:
//		Find group expression with best stats promise and the
//		same children as given expression
//
//---------------------------------------------------------------------------
CGroupExpression *CGroup::BestPromiseGroupExpr(CGroupExpression *pgexprToMatch) {
	duckdb::vector<ColumnBinding> v;
	CGroupExpression *pgexprCurrent = nullptr;
	CGroupExpression *pgexprBest = nullptr;
	list<CGroupExpression *>::iterator itr;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		itr = gp.m_pgroup->m_group_exprs.begin();
		itr = gp.PgexprNextLogical(itr);
		pgexprCurrent = *itr;
	}
	while (m_group_exprs.end() != itr) {
		if (pgexprCurrent->FMatchNonScalarChildren(pgexprToMatch)) {
			pgexprBest = pgexprCurrent;
		}
		// move to next logical group expression
		{
			CGroupProxy gp(this);
			++itr;
			itr = gp.PgexprNextLogical(itr);
			pgexprCurrent = *itr;
		}
	}
	return pgexprBest;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupState
//
//	@doc:
//		Reset group state;
//		resetting state is not thread-safe
//
//---------------------------------------------------------------------------
void CGroup::ResetGroupState() {
	// reset group expression states
	list<CGroupExpression *>::iterator pgexpr_iter = m_group_exprs.begin();
	while (m_group_exprs.end() != pgexpr_iter) {
		CGroupExpression *pgexpr = *pgexpr_iter;
		pgexpr->ResetState();
		pgexpr_iter++;
	}
	// reset group state
	{
		CGroupProxy gp(this);
		m_estate = estUnexplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetLinkMap
//
//	@doc:
//		Reset link map for plan enumeration;
//		this operation is not thread safe
//
//---------------------------------------------------------------------------
void CGroup::ResetLinkMap() {
	m_link_map.clear();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupJobQueues
//
//	@doc:
//		Reset group job queues;
//
//---------------------------------------------------------------------------
void CGroup::ResetGroupJobQueues() {
	CGroupProxy gp(this);
	m_explore_job_queue.Reset();
	m_impl_job_queue.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::CostLowerBound
//
//	@doc:
//		Compute a cost lower bound on plans, rooted by a group expression
//		in current group, and satisfying the given required properties
//
//---------------------------------------------------------------------------
double CGroup::CostLowerBound(CRequiredPhysicalProp *prppInput) {
	auto iter = m_cost_lower_bounds_map.find(prppInput);
	double pcostLowerBound = GPOPT_INFINITE_COST;
	if (m_cost_lower_bounds_map.end() != iter) {
		pcostLowerBound = iter->second;
		return pcostLowerBound;
	}
	double costLowerBound = GPOPT_INFINITE_COST;
	// start with first non-logical group expression
	CGroupExpression *pgexprCurrent = nullptr;
	list<CGroupExpression *>::iterator itr;
	{
		CGroupProxy gp(this);
		itr = gp.m_pgroup->m_group_exprs.begin();
		itr = gp.PgexprSkipLogical(itr);
		pgexprCurrent = *itr;
	}
	while (m_group_exprs.end() != itr) {
		// considering an enforcer introduces a deadlock here since its child is
		// the same group that contains it,
		// since an enforcer must reside on top of another operator from the same
		// group, it cannot produce a better cost lower-bound and can be skipped here
		if (!CUtils::FEnforcer(pgexprCurrent->m_operator.get())) {
			double costLowerBoundGExpr = pgexprCurrent->CostLowerBound(prppInput, nullptr, gpos::ulong_max);
			if (costLowerBoundGExpr < costLowerBound) {
				costLowerBound = costLowerBoundGExpr;
			}
		}
		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			itr = gp.PgexprSkipLogical(itr);
			pgexprCurrent = *itr;
		}
	}
	m_cost_lower_bounds_map.insert(map<CRequiredPhysicalProp *, double>::value_type(prppInput, costLowerBound));
	return costLowerBound;
}