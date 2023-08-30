//---------------------------------------------------------------------------
//	@filename:
//		CMemo.cpp
//
//	@doc:
//		Implementation of Memo structure
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CMemo.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedProperty.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropPlan.h"
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"

#include <assert.h>
#include <list>

#define GPOPT_MEMO_HT_BUCKETS 50000

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CMemo::CMemo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMemo::CMemo() : m_id_counter(0), m_root(nullptr), m_num_groups(0), m_tree_map(nullptr) {
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::~CMemo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMemo::~CMemo() {
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::SetRoot
//
//	@doc:
//		Set root group
//
//---------------------------------------------------------------------------
void CMemo::SetRoot(CGroup *group) {
	m_root = group;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::Add
//
//	@doc:
//		Add new group to list
//
//---------------------------------------------------------------------------
void CMemo::Add(CGroup *group, Operator *expr_origin) {
	// extract expression props
	CDerivedProperty *pdp = expr_origin->m_derived_property_relation;
	ULONG id = m_id_counter++;
	{
		CGroupProxy gp(group);
		gp.SetId(id);
		gp.InitProperties(pdp);
	}
	m_groups_list.emplace_back(group);
	m_num_groups++;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::GroupInsert
//
//	@doc:
//		Helper for inserting group expression in target group
//
//---------------------------------------------------------------------------
CGroup *CMemo::GroupInsert(CGroup *target_group, CGroupExpression *group_expr, Operator *expr_origin, bool is_new) {
	auto itr = group_expr_hashmap.find(group_expr->HashValue());
	// we do a lookup since group expression may have been already inserted
	if (group_expr_hashmap.end() == itr) {
		group_expr_hashmap.insert(make_pair(group_expr->HashValue(), group_expr));
		// group proxy scope
		{
			CGroupProxy gp(target_group);
			gp.Insert(group_expr);
		}
		if (is_new) {
			Add(target_group, expr_origin);
		}
		return group_expr->m_group;
	}
	CGroupExpression *group_expr_found = itr->second;
	return group_expr_found->m_group;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::GroupInsert
//
//	@doc:
//		Helper to check if a new group needs to be created
//
//---------------------------------------------------------------------------
bool CMemo::FNewGroup(CGroup **target_group, CGroupExpression *group_expr, bool is_scalar) {
	if (nullptr == *target_group && nullptr == group_expr) {
		*target_group = new CGroup(is_scalar);
		return true;
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::GroupInsert
//
//	@doc:
//		Attempt inserting a group expression in a target group;
//		if group expression is not in the hash table, insertion
//		succeeds and the function returns the input target group;
//		otherwise insertion fails and the function returns the
//		group containing the existing group expression
//
//---------------------------------------------------------------------------
CGroup *CMemo::GroupInsert(CGroup *group_target, CGroupExpression *group_expr) {
	Operator *op = group_expr->m_operator.get();
	CGroup *group_container = nullptr;
	CGroupExpression *group_expr_found = nullptr;
	// hash table accessor's scope
	{
		auto itr = group_expr_hashmap.find(group_expr->HashValue());
		if (itr != group_expr_hashmap.end()) {
			group_expr_found = itr->second;
		}
	}
	// check if we may need to create a new group
	bool is_new = FNewGroup(&group_target, group_expr_found, false);
	if (is_new) {
		// we may add a new group to Memo, so we derive props here
		(void)op->PdpDerive();
	}
	if (nullptr != group_expr_found) {
		group_container = group_expr_found->m_group;
	} else {
		group_container = GroupInsert(group_target, group_expr, op, is_new);
	}
	// if insertion failed, release group as needed
	if (nullptr == group_expr->m_group && is_new) {
		is_new = false;
	}
	// if a new scalar group is added, we materialize a scalar expression
	// for statistics derivation purposes
	if (is_new && group_target->m_is_calar) {
		group_target->CreateDummyCostContext();
	}
	return group_container;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::ExprExtractPlan
//
//	@doc:
//		Extract a plan that delivers the given required properties
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<Operator> CMemo::ExtractPlan(CGroup *root, CRequiredPropPlan *required_property,
                                                ULONG search_stage) {
	CGroupExpression *best_group_expr;
	COptimizationContext *opt_context;
	double cost = GPOPT_INVALID_COST;
	if (root->m_is_calar) {
		// If the group has scalar expression, this group is called scalar group.
		// It has one and only one group expression, so the expression is also picked
		// up as the best expression for that group.
		// The group expression is a scalar expression, which may have 0 or multiple
		// scalar or non-scalar groups as its children.
		CGroupProxy gp(root);
		best_group_expr = *(gp.PgexprFirst());
	} else {
		// If the group does not have scalar expression, which means it has only logical
		// or physical children_expr. In this case, we lookup the best optimization context
		// for the given required plan properties, and then retrieve the best group
		// expression under the optimization context.
		opt_context = root->PocLookupBest(search_stage, required_property);
		best_group_expr = root->BestExpression(opt_context);
		if (nullptr != best_group_expr) {
			cost = opt_context->m_best_cost_context->m_cost;
		}
	}
	if (nullptr == best_group_expr) {
		// no plan found
		return nullptr;
	}
	duckdb::vector<duckdb::unique_ptr<Operator>> children_expr;
	// Get the length of groups for the best group expression
	// i.e. given the best expression is
	// 0: CScalarCmp (>=) [ 1 7 ]
	// the arity is 2, which means the best_group_expr has 2 children:
	// Group 1 and Group 7. Every single child is a CGroup.
	ULONG arity = best_group_expr->Arity();
	for (ULONG i = 0; i < arity; i++) {
		CGroup *child_group = (*best_group_expr)[i];
		CRequiredPropPlan *child_required_property = nullptr;
		// If the child group doesn't have scalar expression, we get the optimization
		// context for that child group as well as the required plan properties.
		//
		// But if the child group has scalar expression, which means it has one and
		// only one best scalar group expression, which does not need optimization,
		// because CJobGroupExpressionOptimization does not create optimization context
		// for that group. Besides, the scalar expression doesn't have plan properties.
		// In this case, the child_required_property is left to be NULL.
		if (!child_group->m_is_calar) {
			if (root->m_is_calar) {
				// In very rare case, Orca may generate the plan that a group is a scalar
				// group, but it has non-scalar sub groups. i.e.:
				// Group 7 ():  --> root->m_is_calar == true
				//   0: CScalarSubquery["?column?" (19)] [ 6 ]
				// Group 6 (#GExprs: 2): --> child_group->m_is_calar == false
				//   0: CLogicalProject [ 2 5 ]
				//   1: CPhysicalComputeScalar [ 2 5 ]
				// In the above case, because group 7 has scalar expression, Orca skipped
				// generating optimization context for group 7 and its subgroup group 6,
				// even the group 6 doesn't have scalar expression and it needs optimization.
				// Orca doesn't support this feature yet, so falls back to planner.
				assert(false);
			}
			COptimizationContext *child_opt_context = opt_context->m_best_cost_context->m_optimization_contexts[i];
			child_required_property = child_opt_context->m_required_plan_properties;
		}
		duckdb::unique_ptr<Operator> child_expr = ExtractPlan(child_group, child_required_property, search_stage);
		children_expr.emplace_back(std::move(child_expr));
	}
	duckdb::unique_ptr<Operator> expr =
	    best_group_expr->m_operator->CopyWithNewChildren(best_group_expr, std::move(children_expr), cost);
	return expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::MarkDuplicates
//
//	@doc:
//		Mark groups as duplicates
//
//---------------------------------------------------------------------------
void CMemo::MarkDuplicates(CGroup *left, CGroup *right) {
	left->AddDuplicateGrp(right);
	left->ResolveDuplicateMaster();
	right->ResolveDuplicateMaster();
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::FRehash
//
//	@doc:
//		Delete then re-insert all group expressions in memo hash table;
//		we do this at the end of exploration phase since identified
//		duplicate groups during exploration may cause changing hash values
//		of current group expressions,
//		rehashing group expressions using the new hash values allows
//		identifying duplicate group expressions that can be skipped from
//		further processing;
//
//		the function returns TRUE if rehashing resulted in discovering
//		new duplicate groups;
//
//		this function is NOT thread safe, and must not be called while
//		exploration/implementation/optimization is undergoing
//
//
//---------------------------------------------------------------------------
bool CMemo::FRehash() {
	// dump memo hash table into a local list
	list<CGroupExpression *> listGExprs;
	auto itr = group_expr_hashmap.begin();
	CGroupExpression *pgexpr;
	while (group_expr_hashmap.end() != itr) {
		pgexpr = itr->second;
		if (NULL != pgexpr) {
			itr = group_expr_hashmap.erase(itr);
			listGExprs.emplace_back(pgexpr);
		} else {
			++itr;
		}
	}
	// iterate on list and insert non-duplicate group expressions
	// back to memo hash table
	bool fNewDupGroups = false;
	while (!listGExprs.empty()) {
		pgexpr = *(listGExprs.begin());
		listGExprs.pop_front();
		CGroupExpression *pgexprFound = NULL;
		{
			// hash table accessor scope
			itr = group_expr_hashmap.find(pgexpr->HashValue());
			if (itr == group_expr_hashmap.end()) {
				// group expression has no duplicates, insert back to memo hash table
				group_expr_hashmap.insert(make_pair(pgexpr->HashValue(), pgexpr));
				continue;
			}
			pgexprFound = itr->second;
		}
		// mark duplicate group expression
		pgexpr->SetDuplicate(pgexprFound);
		CGroup *pgroup = pgexpr->m_group;
		// move group expression to duplicates list in owner group
		{
			// group proxy scope
			CGroupProxy gp(pgroup);
			gp.MoveDuplicateGExpr(pgexpr);
		}
		// check if we need also to mark duplicate groups
		CGroup *pgroupFound = pgexprFound->m_group;
		if (pgroupFound != pgroup) {
			CGroup *pgroupDup = pgroup->m_group_for_duplicate_groups;
			CGroup *pgroupFoundDup = pgroupFound->m_group_for_duplicate_groups;
			if ((nullptr == pgroupDup && nullptr == pgroupFoundDup) || (pgroupDup != pgroupFoundDup)) {
				MarkDuplicates(pgroup, pgroupFound);
				fNewDupGroups = true;
			}
		}
	}
	return fNewDupGroups;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::GroupMerge
//
//	@doc:
//		Merge duplicate groups
//
//---------------------------------------------------------------------------
void CMemo::GroupMerge() {
	// keep merging groups until we have no new duplicates
	bool has_dup_groups = true;
	while (has_dup_groups) {
		auto itr = m_groups_list.begin();
		while (m_groups_list.end() != itr) {
			CGroup *group = *itr;
			group->MergeGroup();
			++itr;
		}
		// check if root has been merged
		if (m_root->FDuplicateGroup()) {
			m_root = m_root->m_group_for_duplicate_groups;
		}
		has_dup_groups = FRehash();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::DeriveStatsIfAbsent
//
//	@doc:
//		Derive stats when no stats not present for the group
//
//---------------------------------------------------------------------------
void CMemo::DeriveStatsIfAbsent() {
	/*
	auto itr = m_groups_list.begin();
	while (m_groups_list.end() != itr)
	{
	    CGroup* pgroup = *itr;
	    ++itr;
	}
	*/
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::ResetGroupStates
//
//	@doc:
//		Reset states and job queues of memo groups
//
//---------------------------------------------------------------------------
void CMemo::ResetGroupStates() {
	auto itr = m_groups_list.begin();
	while (m_groups_list.end() != itr) {
		CGroup *group = *itr;
		group->ResetGroupState();
		group->ResetGroupJobQueues();
		group->ResetHasNewLogicalOperators();
		++itr;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::BuildTreeMap
//
//	@doc:
//		Build tree map of member group expressions
//
//---------------------------------------------------------------------------
void CMemo::BuildTreeMap(COptimizationContext *poc) {
	m_tree_map = new MemoTreeMap(gpopt::Operator::PexprRehydrate);
	m_root->BuildTreeMap(poc, NULL, gpos::ulong_max, m_tree_map);
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::ResetTreeMap
//
//	@doc:
//		Reset tree map
//
//---------------------------------------------------------------------------
void CMemo::ResetTreeMap() {
	if (nullptr != m_tree_map) {
		delete m_tree_map;
		m_tree_map = nullptr;
	}
	auto itr = m_groups_list.begin();
	while (m_groups_list.end() != itr) {
		// reset link map of all groups
		CGroup *pgroup = *itr;
		pgroup->ResetLinkMap();
		++itr;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::NumDuplicateGroups
//
//	@doc:
//		Return number of duplicate groups
//
//---------------------------------------------------------------------------
ULONG CMemo::NumDuplicateGroups() {
	ULONG ulDuplicates = 0;
	auto itr = m_groups_list.begin();
	while (m_groups_list.end() != itr) {
		CGroup *pgroup = *itr;
		if (pgroup->FDuplicateGroup()) {
			ulDuplicates++;
		}
		++itr;
	}
	return ulDuplicates;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::NumExprs
//
//	@doc:
//		Return total number of group expressions
//
//---------------------------------------------------------------------------
ULONG CMemo::NumExprs() {
	ULONG ulGExprs = 0;
	auto itr = m_groups_list.begin();
	while (m_groups_list.end() != itr) {
		CGroup *pgroup = *itr;
		ulGExprs += pgroup->m_num_exprs;
		++itr;
	}
	return ulGExprs;
}
} // namespace gpopt