//---------------------------------------------------------------------------
//	@filename:
//		CMemo.h
//
//	@doc:
//		Memo lookup table for dynamic programming
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncList.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

#include <list>

namespace gpopt {
class CGroup;
class CDerivedProperty;
class CDrvdPropCtxtPlan;
class CMemoProxy;
class COptimizationContext;

// memo tree map definition
typedef CTreeMap<CCostContext, gpopt::Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals>
    MemoTreeMap;

using namespace gpos;

struct CGroupExpressionHash {
	size_t operator()(const CGroupExpression* gexpr) const {
		size_t ulHash = gexpr->m_operator->HashValue();
		size_t arity = gexpr->m_child_groups.size();
		for (size_t i = 0; i < arity; i++) {
			ulHash = CombineHashes(ulHash, gexpr->m_child_groups[i]->HashValue());
		}
		return ulHash;
	}
};

struct CGroupExpressionCmp {
	size_t operator()(const CGroupExpression* gexpr1, const CGroupExpression* gexpr2) const {
		// make sure we are not comparing to invalid group expression
		if (nullptr == gexpr1->m_operator || nullptr == gexpr2->m_operator) {
			return nullptr == gexpr1->m_operator && nullptr == gexpr2->m_operator;
		}
		// have same arity
		if (gexpr1->Arity() != gexpr2->Arity()) {
			return false;
		}
		// match operators
		if (!(gexpr1->m_operator->logical_type == gexpr2->m_operator->logical_type)
			|| !(gexpr1->m_operator->physical_type == gexpr2->m_operator->physical_type)) {
			return false;
		}
		// compare inputs
		if (0 == gexpr1->Arity()) {
			return true;
		} else {
			if (1 == gexpr1->Arity() || gexpr1->m_operator->FInputOrderSensitive()) {
				return CGroup::FMatchGroups(gexpr1->m_child_groups, gexpr2->m_child_groups);
			} else {
				return CGroup::FMatchGroups(gexpr1->m_child_groups_sorted, gexpr2->m_child_groups_sorted);
			}
		}
		return false;
	}
};

//---------------------------------------------------------------------------
//	@class:
//		CMemo
//
//	@doc:
//		Dynamic programming table
//
//---------------------------------------------------------------------------
class CMemo {
public:
	explicit CMemo();
	CMemo(const CMemo &) = delete;
	~CMemo();

	// id counter for groups
	ULONG m_id_counter;
	// root group
	CGroup *m_root;
	// number of groups
	ULONG_PTR m_num_groups;
	// tree map of member group expressions
	MemoTreeMap *m_tree_map;
	// list of groups
	list<CGroup *> m_groups_list;
	// hashtable of all group expressions
	unordered_map<CGroupExpression*, CGroupExpression*, CGroupExpressionHash, CGroupExpressionCmp> group_expr_hashmap;

public:
	// set root group
	void SetRoot(CGroup *group);
	// return root group
	CGroup *GroupRoot() const {
		return m_root;
	}
	// insert group expression into hash table
	CGroup *GroupInsert(CGroup *group_target, CGroupExpression *group_expr);
	// mark groups as duplicates
	void MarkDuplicates(CGroup *left, CGroup *right);
	// merge duplicate groups
	void GroupMerge();
	// reset states of all memo groups
	void ResetGroupStates();
	// extract a plan that delivers the given required properties
	duckdb::unique_ptr<Operator> ExtractPlan(CGroup *root, CRequiredPhysicalProp *required_property, ULONG search_stage);

	// return number of groups
	ULONG_PTR NumGroups() const {
		return m_num_groups;
	}
	// return total number of group expressions
	ULONG NumExprs();
	// return number of duplicate groups
	ULONG NumDuplicateGroups();

	// build tree map
	void BuildTreeMap(COptimizationContext *poc);
	// reset tree map
	void ResetTreeMap();
	// return tree map
	MemoTreeMap *TreeMap() const {
		return m_tree_map;
	}

private:
	// add new group
	void Add(CGroup *group, Operator *expr_origin);
	// helper for inserting group expression in target group
	CGroup *GroupInsert(CGroup *target_group, CGroupExpression *group_expr, Operator *expr_origin, bool is_new);
	// helper to check if a new group needs to be created
	bool FNewGroup(CGroup **target_group, CGroupExpression *group_expr, bool is_scalar);
	// rehash all group expressions after group merge - not thread-safe
	bool FRehash();
	// derive stats when no stats not present for the group
	void DeriveStatsIfAbsent();
}; // class CMemo
} // namespace gpopt