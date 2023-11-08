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
	size_t operator()(const duckdb::unique_ptr<CGroupExpression> gexpr) const {
		size_t ulHash = gexpr->m_operator->HashValue();
		size_t arity = gexpr->m_child_groups.size();
		for (size_t i = 0; i < arity; i++) {
			ulHash = CombineHashes(ulHash, gexpr->m_child_groups[i]->HashValue());
		}
		return ulHash;
	}
};

struct CGroupExpressionCmp {
	size_t operator()(const duckdb::unique_ptr<CGroupExpression> gexpr1, const duckdb::unique_ptr<CGroupExpression> gexpr2) const {
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
	duckdb::unique_ptr<CGroup> m_root;

	// number of groups
	ULONG_PTR m_num_groups;

	// tree map of member group expressions
	duckdb::unique_ptr<MemoTreeMap> m_tree_map;

	// list of groups
	list<duckdb::unique_ptr<CGroup>> m_groups_list;

	// hashtable of all group expressions
	unordered_map<duckdb::unique_ptr<CGroupExpression>, duckdb::unique_ptr<CGroupExpression>, CGroupExpressionHash, CGroupExpressionCmp> group_expr_hashmap;

public:
	// set root group
	void SetRoot(duckdb::unique_ptr<CGroup> group);

	// return root group
	duckdb::unique_ptr<CGroup> GroupRoot() const {
		return m_root;
	}

	// insert group expression into hash table
	duckdb::unique_ptr<CGroup> GroupInsert(duckdb::unique_ptr<CGroup> group_target, duckdb::unique_ptr<CGroupExpression> group_expr);
	
	// mark groups as duplicates
	void MarkDuplicates(duckdb::unique_ptr<CGroup> left, duckdb::unique_ptr<CGroup> right);
	
	// merge duplicate groups
	void GroupMerge();

	// reset states of all memo groups
	void ResetGroupStates();

	// extract a plan that delivers the given required properties
	duckdb::unique_ptr<Operator> ExtractPlan(duckdb::unique_ptr<CGroup> root,
											 duckdb::unique_ptr<CRequiredPhysicalProp> required_property, ULONG search_stage);

	// return number of groups
	ULONG_PTR NumGroups() const {
		return m_num_groups;
	}

	// return total number of group expressions
	ULONG NumExprs();

	// return number of duplicate groups
	ULONG NumDuplicateGroups();

	// build tree map
	void BuildTreeMap(duckdb::unique_ptr<COptimizationContext> poc);

	// reset tree map
	void ResetTreeMap();

	// return tree map
	duckdb::unique_ptr<MemoTreeMap> TreeMap() const {
		return m_tree_map;
	}

private:
	// add new group
	void Add(duckdb::unique_ptr<CGroup> group, duckdb::unique_ptr<Operator> expr_origin);
	
	// helper for inserting group expression in target group
	duckdb::unique_ptr<CGroup> GroupInsert(duckdb::unique_ptr<CGroup> target_group,
						duckdb::unique_ptr<CGroupExpression> group_expr,
						duckdb::unique_ptr<Operator> expr_origin, bool is_new);

	// helper to check if a new group needs to be created
	bool FNewGroup(duckdb::unique_ptr<CGroup> *target_group, duckdb::unique_ptr<CGroupExpression> group_expr, bool is_scalar);
	
	// rehash all group expressions after group merge - not thread-safe
	bool FRehash();
	
	// derive stats when no stats not present for the group
	void DeriveStatsIfAbsent();
}; // class CMemo
} // namespace gpopt