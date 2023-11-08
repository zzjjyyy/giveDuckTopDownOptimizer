//---------------------------------------------------------------------------
//	@filename:
//		CGroup.h
//
//	@doc:
//		Group of equivalent expressions in the Memo structure
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/common/CSyncList.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CTreeMap.h"

#include <list>
#include <unordered_map>

#define GPOPT_INVALID_GROUP_ID gpos::ulong_max

namespace gpopt {
using namespace gpos;
// forward declarations
class CGroup;
class CGroupExpression;
class CDerivedProperty;
class CDrvdPropCtxtPlan;
class CGroupProxy;
class COptimizationContext;
class CRequiredPhysicalProp;
class CRequiredLogicalProp;

// optimization levels in ascending order,
// under a given optimization context, group expressions in higher levels
// must be optimized before group expressions in lower levels,
// a group expression sets its level in CGroupExpression::SetOptimizationLevel()
enum EOptimizationLevel { EolLow = 0, EolHigh, EolSentinel };

//---------------------------------------------------------------------------
//	@class:
//		SContextLink
//
//	@doc:
//		Internal structure to remember processed links in plan enumeration
//
//---------------------------------------------------------------------------
struct SContextLink {
public:
	// cost context in a parent group
	duckdb::unique_ptr<CCostContext> m_parent_cost_context;
	// index used when treating current group as a child of group expression
	ULONG m_ulChildIndex;
	// optimization context used to locate group expressions in
	// current group to be linked with parent group expression
	duckdb::unique_ptr<COptimizationContext> m_poc;

public:
	// ctor
	SContextLink(duckdb::unique_ptr<CCostContext> pccParent, ULONG child_index, duckdb::unique_ptr<COptimizationContext> poc);

	// dtor
	virtual ~SContextLink();

	// equality function
	bool operator==(const SContextLink &pclink2) const;

	size_t HashValue() const {
		size_t ulHashPcc = 0;
		if (NULL != m_parent_cost_context) {
			ulHashPcc = m_parent_cost_context->HashValue();
		}
		size_t ulHashPoc = 0;
		if (NULL != m_poc) {
			ulHashPoc = m_poc->HashValue();
		}
		return CombineHashes(m_ulChildIndex, CombineHashes(ulHashPcc, ulHashPoc));
	}
}; // struct SContextLink

//---------------------------------------------------------------------------
//	@class:
//		CGroup
//
//	@doc:
//		Group of equivalent expressions in the Memo structure
//
//---------------------------------------------------------------------------
class CGroup {
	friend class CGroupProxy;

public:
	// type definition of optimization context hash table
	typedef unordered_map<ULONG, duckdb::unique_ptr<COptimizationContext>> opt_context_hashmap_t;

	// states of a group
	enum EState {
		estUnexplored,
		estExploring,
		estExplored,
		estImplementing,
		estImplemented,
		estOptimizing,
		estOptimized,
		estSentinel
	};

public:
	explicit CGroup(bool fScalar = false);
	CGroup(const CGroup &) = delete;
	~CGroup();

	// id is used when printing memo contents
	ULONG m_id;

	// true if group hold scalar expressions
	bool m_is_scalar;

	// join keys for outer child (only for scalar groups) (used by hash & merge joins)
	duckdb::vector<duckdb::unique_ptr<Expression>> m_join_keys_outer;

	// join keys for inner child (only for scalar groups) (used by hash & merge joins)
	duckdb::vector<duckdb::unique_ptr<Expression>> m_join_keys_inner;

	// list of group expressions
	std::list<duckdb::unique_ptr<CGroupExpression>> m_group_exprs;

	// list of duplicate group expressions identified by group merge
	// std::list<CGroupExpression *> m_duplicate_group_exprs;

	// group derived properties
	duckdb::unique_ptr<CDerivedProperty> m_derived_properties;

	// scalar expression for stat derivation (subqueries substituted with a dummy)
	duckdb::unique_ptr<Expression> m_scalar_expr;

	// scalar expression above is exactly the same as the scalar expr in the group
	bool m_is_scalar_expr_exact;

	// dummy cost context used in scalar groups for plan enumeration
	duckdb::unique_ptr<CCostContext> m_dummy_cost_context;

	// pointer to group containing the group expressions
	// of all duplicate groups
	duckdb::unique_ptr<CGroup> m_group_for_duplicate_groups;

	// map of processed links
	unordered_map<ULONG, bool> m_link_map;

	// hashtable of optimization contexts
	opt_context_hashmap_t m_sht;

	// number of group expressions
	ULONG m_num_exprs;

	// map of cost lower bounds
	std::map<duckdb::unique_ptr<CRequiredPhysicalProp>, double> m_cost_lower_bounds_map;

	// number of optimization contexts
	ULONG_PTR m_num_opt_contexts;

	// current state
	EState m_estate;

	// maximum optimization level of member group expressions
	EOptimizationLevel m_max_opt_level;

	// were new logical operators added to the group?
	bool m_has_new_logical_operators;

	// exploration job queue
	CJobQueue m_explore_job_queue;

	// implementation job queue
	CJobQueue m_impl_job_queue;

public:
	// find a context by id
	duckdb::unique_ptr<COptimizationContext> Ppoc(ULONG id) const;

	// insert given context into contexts hash table
	duckdb::unique_ptr<COptimizationContext> PocInsert(duckdb::unique_ptr<COptimizationContext> poc);

	// lookup the best context across all stages for the given required properties
	duckdb::unique_ptr<COptimizationContext>
	PocLookupBest(duckdb::unique_ptr<CGroup> this_group,
				  ULONG ulSearchStages,
				  duckdb::unique_ptr<CRequiredPhysicalProp> required_properties);

	// cleanup optimization contexts on destruction
	void CleanupContexts();

	// increment number of optimization contexts
	ULONG_PTR IncreaseOptContextsNumber() {
		return m_num_opt_contexts++;
	}

	// the following functions are only accessed through group proxy
	// setter of group id
	void SetId(ULONG id);

	// setter of group state
	void SetState(EState estNewState);

	// insert new group expression
	void Insert(duckdb::unique_ptr<CGroupExpression> pgexpr);

	// move duplicate group expression to duplicates list
	void MoveDuplicateGExpr(duckdb::unique_ptr<CGroupExpression> pgexpr);

	// initialize group's properties
	void InitProperties(duckdb::unique_ptr<CDerivedProperty> pdp);

	// retrieve first group expression
	list<duckdb::unique_ptr<CGroupExpression>>::iterator FirstGroupExpr();

	// retrieve next group expression
	list<duckdb::unique_ptr<CGroupExpression>>::iterator
	NextGroupExpr(list<duckdb::unique_ptr<CGroupExpression>>::iterator pgexpr_iter);

	// find the group expression having the best stats promise
	duckdb::unique_ptr<CGroupExpression>
	PgexprBestPromise(duckdb::unique_ptr<CRequiredLogicalProp> prprelInput);

	// lookup best expression under given optimization context
	duckdb::unique_ptr<CGroupExpression>
	BestExpression(duckdb::unique_ptr<COptimizationContext> poc);

	// hash function
	size_t HashValue() const;

	// has group been explored?
	bool FExplored() const {
		return estExplored <= m_estate;
	}

	// has group been implemented?
	bool FImplemented() const {
		return estImplemented <= m_estate;
	}

	// has group been optimized?
	bool FOptimized() const {
		return estOptimized <= m_estate;
	}

	// check if group has duplicates
	bool FDuplicateGroup() const {
		return NULL != m_group_for_duplicate_groups;
	}

	// reset has new logical operators flag
	void ResetHasNewLogicalOperators() {
		m_has_new_logical_operators = false;
	}

	// reset group state
	void ResetGroupState(duckdb::unique_ptr<CGroup> this_group);

	// reset group job queues
	void ResetGroupJobQueues(duckdb::unique_ptr<CGroup> this_group);

	// resolve master duplicate group;
	// this is the group that will host all expressions in current group after merging
	void ResolveDuplicateMaster();

	// add duplicate group
	void AddDuplicateGrp(duckdb::unique_ptr<CGroup> this_group, duckdb::unique_ptr<CGroup> pgroup);

	// merge group with its duplicate - not thread-safe
	void MergeGroup();

	// update the best group cost under the given optimization context
	void UpdateBestCost(duckdb::unique_ptr<COptimizationContext> poc, duckdb::unique_ptr<CCostContext> pcc);

	// materialize a dummy cost context attached to the first group expression
	void CreateDummyCostContext(duckdb::unique_ptr<CGroup> this_group);

	// find group expression with best stats promise and the same given children
	duckdb::unique_ptr<CGroupExpression>
	BestPromiseGroupExpr(duckdb::unique_ptr<CGroup> this_group, 
						 duckdb::unique_ptr<CGroupExpression> pgexprToMatch);

	// link parent group expression to group members
	void BuildTreeMap(duckdb::unique_ptr<CGroup> this_group,
	    duckdb::unique_ptr<COptimizationContext> poc, duckdb::unique_ptr<CCostContext> pccParent, ULONG child_index,
	    duckdb::unique_ptr<CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals>> ptmap);
	
	// reset link map used in plan enumeration
	void ResetLinkMap();

	// compute cost lower bound for the plan satisfying given required properties
	double CostLowerBound(duckdb::unique_ptr<CGroup> this_group, duckdb::unique_ptr<CRequiredPhysicalProp> prppInput);

	// matching of pairs of arrays of groups
	static bool FMatchGroups(duckdb::vector<duckdb::unique_ptr<CGroup>> pdrgpgroupFst,
							 duckdb::vector<duckdb::unique_ptr<CGroup>> pdrgpgroupSnd);
	
	// matching of pairs of arrays of groups while skipping scalar groups
	static bool FMatchNonScalarGroups(duckdb::vector<duckdb::unique_ptr<CGroup>> pdrgpgroupFst,
									  duckdb::vector<duckdb::unique_ptr<CGroup>> pdrgpgroupSnd);
	
	// determine if a pair of groups are duplicates
	static bool FDuplicateGroups(duckdb::unique_ptr<CGroup> pgroupFst, duckdb::unique_ptr<CGroup> pgroupSnd);

private:
	// lookup a given context in contexts hash table
	duckdb::unique_ptr<COptimizationContext>
	PocLookup(duckdb::unique_ptr<CGroup> this_group,
			  duckdb::unique_ptr<CRequiredPhysicalProp> prpp,
			  ULONG ulSearchStageIndex);

	// helper function to add links in child groups
	void RecursiveBuildTreeMap(
	    duckdb::unique_ptr<COptimizationContext> poc, duckdb::unique_ptr<CCostContext> pccParent,
		duckdb::unique_ptr<CGroupExpression> pgexprCurrent, ULONG child_index,
	    duckdb::unique_ptr<CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals>> ptmap);
}; // class CGroup
} // namespace gpopt