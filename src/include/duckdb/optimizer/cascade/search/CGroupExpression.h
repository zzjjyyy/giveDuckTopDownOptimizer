//---------------------------------------------------------------------------
//	@filename:
//		CGroupExpression.h
//
//	@doc:
//		Equivalent of CExpression inside Memo structure
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/engine/CPartialPlan.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/search/CBinding.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

#define GPOPT_INVALID_GEXPR_ID gpos::ulong_max

namespace gpopt {
using namespace gpos;
//---------------------------------------------------------------------------
//	@class:
//		CGroupExpression
//
//	@doc:
//		Expression representation inside Memo structure
//
//---------------------------------------------------------------------------
class CGroupExpression {
public:
	// dummy ctor; used for creating invalid gexpr
	CGroupExpression()
	    : m_id(GPOPT_INVALID_GEXPR_ID), m_xform_id_origin(CXform::ExfInvalid), m_intermediate(false),
	      m_estate(estUnexplored), m_eol(EolLow) {
	}
	CGroupExpression(duckdb::unique_ptr<Operator> op, duckdb::vector<CGroup *> groups, CXform::EXformId xform_id,
	                 CGroupExpression *group_expr_origin, bool fIntermediate);
	CGroupExpression(const CGroupExpression &) = delete;
	virtual ~CGroupExpression();

	// states of a group expression
	enum EState { estUnexplored, estExploring, estExplored, estImplementing, estImplemented, estSentinel };
	// circular dependency state
	enum ECircularDependency { ecdDefault, ecdCircularDependency, ecdSentinel };
	// type definition of cost context hash table
	typedef unordered_map<ULONG, CCostContext *> CostContextMap;

public:
	// expression id
	ULONG m_id;
	// duplicate group expression
	CGroupExpression *m_duplicate_group_expr;
	// operator class
	duckdb::unique_ptr<Operator> m_operator;
	// array of child groups
	duckdb::vector<CGroup *> m_child_groups;
	// sorted array of children groups for faster comparison
	// of order-insensitive operators
	duckdb::vector<CGroup *> m_child_groups_sorted;
	// back pointer to group
	CGroup *m_group;
	// id of xform that generated group expression
	CXform::EXformId m_xform_id_origin;
	// group expression that generated current group expression via xform
	CGroupExpression *m_group_expr_origin;
	// flag to indicate if group expression was created as a node at some
	// intermediate level when origin expression was inserted to memo
	bool m_intermediate;
	// state of group expression
	EState m_estate;
	// optimization level
	EOptimizationLevel m_eol;
	// map of partial plans to their cost lower bound
	unordered_map<ULONG, double> m_partial_plan_cost_map;
	// circular dependency state
	ECircularDependency m_circular_dependency;
	// hashtable of cost contexts
	CostContextMap m_cost_context_map;

public:
	// set group back pointer
	void SetGroup(CGroup *pgroup);
	// set group expression id
	void SetId(ULONG id);
	// preprocessing before applying transformation
	void PreprocessTransform(CXform *pxform);
	// postprocessing after applying transformation
	void PostprocessTransform(CXform *pxform);
	// costing scheme
	double CostCompute(CCostContext *pcc) const;
	// set optimization level of group expression
	void SetOptimizationLevel();
	// check validity of group expression
	bool FValidContext(COptimizationContext *poc, duckdb::vector<COptimizationContext *> child_optimization_contexts);
	// remove cost context in hash table
	CCostContext *CostContextRemove(COptimizationContext *poc, ULONG id);
	// insert given context in hash table only if a better context does not exist, return the context that is kept it in
	// hash table
	CCostContext *CostContextInsertBest(CCostContext *pcc);

public:
	// set duplicate group expression
	void SetDuplicate(CGroupExpression *group_expr) {
		m_duplicate_group_expr = group_expr;
	}
	// cleanup cost contexts
	void CleanupContexts();
	// check if cost context already exists in group expression hash table
	bool FCostContextExists(COptimizationContext *poc, duckdb::vector<COptimizationContext *> optimization_contexts);
	// compute and store expression's cost under a given context
	CCostContext *PccComputeCost(COptimizationContext *poc, ULONG optimization_request_num,
	                             duckdb::vector<COptimizationContext *> optimization_contexts, bool fPruned,
	                             double cost_lower_bound);
	// compute a cost lower bound for plans, rooted by current group expression, and satisfying the given required
	// properties
	double CostLowerBound(CRequiredPropPlan *input_required_prop_plan, CCostContext *child_cost_context,
	                      ULONG child_index);
	// initialize group expression
	void Init(CGroup *pgroup, ULONG id);
	// reset group expression
	void Reset(CGroup *pgroup, ULONG id) {
		m_group = pgroup;
		m_id = id;
	}
	// optimization level accessor
	EOptimizationLevel OptimizationLevel() const {
		return m_eol;
	}
	// shorthand to access children
	CGroup *operator[](ULONG pos) const {
		CGroup *pgroup = m_child_groups[pos];
		// during optimization, the operator returns the duplicate group;
		// in exploration and implementation the group may contain
		// group expressions that have not been processed yet;
		if (0 == pgroup->m_num_exprs) {
			return pgroup->m_group_for_duplicate_groups;
		}
		return pgroup;
	};
	// arity function
	ULONG Arity() const {
		return m_child_groups.size();
	}
	// comparison operator for hash tables
	bool operator==(const CGroupExpression &group_expr) const {
		return group_expr.Matches(this);
	}
	// equality function for hash table
	static bool Equals(const CGroupExpression &left, const CGroupExpression &right) {
		return left == right;
	}
	// match group expression against given operator and its children
	bool Matches(const CGroupExpression *group_expr) const;
	// match non-scalar children of group expression against given children of passed expression
	bool FMatchNonScalarChildren(CGroupExpression *group_expr) const;
	// hash function
	ULONG HashValue() const {
		return HashValue(m_operator.get(), m_child_groups);
	}
	// static hash function for operator and group references
	static ULONG HashValue(Operator *pop, duckdb::vector<CGroup *> groups);
	// static hash function for group expression
	static ULONG HashValue(const CGroupExpression &);
	// transform group expression
	void Transform(CXform *pxform, CXformResult *results, ULONG *elapsed_time, ULONG *num_bindings);
	// set group expression state
	void SetState(EState state);
	// reset group expression state
	void ResetState();
	// check if group expression has been explored
	bool FExplored() const {
		return (estExplored <= m_estate);
	}
	// check if group expression has been implemented
	bool FImplemented() const {
		return (estImplemented == m_estate);
	}
	// check if transition to the given state is completed
	bool FTransitioned(EState estate) const;
	// lookup cost context in hash table
	CCostContext *CostContextLookup(COptimizationContext *poc, ULONG optimization_request_num);
	// lookup all cost contexts matching given optimization context
	duckdb::vector<CCostContext *> LookupAllMatchedCostContexts(COptimizationContext *poc);
	// insert a cost context in hash table
	CCostContext *CostContextInsert(CCostContext *pcc);
	// link for list in Group
	SLink m_link_group;
	// link for group expression hash table
	SLink m_link_memo;
	// invalid group expression
	static const CGroupExpression M_INVALID_GROUP_EXPR;

	virtual bool ContainsCircularDependencies();
}; // class CGroupExpression
} // namespace gpopt