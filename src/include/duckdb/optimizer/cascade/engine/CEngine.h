//---------------------------------------------------------------------------
//	@filename:
//		CEngine.h
//
//	@doc:
//		Optimization engine
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COrderProperty.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"
#include "duckdb/planner/logical_operator.hpp"

using namespace gpos;
using namespace duckdb;

namespace gpopt {
// forward declarations
class CGroup;
class CJob;
class CJobFactory;
class CQueryContext;
class COptimizationContext;
class CRequiredPhysicalProp;
class CRequiredLogicalProp;
class CEnumeratorConfig;

//---------------------------------------------------------------------------
//	@class:
//		CEngine
//
//	@doc:
//		Optimization engine; owns entire optimization workflow
//
//---------------------------------------------------------------------------
class CEngine {
public:
	explicit CEngine();
	CEngine(const CEngine &) = delete;
	~CEngine();

	// query context
	CQueryContext *m_query_context;
	// search strategy
	duckdb::vector<CSearchStage *> m_search_strategy;
	// index of current search stage
	ULONG m_current_search_stage;
	// memo table
	CMemo *m_memo_table;
	// pattern used for adding enforcers
	duckdb::unique_ptr<Operator> m_expr_enforcer_pattern;

	// the following variables are used for maintaining optimization statistics
	// set of activated xforms
	CXform_set *m_xforms;
	// number of calls to each xform
	duckdb::vector<ULONG_PTR *> m_xform_call_num;
	// time consumed by each xform
	duckdb::vector<ULONG_PTR *> m_xform_running_time;
	// number of bindings for each xform
	duckdb::vector<ULONG_PTR *> m_xform_binding_num;
	// number of alternatives generated by each xform
	duckdb::vector<ULONG_PTR *> m_xform_results_num;

public:
	// initialize query logical expression
	void InitLogicalExpression(duckdb::unique_ptr<Operator> pexpr);
	// insert children of the given expression to memo, and copy the resulting groups to the given group array
	void InsertExpressionChildren(Operator *expr, duckdb::vector<CGroup *> &group_children,
	                              CXform::EXformId xform_id_origin, CGroupExpression *expr_origin);
	// create and schedule the main optimization job
	void ScheduleMainJob(CSchedulerContext *scheduler_context, COptimizationContext *optimization_context);
	// check if search has terminated
	bool FSearchTerminated() const {
		// at least one stage has completed and achieved required cost
		return (nullptr != PreviousSearchStage() && PreviousSearchStage()->FAchievedReqdCost());
	}
	// generate random plan id
	ULLONG RandomPlanId(ULONG *seed);
	// extract a plan sample and handle exceptions according to enumerator configurations
	bool FValidPlanSample(CEnumeratorConfig *enumerator_config, ULLONG plan_id, Operator **ppexpr);
	// check if all children were successfully optimized
	bool FChildrenOptimized(duckdb::vector<COptimizationContext *> optimization_contexts);
	// check if any of the given property enforcing types prohibits enforcement
	static bool FProhibited(duckdb::COrderProperty::EPropEnforcingType enforcing_type_order);
	// check whether the given memo groups can be marked as duplicates. This is
	// true only if they have the same logical properties
	static bool FPossibleDuplicateGroups(CGroup *group_left, CGroup *group_right);
	// check if optimization is possible under the given property enforcing types
	static bool FOptimize(duckdb::COrderProperty::EPropEnforcingType enforcing_type);
	// Unrank the plan with the given 'plan_id' from the memo
	Operator *PexprUnrank(ULLONG plan_id);

public:
	// initialize engine with a query context and search strategy
	void Init(CQueryContext *query_context, duckdb::vector<CSearchStage *> search_strategy);
	// accessor of memo's root group
	CGroup *GroupRoot() const {
		return m_memo_table->GroupRoot();
	}
	// check if a group is the root one
	bool FRoot(CGroup *pgroup) const {
		return (GroupRoot() == pgroup);
	}
	// insert expression tree to memo
	CGroup *GroupInsert(CGroup *group_target, duckdb::unique_ptr<Operator> expr, CXform::EXformId xform_id_origin,
	                     CGroupExpression *group_expr_origin, bool f_intermediate);
	// insert a set of xform results into the memo
	void InsertXformResult(CGroup *pgroup_origin, CXformResult *pxfres, CXform::EXformId exfid_origin,
	                       CGroupExpression *group_expr_origin, ULONG xform_time, ULONG number_of_bindings);
	// add enforcers to the memo
	void AddEnforcers(CGroupExpression *group_expr, duckdb::vector<duckdb::unique_ptr<Operator>> group_expr_enforcers);
	// extract a physical plan from the memo
	Operator *ExprExtractPlan();
	// check required properties;
	// return false if it's impossible for the operator to satisfy one or more
	bool FCheckRequiredProps(CExpressionHandle &exprhdl, CRequiredPhysicalProp *prpp, ULONG ul_opt_req);
	// check enforceable properties;
	// return false if it's impossible for the operator to satisfy one or more
	bool FCheckEnforceableProps(CGroupExpression *expr, COptimizationContext *poc, ULONG num_opt_request,
	                            duckdb::vector<COptimizationContext *> pdrgpoc);
	// check if the given expression has valid cte and partition properties
	// with respect to the given requirements
	bool FValidCTEAndPartitionProperties(CExpressionHandle &exprhdl, CRequiredPhysicalProp *prpp);
	// derive statistics
	void DeriveStats();
	// execute operations after exploration completes
	void FinalizeExploration();
	// execute operations after implementation completes
	void FinalizeImplementation();
	// execute operations after search stage completes
	void FinalizeSearchStage();
	// main driver of optimization engine
	void Optimize();
	// merge duplicate groups
	void GroupMerge() {
		m_memo_table->GroupMerge();
	}
	// return current search stage
	CSearchStage *CurrentSearchStage() const {
		return m_search_strategy[m_current_search_stage];
	}
	// current search stage index accessor
	ULONG CurrentSearchStageIdx() const {
		return m_current_search_stage;
	}
	// return previous search stage
	CSearchStage *PreviousSearchStage() const {
		if (0 == m_current_search_stage) {
			return nullptr;
		}
		return m_search_strategy[m_current_search_stage - 1];
	}
	// number of search stages accessor
	ULONG PreviousSearchStageIdx() const {
		return m_search_strategy.size();
	}
	// set of xforms of current stage
	CXform_set *CurrentStageXforms() const {
		return m_search_strategy[m_current_search_stage]->m_xforms;
	}
	// return array of child optimization contexts corresponding to handle requirements
	duckdb::vector<COptimizationContext *> ChildrenOptimizationContext(CExpressionHandle &exprhdl);
	// build tree map on memo
	MemoTreeMap *MemoToMap();
	// reset tree map
	void ResetTreeMap() {
		m_memo_table->ResetTreeMap();
	}
	// check if parent group expression can optimize child group expression
	bool FOptimizeChild(CGroupExpression *pgexpr_parent, CGroupExpression *pgexpr_child,
	                    COptimizationContext *poc_child, EOptimizationLevel eol);
	// determine if a plan, rooted by given group expression, can be safely pruned based on cost bounds
	bool FSafeToPrune(CGroupExpression *pgexpr, CRequiredPhysicalProp *prpp, CCostContext *pcc_child, ULONG child_index,
	                  double *pcost_lower_bound);
	// damp optimization level to process group expressions in the next lower optimization level
	static EOptimizationLevel DampOptimizationLevel(EOptimizationLevel eol);
	// derive statistics
	static void DeriveStats(CGroup *pgroup, CRequiredLogicalProp *prprel);
	// return the first group expression in a given group
	static CGroupExpression *PgexprFirst(CGroup *pgroup);
	duckdb::vector<ULONG_PTR *> GetNumberOfBindings();
}; // class CEngine
} // namespace gpopt