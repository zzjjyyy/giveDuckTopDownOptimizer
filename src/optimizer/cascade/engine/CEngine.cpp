//---------------------------------------------------------------------------
//	@filename:
//		CEngine.cpp
//
//	@doc:
//		Implementation of optimization engine
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/engine/CEngine.h"

#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/base/CRequiredPhysicalProp.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropRelational.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/common/syslibwrapper.h"
#include "duckdb/optimizer/cascade/engine/CEnumeratorConfig.h"
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

#define GPOPT_SAMPLING_MAX_ITERS 30
#define GPOPT_JOBS_CAP           5000 // maximum number of initial optimization jobs
#define GPOPT_JOBS_PER_GROUP     20   // estimated number of needed optimization jobs per memo group

// memory consumption unit in bytes -- currently MB
#define GPOPT_MEM_UNIT      (1024 * 1024)
#define GPOPT_MEM_UNIT_NAME "MB"

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEngine::CEngine() : m_query_context(nullptr), m_current_search_stage(0), m_memo_table(nullptr), m_xforms(nullptr) {
	m_memo_table = new CMemo();
	m_expr_enforcer_pattern = make_uniq<CPatternLeaf>();
	m_xforms = new CXform_set();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::~CEngine
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEngine::~CEngine() {
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InitLogicalExpression
//
//	@doc:
//		Initialize engine with a given expression
//
//---------------------------------------------------------------------------
void CEngine::InitLogicalExpression(duckdb::unique_ptr<Operator> expr) {
	CGroup *group_root = GroupInsert(nullptr, std::move(expr), CXform::ExfInvalid, nullptr, false);
	m_memo_table->SetRoot(group_root);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::Init
//
//	@doc:
//		Initialize engine using a given query context
//
//---------------------------------------------------------------------------
void CEngine::Init(CQueryContext *query_context, duckdb::vector<CSearchStage *> search_strategy) {
	m_search_strategy = search_strategy;
	if (search_strategy.empty()) {
		m_search_strategy = CSearchStage::DefaultStrategy();
	}
	m_query_context = query_context;
	InitLogicalExpression(std::move(m_query_context->m_expr));
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::AddEnforcers
//
//	@doc:
//		Add enforcers to a memo group
//
//---------------------------------------------------------------------------
void CEngine::AddEnforcers(CGroupExpression *group_expr,
                           duckdb::vector<duckdb::unique_ptr<Operator>> group_expr_enforcers) {
	for (ULONG ul = 0; ul < group_expr_enforcers.size(); ul++) {
		// assemble an expression rooted by the enforcer operator
		CGroup *group =
		    GroupInsert(group_expr->m_group, std::move(group_expr_enforcers[ul]), CXform::ExfInvalid, nullptr, false);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertExpressionChildren
//
//	@doc:
//		Insert children of the given expression to memo, and  the groups
//		they end up at to the given group array
//
//---------------------------------------------------------------------------
void CEngine::InsertExpressionChildren(Operator *expr, duckdb::vector<CGroup *> &group_children,
                                       CXform::EXformId xform_id_origin, CGroupExpression *expr_origin) {
	ULONG arity = expr->Arity();
	for (ULONG i = 0; i < arity; i++) {
		// insert child expression recursively
		CGroup *group_child = GroupInsert(nullptr, expr->children[i]->Copy(), xform_id_origin, expr_origin, true);
		group_children.emplace_back(group_child);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::GroupInsert
//
//	@doc:
//		Insert an expression tree into the memo, with explicit target group;
//		the function returns a pointer to the group that contains the given group expression
//
//---------------------------------------------------------------------------
CGroup *CEngine::GroupInsert(CGroup *group_target, duckdb::unique_ptr<Operator> expr, CXform::EXformId xform_id_origin,
                             CGroupExpression *group_expr_origin, bool f_intermediate) {
	CGroup *group_origin;
	// check if expression was produced by extracting
	// a binding from the memo
	if (nullptr != expr->m_group_expression) {
		group_origin = expr->m_group_expression->m_group;
		// if parent has group pointer, all children must have group pointers;
		// terminate recursive insertion here
		return group_origin;
	}

	// insert expression's children to memo by recursive call
	duckdb::vector<CGroup *> group_children;
	InsertExpressionChildren(expr.get(), group_children, xform_id_origin, group_expr_origin);
	CGroupExpression *group_expr =
	    new CGroupExpression(std::move(expr), group_children, xform_id_origin, group_expr_origin, f_intermediate);

	// find the group that contains created group expression
	CGroup *group_container = m_memo_table->GroupInsert(group_target, group_expr);
	if (nullptr == group_expr->m_group) {
		// insertion failed, release created group expression
		delete group_expr;
		group_expr = nullptr;
	}
	return group_container;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertXformResult
//
//	@doc:
//		Insert a set of transformation results to memo
//
//---------------------------------------------------------------------------
void CEngine::InsertXformResult(CGroup *pgroup_origin, CXformResult *pxfres, CXform::EXformId exfid_origin,
                                CGroupExpression *group_expr_origin, ULONG xform_time, ULONG number_of_bindings) {
	duckdb::unique_ptr<Operator> pexpr = pxfres->NextExpression();
	while (nullptr != pexpr) {
		CGroup *pgroup_container = GroupInsert(pgroup_origin, std::move(pexpr), exfid_origin, group_expr_origin, false);
		if (pgroup_container != pgroup_origin && FPossibleDuplicateGroups(pgroup_container, pgroup_origin)) {
			m_memo_table->MarkDuplicates(pgroup_origin, pgroup_container);
		}
		pexpr = pxfres->NextExpression();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FPossibleDuplicateGroups
//
//	@doc:
//		Check whether the given memo groups can be marked as duplicates.
// This is 		true only if they have the same logical properties
//
//---------------------------------------------------------------------------
bool CEngine::FPossibleDuplicateGroups(CGroup *group_left, CGroup *group_right) {
	CDerivedLogicalProp *pdprel_fst = CDerivedLogicalProp::GetRelationalProperties(group_left->m_derived_properties);
	CDerivedLogicalProp *pdprel_snd = CDerivedLogicalProp::GetRelationalProperties(group_right->m_derived_properties);
	// right now we only check the output columns, but we may possibly need to
	// check other properties as well
	duckdb::vector<ColumnBinding> v1 = pdprel_fst->GetOutputColumns();
	duckdb::vector<ColumnBinding> v2 = pdprel_snd->GetOutputColumns();
	return CUtils::Equals(v1, v2);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the root group
//
//---------------------------------------------------------------------------
void CEngine::DeriveStats() {
	// derive stats on root group
	CEngine::DeriveStats(GroupRoot(), nullptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the group
//
//---------------------------------------------------------------------------
void CEngine::DeriveStats(CGroup *pgroup, CRequiredLogicalProp *prprel) {
	CGroupExpression *pgexpr_first = CEngine::PgexprFirst(pgroup);
	CRequiredLogicalProp *prprel_new = prprel;
	if (nullptr == prprel_new) {
		// create empty property container
		duckdb::vector<ColumnBinding> pcrs;
		prprel_new = new CRequiredLogicalProp(pcrs);
	}
	// (void) pgexprFirst->Pgroup()->PstatsRecursiveDerive(pmpLocal, pmpGlobal,
	// prprelNew, pdrgpstatCtxtNew); pdrgpstatCtxtNew->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FirstGroupExpr
//
//	@doc:
//		Return the first group expression in a given group
//
//---------------------------------------------------------------------------
CGroupExpression *CEngine::PgexprFirst(CGroup *pgroup) {
	CGroupExpression *pgexpr_first;
	{
		// group proxy scope
		CGroupProxy gp(pgroup);
		pgexpr_first = *(gp.PgexprFirst());
	}
	return pgexpr_first;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DampOptimizationLevel
//
//	@doc:
//		Damp optimization level
//
//---------------------------------------------------------------------------
EOptimizationLevel CEngine::DampOptimizationLevel(EOptimizationLevel eol) {
	if (EolHigh == eol) {
		return EolLow;
	}
	return EolSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimizeChild
//
//	@doc:
//		Check if parent group expression needs to optimize child group
// expression. 		This method is called right before a group optimization
// job is about to 		schedule a group expression optimization job.
//
//		Relation properties as well the optimizing parent group
// expression
// is 		available to make the decision. So, operators can reject being
// optimized 		under specific parent operators. For example, a
// GatherMerge under
// a Sort 		can be prevented here since it destroys the order from a
// GatherMerge.
//---------------------------------------------------------------------------
bool CEngine::FOptimizeChild(CGroupExpression *pgexpr_parent, CGroupExpression *pgexpr_child,
                             COptimizationContext *poc_child, EOptimizationLevel eol_current) {
	if (pgexpr_parent == pgexpr_child) {
		// a group expression cannot optimize itself
		return false;
	}
	if (pgexpr_child->OptimizationLevel() != eol_current) {
		// child group expression does not match current optimization level
		return false;
	}
	return COptimizationContext::FOptimize(pgexpr_parent, pgexpr_child, poc_child, PreviousSearchStageIdx());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FSafeToPrune
//
//	@doc:
//		Determine if a plan rooted by given group expression can be
// safely 		pruned during optimization
//
//---------------------------------------------------------------------------
bool CEngine::FSafeToPrune(CGroupExpression *pgexpr, CRequiredPhysicalProp *prpp, CCostContext *pcc_child,
                           ULONG child_index, double *pcost_lower_bound) {
	*pcost_lower_bound = GPOPT_INVALID_COST;
	// check if container group has a plan for given properties
	CGroup *pgroup = pgexpr->m_group;
	COptimizationContext *poc_group = pgroup->PocLookupBest(m_current_search_stage, prpp);
	if (nullptr != poc_group && nullptr != poc_group->m_best_cost_context) {
		// compute a cost lower bound for the equivalent plan rooted by given group
		// expression
		double cost_lower_bound = pgexpr->CostLowerBound(prpp, pcc_child, child_index);
		*pcost_lower_bound = cost_lower_bound;
		if (cost_lower_bound > poc_group->m_best_cost_context->m_cost) {
			// group expression cannot deliver a better plan for given properties and
			// can be safely pruned
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::MemoToMap
//
//	@doc:
//		Build tree map on memo
//
//---------------------------------------------------------------------------
MemoTreeMap *CEngine::MemoToMap() {
	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	if (nullptr == m_memo_table->TreeMap()) {
		duckdb::vector<ColumnBinding> v;
		COptimizationContext *poc = new COptimizationContext(GroupRoot(), m_query_context->m_required_plan_property,
		                                                     new CRequiredLogicalProp(v), 0);
		m_memo_table->BuildTreeMap(poc);
	}
	return m_memo_table->TreeMap();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::ChildrenOptimizationContext
//
//	@doc:
//		Return array of child optimization contexts corresponding
//		to handle requirements
//
//---------------------------------------------------------------------------
duckdb::vector<COptimizationContext *> CEngine::ChildrenOptimizationContext(CExpressionHandle &exprhdl) {
	duckdb::vector<COptimizationContext *> pdrgpoc;
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		CGroup *pgroup_child = (*exprhdl.group_expr())[ul];
		if (!pgroup_child->m_is_scalar) {
			COptimizationContext *poc =
			    pgroup_child->PocLookupBest(m_search_strategy.size(), exprhdl.RequiredPropPlan(ul));
			pdrgpoc.emplace_back(poc);
		}
	}
	return pdrgpoc;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::ScheduleMainJob
//
//	@doc:
//		Create and schedule the main optimization job
//
//---------------------------------------------------------------------------
void CEngine::ScheduleMainJob(CSchedulerContext *scheduler_context, COptimizationContext *optimization_context) {
	CJobGroupOptimization::ScheduleJob(scheduler_context, GroupRoot(), nullptr, optimization_context, nullptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeExploration
//
//	@doc:
//		Execute operations after exploration completes
//
//---------------------------------------------------------------------------
void CEngine::FinalizeExploration() {
	GroupMerge();
	//  if (m_query_context->FDeriveStats())
	//  {
	//		// derive statistics
	//  	m_memo_table->ResetStats();
	//  	DeriveStats();
	//  }
	// if (!GPOS_FTRACE(EopttraceDonotDeriveStatsForAllGroups))
	// {
	//		// derive stats for every group without stats
	//  	m_memo_table->DeriveStatsIfAbsent();
	// }
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeImplementation
//
//	@doc:
//		Execute operations after implementation completes
//
//---------------------------------------------------------------------------
void CEngine::FinalizeImplementation() {
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeSearchStage
//
//	@doc:
//		Execute operations after search stage completes
//
//---------------------------------------------------------------------------
void CEngine::FinalizeSearchStage() {
	m_xforms = nullptr;
	m_xforms = new CXform_set();
	m_current_search_stage++;
	m_memo_table->ResetGroupStates();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::Optimize
//
//	@doc:
//		Main driver of optimization engine
//
//---------------------------------------------------------------------------
void CEngine::Optimize() {
	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	const ULONG num_jobs = std::min((ULONG)GPOPT_JOBS_CAP, (ULONG)(m_memo_table->NumGroups() * GPOPT_JOBS_PER_GROUP));
	CJobFactory job_factory(num_jobs);
	CScheduler scheduler(num_jobs);
	CSchedulerContext scheduler_context;
	scheduler_context.Init(&job_factory, &scheduler, this);

	const ULONG num_stages = m_search_strategy.size();
	for (ULONG ul = 0; !FSearchTerminated() && ul < num_stages; ul++) {
		CurrentSearchStage()->RestartTimer();
		// optimize root group
		duckdb::vector<ColumnBinding> v;
		COptimizationContext *poc = new COptimizationContext(GroupRoot(), m_query_context->m_required_plan_property,
		                                                     new CRequiredLogicalProp(v), m_current_search_stage);
		// schedule main optimization job
		ScheduleMainJob(&scheduler_context, poc);
		// run optimization job
		CScheduler::Run(&scheduler_context);
		// extract best plan found at the end of current search stage
		duckdb::unique_ptr<Operator> expr_plan = m_memo_table->ExtractPlan(m_memo_table->GroupRoot(), m_query_context->m_required_plan_property,
		                                           m_search_strategy.size());
		CurrentSearchStage()->SetBestExpr(expr_plan.release());
		FinalizeSearchStage();
	}
	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
Operator *CEngine::PexprUnrank(ULLONG plan_id) {
	// The CTE map will be updated by the Producer instead of the Sequence
	// operator because we are doing a DFS traversal of the TreeMap.
	CDrvdPropCtxtPlan *pdpctxtplan = new CDrvdPropCtxtPlan(false);
	Operator *pexpr = MemoToMap()->PrUnrank(pdpctxtplan, plan_id);
	delete pdpctxtplan;
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::ExprExtractPlan
//
//	@doc:
//		Extract a physical plan from the memo
//
//---------------------------------------------------------------------------
Operator *CEngine::ExprExtractPlan() {
	bool f_generate_alt = false;
	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	CEnumeratorConfig *pec = optimizer_config->m_enumerator_cfg;
	/* I comment here */
	// if (pec->FEnumerate())
	if (true) {
		/* I comment here */
		// if (0 < pec->GetPlanId())
		if (0 <= pec->GetPlanId()) {
			// a valid plan number was chosen
			f_generate_alt = true;
		}
	}
	Operator *pexpr = nullptr;
	if (f_generate_alt) {
		/* I comment here */
		// pexpr = PexprUnrank(pec->GetPlanId() - 1);
		pexpr = PexprUnrank(0);
	} else {
		pexpr = m_memo_table
		            ->ExtractPlan(m_memo_table->GroupRoot(), m_query_context->m_required_plan_property,
		                          m_search_strategy.size())
		            .get();
	}
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::RandomPlanId
//
//	@doc:
//		Generate random plan id
//
//---------------------------------------------------------------------------
ULLONG CEngine::RandomPlanId(ULONG *seed) {
	ULLONG ull_count = MemoToMap()->UllCount();
	ULLONG plan_id = 0;
	do {
		plan_id = clib::Rand(seed);
	} while (plan_id >= ull_count);
	return plan_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidPlanSample
//
//	@doc:
//		Extract a plan sample and handle exceptions according to
// enumerator 		configurations
//
//---------------------------------------------------------------------------
bool CEngine::FValidPlanSample(CEnumeratorConfig *enumerator_config, ULLONG plan_id, Operator **ppexpr) {
	bool f_valid_plan = true;
	if (enumerator_config->FSampleValidPlans()) {
		// if enumerator is configured to extract valid plans only,
		// we extract plan and catch invalid plan exception here
		*ppexpr = PexprUnrank(plan_id);
	} else {
		// otherwise, we extract plan and leave exception handling to the caller
		*ppexpr = PexprUnrank(plan_id);
	}
	return f_valid_plan;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckEnforceableProps
//
//	@doc:
//		Check enforceable properties and append enforcers to the current group if required.
//
//		This check is done in two steps:
//
//		First, it determines if any particular property needs to be enforced at all. For example, the
// EopttraceDisableSort traceflag can disable order enforcement. Also, if there are no partitioned tables referenced in
// the subtree, partition propagation enforcement can be skipped.
//
//		Second, EPET methods are called for each property to determine if an enforcer needs to be added. These methods
// in turn call into virtual methods in the different operators. For example, CPhysical::EnforcingTypeOrder() is used
// to determine a Sort node needs to be added to the group. These methods are passed an expression handle (to access
// derived properties of the subtree) and the required properties as a object of a subclass of CEnfdProp.
//
//		Finally, based on return values of the EPET methods, CEnfdProp::AppendEnforcers() is called for each of the
// enforced properties.
//
//		Returns true if no enforcers were created because they were deemed unnecessary or optional i.e all enforced
// properties were satisfied for the group expression under the current optimization context. Returns false otherwise.
//
//		NB: This method is only concerned with a certain enforcer needs to be added into the group. Once added, there is
// no connection between the enforcer and the operator that created it. That is although some group expression X created
// the enforcer E, later, during costing, E can still decide to pick some other group expression Y for its child, since
// theoretically, all group expressions in a group are equivalent.
//
//---------------------------------------------------------------------------
bool CEngine::FCheckEnforceableProps(CGroupExpression *expr, COptimizationContext *poc, ULONG num_opt_request,
                                     duckdb::vector<COptimizationContext *> pdrgpoc) {
	// check if all children could be successfully optimized
	if (!FChildrenOptimized(pdrgpoc)) {
		return false;
	}
	// load a handle with derived plan properties
	CCostContext *pcc = new CCostContext(poc, num_opt_request, expr);
	pcc->SetChildContexts(pdrgpoc);
	CExpressionHandle expr_handle;
	expr_handle.Attach(pcc);
	expr_handle.DerivePlanPropsForCostContext();
	PhysicalOperator *pop_physical = (PhysicalOperator *)(pcc->m_group_expression->m_operator.get());
	CRequiredPhysicalProp *prpp = poc->m_required_plan_properties;
	// Determine if any property enforcement is disabled or unnecessary
	bool f_order_reqd = !prpp->m_sort_order->m_order_spec->IsEmpty();
	// Determine if adding an enforcer to the group is required, optional, unnecessary or prohibited over the group
	// expression and given the current optimization context (required properties) get order enforcing type
	COrderProperty::EPropEnforcingType enforcing_type_order =
	    prpp->m_sort_order->EorderEnforcingType(expr_handle, pop_physical, f_order_reqd);
	// Skip adding enforcers entirely if any property determines it to be
	// 'prohibited'. In this way, a property may veto out the creation of an
	// enforcer for the current group expression and optimization context.
	//
	// NB: Even though an enforcer E is not added because of some group
	// expression G because it was prohibited, some other group expression H may
	// decide to add it. And if E is added, it is possible for E to consider both
	// G and H as its child.
	if (FProhibited(enforcing_type_order)) {
		return false;
	}
	duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr_enforcers;
	// extract a leaf pattern from target group
	CBinding binding;
	Operator *pexpr = binding.PexprExtract(expr_handle.group_expr(), m_expr_enforcer_pattern.get(), nullptr);
	prpp->m_sort_order->AppendEnforcers(prpp, pdrgpexpr_enforcers, pexpr->Copy(), enforcing_type_order, expr_handle);
	if (!pdrgpexpr_enforcers.empty()) {
		AddEnforcers(expr_handle.group_expr(), std::move(pdrgpexpr_enforcers));
	}
	return FOptimize(enforcing_type_order);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidCTEAndPartitionProperties
//
//	@doc:
//		Check if the given expression has valid cte with respect to the
// given requirements. 		This function returns true iff 		ALL the
// following conditions are met:
//		1. The expression satisfies the CTE requirements
//
//---------------------------------------------------------------------------
bool CEngine::FValidCTEAndPartitionProperties(CExpressionHandle &exprhdl, CRequiredPhysicalProp *prpp) {
	// PhysicalOperator* popPhysical = (PhysicalOperator*)exprhdl.Pop();
	return true;
	// return popPhysical->FProvidesReqdCTEs(prpp->Pcter());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FChildrenOptimized
//
//	@doc:
//		Check if all children were successfully optimized
//
//---------------------------------------------------------------------------
bool CEngine::FChildrenOptimized(duckdb::vector<COptimizationContext *> optimization_contexts) {
	const ULONG length = optimization_contexts.size();
	for (ULONG ul = 0; ul < length; ul++) {
		if (nullptr == optimization_contexts[ul]->BestExpression()) {
			COptimizationContext *opt = optimization_contexts[ul];
			LogicalOperatorType type = opt->m_group->m_group_exprs.front()->m_operator->logical_type;
			throw std::runtime_error("[CEngine::FChildrenOptimized] - child " + LogicalOperatorToString(type) +
			                         " not optimized.");
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimize
//
//	@doc:
//		Check if optimization is possible under the given property
// enforcing 		types
//
//---------------------------------------------------------------------------
bool CEngine::FOptimize(COrderProperty::EPropEnforcingType enforcing_type) {
	return COrderProperty::FOptimize(enforcing_type);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FProhibited
//
//	@doc:
//		Check if any of the given property enforcing types prohibits
// enforcement
//
//---------------------------------------------------------------------------
bool CEngine::FProhibited(COrderProperty::EPropEnforcingType enforcing_type_order) {
	return (COrderProperty::EpetProhibited == enforcing_type_order);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckRequiredProps
//
//	@doc:
//		Determine if checking required properties is needed. This method is called after a group expression optimization
// job has started executing and can be used to cancel the job early.
//
//		This is useful to prevent deadlocks when an enforcer optimizes same group with the same optimization context.
// Also, in case the subtree doesn't provide the required columns we can save optimization time by skipping this
// optimization request.
//
//		NB: Only relational properties are available at this stage to make this decision.
//---------------------------------------------------------------------------
bool CEngine::FCheckRequiredProps(CExpressionHandle &exprhdl, CRequiredPhysicalProp *prpp, ULONG ul_opt_req) {
	// check if operator provides required columns
	if (!prpp->FProvidesReqdCols(exprhdl, ul_opt_req)) {
		return false;
	}

	PhysicalOperator *pop_physical = (PhysicalOperator *)exprhdl.Pop();
	// check if sort operator is passed an empty order spec; this check is required to avoid self-deadlocks, i.e.  sort
	// optimizing same group with the same optimization context; A variable "is_enforced" is added to PhysicalOrder to
	// separate the enforced orderby and duckdb generated orderby.
	if (PhysicalOperatorType::ORDER_BY == pop_physical->physical_type) {
		PhysicalOrder *orderby = (PhysicalOrder *)pop_physical;
		bool f_order_reqd = !prpp->m_sort_order->m_order_spec->IsEmpty();
		if (!f_order_reqd && orderby->is_enforced) {
			return false;
		}
	}
	return true;
}

duckdb::vector<ULONG_PTR *> CEngine::GetNumberOfBindings() {
	return m_xform_binding_num;
}
} // namespace gpopt