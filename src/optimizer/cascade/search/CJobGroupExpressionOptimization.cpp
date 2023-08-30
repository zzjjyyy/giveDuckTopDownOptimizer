//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionOptimization.cpp
//
//	@doc:
//		Implementation of group expression optimization job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionOptimization.h"

#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropPlan.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropRelational.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/planner/logical_operator.hpp"

using namespace gpopt;

// State transition diagram for group expression optimization job state machine;
//
//                 +------------------------+
//                 |    estInitialized:     |
//  +------------- |    EevtInitialize()    |
//  |              +------------------------+
//  |                |
//  |                | eevOptimizingChildren
//  |                v
//  |              +------------------------+   eevOptimizingChildren
//  |              | estOptimizingChildren: | ------------------------+
//  |              | EevtOptimizeChildren() |                         |
//  +------------- |                        | <-----------------------+
//  |              +------------------------+
//  |                |
//  | eevFinalized   | eevChildrenOptimized
//  |                v
//  |              +------------------------+
//  |              | estChildrenOptimized:  |
//  +------------- |   EevtAddEnforcers()   |
//  |              +------------------------+
//  |                |
//  |                | eevOptimizingSelf
//  |                v
//  |              +------------------------+   eevOptimizingSelf
//  |              |  estEnfdPropsChecked:  | ------------------------+
//  |              |   EevtOptimizeSelf()   |                         |
//  +------------- |                        | <-----------------------+
//  |              +------------------------+
//  |                |
//  |                | eevSelfOptimized
//  |                v
//  |              +------------------------+
//  |              |   estSelfOptimized:    |
//  | eevFinalized |     EevtFinalize()     |
//  |              +------------------------+
//  |                |
//  |                |
//  |                |
//  |                |
//  +----------------+
//                   |
//                   |
//                   | eevFinalized
//                   v
//                 +------------------------+
//                 |      estCompleted      |
//                 +------------------------+
//
const CJobGroupExpressionOptimization::EEvent
    rgeev[CJobGroupExpressionOptimization::estSentinel][CJobGroupExpressionOptimization::estSentinel] = {
        {// estInitialized
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevOptimizingChildren,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevFinalized},
        {// estOptimizingChildren
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevOptimizingChildren,
         CJobGroupExpressionOptimization::eevChildrenOptimized, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevFinalized},
        {// estChildrenOptimized
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevOptimizingSelf,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevFinalized},
        {// estEnfdPropsChecked
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevOptimizingSelf,
         CJobGroupExpressionOptimization::eevSelfOptimized, CJobGroupExpressionOptimization::eevFinalized},
        {// estSelfOptimized
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevFinalized},
        {// estCompleted
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel,
         CJobGroupExpressionOptimization::eevSentinel, CJobGroupExpressionOptimization::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::CJobGroupExpressionOptimization
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::CJobGroupExpressionOptimization() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::~CJobGroupExpressionOptimization
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::~CJobGroupExpressionOptimization() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::Init(CGroupExpression *pgexpr, COptimizationContext *poc, ULONG opt_request_num) {
	CJobGroupExpression::Init(pgexpr);
	m_job_state_machine.Init(rgeev);
	// set job actions
	m_job_state_machine.SetAction(estInitialized, EevtInitialize);
	m_job_state_machine.SetAction(estOptimizingChildren, EevtOptimizeChildren);
	m_job_state_machine.SetAction(estChildrenOptimized, EevtAddEnforcers);
	m_job_state_machine.SetAction(estEnfdPropsChecked, EevtOptimizeSelf);
	m_job_state_machine.SetAction(estSelfOptimized, EevtFinalize);
	m_plan_properties_handler = nullptr;
	m_relation_properties_handler = nullptr;
	m_arity = pgexpr->Arity();
	m_children_index = gpos::ulong_max;
	m_opt_context = poc;
	m_opt_request_num = opt_request_num;
	m_child_optimization_failed = false;
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::Cleanup
//
//	@doc:
//		Cleanup allocated memory
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::Cleanup() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::InitChildGroupsOptimization
//
//	@doc:
//		Initialization routine for child groups optimization
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::InitChildGroupsOptimization(CSchedulerContext *psc) {
	// initialize required plan properties computation
	m_plan_properties_handler = new CExpressionHandle();
	m_plan_properties_handler->Attach(m_group_expression);
	if (0 < m_arity) {
		m_children_index = m_plan_properties_handler->UlFirstOptimizedChildIndex();
	}
	m_plan_properties_handler->DeriveProps(nullptr);
	m_plan_properties_handler->InitReqdProps(m_opt_context->m_required_plan_properties);
	// initialize required relational properties computation
	m_relation_properties_handler = new CExpressionHandle();
	CGroupExpression *pgexprForStats = m_group_expression->m_group->BestPromiseGroupExpr(m_group_expression);
	if (nullptr != pgexprForStats) {
		m_relation_properties_handler->Attach(pgexprForStats);
		m_relation_properties_handler->DeriveProps(nullptr);
		m_relation_properties_handler->ComputeReqdProps(
		    (CRequiredProperty *)m_opt_context->m_required_relational_properties, 0);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtInitialize
//
//	@doc:
//		Initialize internal data structures;
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent
CJobGroupExpressionOptimization::EevtInitialize(CSchedulerContext *scheduler_context, CJob *job_owner) {
	// get a job pointer
	CJobGroupExpressionOptimization *job = PjConvert(job_owner);
	CExpressionHandle handle;
	handle.Attach(job->m_group_expression);
	handle.DeriveProps(nullptr);
	if (!scheduler_context->m_engine->FCheckRequiredProps(handle, job->m_opt_context->m_required_plan_properties,
	                                                      job->m_opt_request_num)) {
		return eevFinalized;
	}
	// check if job can be early terminated without optimizing any child
	double cost_lower_bound = GPOPT_INVALID_COST;
	if (scheduler_context->m_engine->FSafeToPrune(job->m_group_expression,
	                                              job->m_opt_context->m_required_plan_properties, nullptr,
	                                              gpos::ulong_max, &cost_lower_bound)) {
		duckdb::vector<COptimizationContext *> v;
		(void)job->m_group_expression->PccComputeCost(job->m_opt_context, job->m_opt_request_num, v, true,
		                                              cost_lower_bound);
		return eevFinalized;
	}
	job->InitChildGroupsOptimization(scheduler_context);
	return eevOptimizingChildren;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::DerivePrevChildProps
//
//	@doc:
//		Derive plan properties and stats of the child previous to
//		the one being optimized
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::DerivePrevChildProps(CSchedulerContext *scheduler_context) {
	ULONG prev_child_index = m_plan_properties_handler->UlPreviousOptimizedChildIndex(m_children_index);
	// retrieve plan properties of the optimal implementation of previous child group
	CGroup *child = (*m_group_expression)[prev_child_index];
	if (child->m_is_scalar) {
		// exit if previous child is a scalar group
		return;
	}
	COptimizationContext *poc_child = child->PocLookupBest(scheduler_context->m_engine->PreviousSearchStageIdx(),
	                                                       m_plan_properties_handler->Prpp(prev_child_index));
	CCostContext *pcc_child_best = poc_child->m_best_cost_context;
	if (nullptr == pcc_child_best) {
		// failed to optimize child
		m_child_optimization_failed = true;
		return;
	}
	// check if job can be early terminated after previous children have been optimized
	double cost_lower_bound = GPOPT_INVALID_COST;
	if (scheduler_context->m_engine->FSafeToPrune(m_group_expression, m_opt_context->m_required_plan_properties,
	                                              pcc_child_best, prev_child_index, &cost_lower_bound)) {
		duckdb::vector<COptimizationContext *> v;
		// failed to optimize child due to cost bounding
		(void)m_group_expression->PccComputeCost(m_opt_context, m_opt_request_num, v, true, cost_lower_bound);
		m_child_optimization_failed = true;
		return;
	}
	CExpressionHandle handle;
	handle.Attach(pcc_child_best);
	handle.DerivePlanPropsForCostContext();
	m_children_derived_properties.emplace_back(handle.DerivedProperty());
	/* I comment here */
	// copy stats of child's best cost context to current stats context
	// IStatistics *pstat = pccChildBest->Pstats();
	// pstat->AddRef();
	// m_pdrgpstatCurrentCtxt->Append(pstat);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::ComputeCurrentChildRequirements
//
//	@doc:
//		Compute required plan properties for current child
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::ComputeCurrentChildRequirements(CSchedulerContext *psc) {
	// derive plan properties of previous child group
	if (m_children_index != m_plan_properties_handler->UlFirstOptimizedChildIndex()) {
		DerivePrevChildProps(psc);
		if (m_child_optimization_failed) {
			return;
		}
	}
	// compute required plan properties of current child group
	m_plan_properties_handler->ComputeChildReqdProps(m_children_index, m_children_derived_properties,
	                                                 m_opt_request_num);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::ScheduleChildGroupsJobs
//
//	@doc:
//		Schedule optimization job for the next child group; skip child groups
//		as they do not require optimization
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::ScheduleChildGroupsJobs(CSchedulerContext *psc) {
	CGroup *pgroupChild = (*m_group_expression)[m_children_index];
	if (pgroupChild->m_is_scalar) {
		if (!m_plan_properties_handler->FNextChildIndex(&m_children_index)) {
			// child group optimization is complete
			SetChildrenScheduled();
		}
		return;
	}
	ComputeCurrentChildRequirements(psc);
	if (m_child_optimization_failed) {
		return;
	}
	// compute required relational properties
	CRequiredPropRelational *prprel =
	    new CRequiredPropRelational(); // m_relation_properties_handler->GetReqdRelationalProps(m_children_index);
	// schedule optimization job for current child group
	COptimizationContext *pocChild = new COptimizationContext(
	    pgroupChild, m_plan_properties_handler->Prpp(m_children_index), prprel, psc->m_engine->CurrentSearchStageIdx());
	if (pgroupChild == m_group_expression->m_group && pocChild->Matches(m_opt_context)) {
		// this is to prevent deadlocks, child context cannot be the same as parent context
		m_child_optimization_failed = true;
		return;
	}
	CJobGroupOptimization::ScheduleJob(psc, pgroupChild, m_group_expression, pocChild, this);
	// advance to next child
	if (!m_plan_properties_handler->FNextChildIndex(&m_children_index)) {
		// child group optimization is complete
		SetChildrenScheduled();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtOptimizeChildren
//
//	@doc:
//		Optimize child groups
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtOptimizeChildren(CSchedulerContext *psc,
                                                                                              CJob *pjOwner) {
	// get a job pointer
	CJobGroupExpressionOptimization *pjgeo = PjConvert(pjOwner);
	if (0 < pjgeo->m_arity && !pjgeo->FChildrenScheduled()) {
		pjgeo->ScheduleChildGroupsJobs(psc);
		if (pjgeo->m_child_optimization_failed) {
			// failed to optimize child, terminate job
			pjgeo->Cleanup();
			return eevFinalized;
		}
		return eevOptimizingChildren;
	}
	return eevChildrenOptimized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtAddEnforcers
//
//	@doc:
//		Add required enforcers to owning group
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtAddEnforcers(CSchedulerContext *psc,
                                                                                          CJob *pjOwner) {
	// get a job pointer
	CJobGroupExpressionOptimization *pjgeo = PjConvert(pjOwner);
	// build child contexts array
	pjgeo->m_children_opt_contexts = psc->m_engine->ChildrenOptimizationContext(*pjgeo->m_plan_properties_handler);
	// enforce physical properties
	BOOL fCheckEnfdProps = psc->m_engine->FCheckEnforceableProps(
	    pjgeo->m_group_expression, pjgeo->m_opt_context, pjgeo->m_opt_request_num, pjgeo->m_children_opt_contexts);
	if (fCheckEnfdProps) {
		// No new enforcers group expressions were added because they were either
		// optional or unnecessary. So, move on to optimize the current group
		// expression.
		return eevOptimizingSelf;
	}
	// Either adding enforcers was prohibited or at least one enforcer was added
	// because it was required. In any case, this job can be finalized, since
	// optimizing the current group expression is not needed (because of the
	// prohibition) or the newly created enforcer group expression job will get
	// to it later on.
	pjgeo->Cleanup();
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtOptimizeSelf
//
//	@doc:
//		Optimize group expression
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtOptimizeSelf(CSchedulerContext *psc,
                                                                                          CJob *job_owner) {
	// get a job pointer
	CJobGroupExpressionOptimization *job = PjConvert(job_owner);
	// compute group expression cost under current context
	COptimizationContext *poc = job->m_opt_context;
	CGroupExpression *expr = job->m_group_expression;
	duckdb::vector<COptimizationContext *> opt_context = job->m_children_opt_contexts;
	ULONG request_num = job->m_opt_request_num;
	CCostContext *pcc = expr->PccComputeCost(poc, request_num, opt_context, false, 0.0);
	if (nullptr == pcc) {
		job->Cleanup();
		// failed to create cost context, terminate optimization job
		return eevFinalized;
	}
	expr->m_group->UpdateBestCost(poc, pcc);
	return eevSelfOptimized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::EevtFinalize
//
//	@doc:
//		Finalize optimization
//
//---------------------------------------------------------------------------
CJobGroupExpressionOptimization::EEvent CJobGroupExpressionOptimization::EevtFinalize(CSchedulerContext *psc,
                                                                                      CJob *pjOwner) {
	// get a job pointer
	CJobGroupExpressionOptimization *pjgeo = PjConvert(pjOwner);
	pjgeo->Cleanup();
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
BOOL CJobGroupExpressionOptimization::FExecute(CSchedulerContext *psc) {
	return m_job_state_machine.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionOptimization::ScheduleJob
//
//	@doc:
//		Schedule a new group expression optimization job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionOptimization::ScheduleJob(CSchedulerContext *psc, CGroupExpression *pgexpr,
                                                  COptimizationContext *poc, ULONG ulOptReq, CJob *job_parent) {
	CJob *pj = psc->m_job_factory->CreateJob(CJob::EjtGroupExpressionOptimization);
	// initialize job
	CJobGroupExpressionOptimization *pjgeo = PjConvert(pj);
	pjgeo->Init(pgexpr, poc, ulOptReq);
	psc->m_scheduler->Add(pjgeo, job_parent);
}