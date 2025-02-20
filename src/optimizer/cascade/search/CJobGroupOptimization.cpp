//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupOptimization.cpp
//
//	@doc:
//		Implementation of group optimization job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupOptimization.h"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionOptimization.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"

namespace gpopt {
using namespace duckdb;

// State transition diagram for group optimization job state machine:
//
//     eevImplementing   +------------------------------+
//  +------------------ |       estInitialized:        |
//  |                   |   EevtStartOptimization()    |
//  +-----------------> |                              | -+
//                      +------------------------------+  |
//                        |                               |
//                        | eevImplemented                |
//                        v                               |
//                      +------------------------------+  |
//      eevOptimizing   |                              |  |
//  +------------------ |                              |  |
//  |                   |    estOptimizingChildren:    |  |
//  +-----------------> |    EevtOptimizeChildren()    |  |
//                      |                              |  |
//  +-----------------> |                              |  |
//  |                   +------------------------------+  |
//  |                     |                               |
//  | eevOptimizing       | eevOptimizedCurrentLevel      |
//  |                     v                               |
//  |                   +------------------------------+  |
//  |                   | estDampingOptimizationLevel: |  |
//  +------------------ |  EevtCompleteOptimization()  |  |
//                      +------------------------------+  |
//                        |                               |
//                        | eevOptimized                  | eevOptimized
//                        v                               |
//                      +------------------------------+  |
//                      |         estCompleted         | <+
//                      +------------------------------+
//
const CJobGroupOptimization::EEvent rgeev1[CJobGroupOptimization::estSentinel][CJobGroupOptimization::estSentinel] = {
    {// estInitialized
     CJobGroupOptimization::eevImplementing, CJobGroupOptimization::eevImplemented, CJobGroupOptimization::eevSentinel,
     CJobGroupOptimization::eevOptimized},
    {// estOptimizingChildren
     CJobGroupOptimization::eevSentinel, CJobGroupOptimization::eevOptimizing,
     CJobGroupOptimization::eevOptimizedCurrentLevel, CJobGroupOptimization::eevSentinel},
    {// estDampingOptimizationLevel
     CJobGroupOptimization::eevSentinel, CJobGroupOptimization::eevOptimizing, CJobGroupOptimization::eevSentinel,
     CJobGroupOptimization::eevOptimized},
    {// estCompleted
     CJobGroupOptimization::eevSentinel, CJobGroupOptimization::eevSentinel, CJobGroupOptimization::eevSentinel,
     CJobGroupOptimization::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::CJobGroupOptimization
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupOptimization::CJobGroupOptimization() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::~CJobGroupOptimization
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupOptimization::~CJobGroupOptimization() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupOptimization::Init(CGroup *pgroup, CGroupExpression *pgexprOrigin, COptimizationContext *poc) {
	CJobGroup::Init(pgroup);
	m_jsm.Init(rgeev1);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtStartOptimization);
	m_jsm.SetAction(estOptimizingChildren, EevtOptimizeChildren);
	m_jsm.SetAction(estDampingOptimizationLevel, EevtCompleteOptimization);
	m_pgexprOrigin = pgexprOrigin;
	m_poc = m_pgroup->PocInsert(poc);
	SetJobQueue(m_poc->PjqOptimization());
	// initialize current optimization level as low
	m_eolCurrent = EolLow;
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::FScheduleGroupExpressions
//
//	@doc:
//		Schedule optimization jobs for all unoptimized group expressions with
//		the current optimization priority;
//		the function returns true if it could schedule any new jobs
//
//---------------------------------------------------------------------------
bool CJobGroupOptimization::FScheduleGroupExpressions(CSchedulerContext *psc) {
	auto Last_itr = m_last_scheduled_expr;
	// iterate on expressions and schedule them as needed
	auto itr = PgexprFirstUnsched();
	while (m_pgroup->m_group_exprs.end() != itr) {
		CGroupExpression *pgexpr = *itr;
		// we consider only group expressions matching current optimization level,
		// other group expressions will be optimized when damping current
		// optimization level
		if (psc->m_engine->FOptimizeChild(m_pgexprOrigin, pgexpr, m_poc, EolCurrent())) {
			const ULONG ulOptRequests = ((PhysicalOperator *)pgexpr->m_operator.get())->UlOptRequests();
			for (ULONG ul = 0; ul < ulOptRequests; ul++) {
				// schedule an optimization job for each request
				CJobGroupExpressionOptimization::ScheduleJob(psc, pgexpr, m_poc, ul, this);
			}
		}
		Last_itr = itr;
		// move to next expression
		{
			CGroupProxy gp(m_pgroup);
			++itr;
		}
	}
	bool fNewJobs = (m_last_scheduled_expr != Last_itr);
	// set last scheduled expression
	m_last_scheduled_expr = Last_itr;
	return fNewJobs;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::EevtStartOptimization
//
//	@doc:
//		Start group optimization
//
//---------------------------------------------------------------------------
CJobGroupOptimization::EEvent CJobGroupOptimization::EevtStartOptimization(CSchedulerContext *psc, CJob *pjOwner) {
	// get a job pointer
	CJobGroupOptimization *pjgo = ConvertJob(pjOwner);
	CGroup *pgroup = pjgo->m_pgroup;
	if (!pgroup->FImplemented()) {
		// CJobGroup::PrintJob(ConvertJob(pjOwner), "[StartOptimization]");
		// schedule a group implementation child job
		CJobGroupImplementation::ScheduleJob(psc, pgroup, pjgo);
		return eevImplementing;
	}
	// move optimization context to optimizing state
	pjgo->m_poc->SetState(COptimizationContext::estOptimizing);
	// if this is the root, release implementation jobs
	if (psc->m_engine->FRoot(pgroup)) {
		psc->m_job_factory->Truncate(EjtGroupImplementation);
		psc->m_job_factory->Truncate(EjtGroupExpressionImplementation);
	}
	// at this point all group expressions have been added to group,
	// we set current job optimization level as the max group optimization level
	pjgo->m_eolCurrent = pgroup->m_max_opt_level;
	return eevImplemented;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::EevtOptimizeChildren
//
//	@doc:
//		Optimize child group expressions
//
//---------------------------------------------------------------------------
CJobGroupOptimization::EEvent CJobGroupOptimization::EevtOptimizeChildren(CSchedulerContext *psc, CJob *pjOwner) {
	// get a job pointer
	CJobGroupOptimization *pjgo = ConvertJob(pjOwner);
	if (pjgo->FScheduleGroupExpressions(psc)) {
		// CJobGroup::PrintJob(ConvertJob(pjOwner), "[OptimizeChildren]");
		// optimization is in progress
		return eevOptimizing;
	}
	// optimization of current level is complete
	return eevOptimizedCurrentLevel;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::EevtCompleteOptimization
//
//	@doc:
//		Complete optimization action
//
//---------------------------------------------------------------------------
CJobGroupOptimization::EEvent CJobGroupOptimization::EevtCompleteOptimization(CSchedulerContext *psc, CJob *pjOwner) {
	// CJobGroup::PrintJob(ConvertJob(pjOwner), "[CompleteOptimization]");

	// get a job pointer
	CJobGroupOptimization *pjgo = ConvertJob(pjOwner);
	// move to next optimization level
	pjgo->DampOptimizationLevel();
	if (EolSentinel != pjgo->EolCurrent()) {
		// we need to optimize group expressions matching current level
		pjgo->m_last_scheduled_expr = pjgo->m_pgroup->m_group_exprs.end();
		return eevOptimizing;
	}
	// move optimization context to optimized state
	pjgo->m_poc->SetState(COptimizationContext::estOptimized);
	return eevOptimized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool CJobGroupOptimization::FExecute(CSchedulerContext *psc) {
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupOptimization::ScheduleJob
//
//	@doc:
//		Schedule a new group optimization job
//
//---------------------------------------------------------------------------
void CJobGroupOptimization::ScheduleJob(CSchedulerContext *scheduler_context, CGroup *group,
                                        CGroupExpression *group_expr_origin, COptimizationContext *opt_context,
                                        CJob *parent_job) {
	CJob *job = scheduler_context->m_job_factory->CreateJob(CJob::EjtGroupOptimization);
	// initialize job
	CJobGroupOptimization *job_group_opt = ConvertJob(job);
	job_group_opt->Init(group, group_expr_origin, opt_context);
	scheduler_context->m_scheduler->Add(job_group_opt, parent_job);
}
} // namespace gpopt