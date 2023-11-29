//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupImplementation.cpp
//
//	@doc:
//		Implementation of group implementation job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"

#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"

using namespace gpopt;

clock_t start_implementation;

clock_t end_implementation;

// State transition diagram for group implementation job state machine;
//
// +---------------------------+   eevExploring
// |      estInitialized:      | ------------------+
// | EevtStartImplementation() |                   |
// |                           | <-----------------+
// +---------------------------+
//   |
//   | eevExplored
//   v
// +---------------------------+   eevImplementing
// | estImplementingChildren:  | ------------------+
// |  EevtImplementChildren()  |                   |
// |                           | <-----------------+
// +---------------------------+
//   |
//   | eevImplemented
//   v
// +---------------------------+
// |       estCompleted        |
// +---------------------------+
//
const CJobGroupImplementation::EEvent
    rgeev4[CJobGroupImplementation::estSentinel][CJobGroupImplementation::estSentinel] = {
        {CJobGroupImplementation::eevExploring, CJobGroupImplementation::eevExplored,
         CJobGroupImplementation::eevSentinel},
        {CJobGroupImplementation::eevSentinel, CJobGroupImplementation::eevImplementing,
         CJobGroupImplementation::eevImplemented},
        {CJobGroupImplementation::eevSentinel, CJobGroupImplementation::eevSentinel,
         CJobGroupImplementation::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::CJobGroupImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupImplementation::CJobGroupImplementation() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::~CJobGroupImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupImplementation::~CJobGroupImplementation() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupImplementation::Init(duckdb::unique_ptr<CGroup> pgroup) {
	CJobGroup::Init(pgroup);
	m_jsm.Init(rgeev4);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtStartImplementation);
	m_jsm.SetAction(estImplementingChildren, EevtImplementChildren);
	SetJobQueue(&pgroup->m_impl_job_queue);
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::FScheduleGroupExpressions
//
//	@doc:
//		Schedule implementation jobs for all unimplemented group expressions;
//		the function returns true if it could schedule any new jobs
//
//---------------------------------------------------------------------------
bool CJobGroupImplementation::FScheduleGroupExpressions(duckdb::unique_ptr<CSchedulerContext> psc) {
	auto pgexprLast = m_last_scheduled_expr;
	// iterate on expressions and schedule them as needed
	auto itr = PgexprFirstUnsched();
	while (m_pgroup->m_group_exprs.end() != itr) {
		auto pgexpr = *itr;
		if (!pgexpr->FTransitioned(CGroupExpression::estImplemented) && !pgexpr->ContainsCircularDependencies()) {
			CJobGroupExpressionImplementation::ScheduleJob(psc, pgexpr, this);
			pgexprLast = itr;
		}
		// move to next expression
		{
			CGroupProxy gp(m_pgroup);
			++itr;
		}
	}
	bool fNewJobs = (m_last_scheduled_expr != pgexprLast);
	// set last scheduled expression
	m_last_scheduled_expr = pgexprLast;
	return fNewJobs;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::EevtStartImplementation
//
//	@doc:
//		Start group implementation
//
//---------------------------------------------------------------------------
CJobGroupImplementation::EEvent
CJobGroupImplementation::EevtStartImplementation(duckdb::unique_ptr<CSchedulerContext> psc,
                                                 CJob *pjOwner) {
	// get a job pointer
	auto pjgi = ConvertJob(pjOwner);
#ifdef DEBUG
	CJobGroup::PrintJob(pjgi, "[StartImplementation]");
#endif
	auto pgroup = pjgi->m_pgroup;
	if (!pgroup->FExplored()) {
		// schedule a child exploration job
		CJobGroupExploration::ScheduleJob(psc, pgroup, pjgi);
		return eevExploring;
	} else {
		// move group to implementation state
		{
			CGroupProxy gp(pgroup);
			gp.SetState(CGroup::estImplementing);
		}
		// if this is the root, release exploration jobs
		if (psc->m_engine->FRoot(pgroup)) {
			psc->m_job_factory->Truncate(EjtGroupExploration);
			psc->m_job_factory->Truncate(EjtGroupExpressionExploration);
		}
		return eevExplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::EevtImplementChildren
//
//	@doc:
//		Implement child group expressions
//
//---------------------------------------------------------------------------
CJobGroupImplementation::EEvent
CJobGroupImplementation::EevtImplementChildren(duckdb::unique_ptr<CSchedulerContext> psc,
											   CJob *pjOwner) {
	// get a job pointer
	auto pjgi = ConvertJob(pjOwner);
#ifdef DEBUG
	CJobGroup::PrintJob(pjgi, "[StartImplementChildren]");
#endif
	if (pjgi->FScheduleGroupExpressions(psc)) {
		// if (psc->m_engine->FRoot(pjgi->m_pgroup)) {
		//    start_implementation = clock();
		// }
		// implementation is in progress
		return eevImplementing;
	} else {
		// move group to implemented state
		{
			CGroupProxy gp(pjgi->m_pgroup);
			gp.SetState(CGroup::estImplemented);
		}
		// if this is the root, complete implementation phase
		if (psc->m_engine->FRoot(pjgi->m_pgroup)) {
			// end_implementation = clock();
			// implementation_time = double(end_implementation - start_implementation) / CLOCKS_PER_SEC;
			// FILE* time_f = fopen("/home/ecs-user/giveDuckTopDownOptimizer/expr/result.txt", "a+");
			// fprintf(time_f, "imple = %lf s, ", implementation_time);
			// fclose(time_f);
			psc->m_engine->FinalizeImplementation();
		}
		return eevImplemented;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool
CJobGroupImplementation::FExecute(duckdb::unique_ptr<CSchedulerContext> psc) {
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupImplementation::ScheduleJob
//
//	@doc:
//		Schedule a new group implementation job
//
//---------------------------------------------------------------------------
void CJobGroupImplementation::ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
										  duckdb::unique_ptr<CGroup> pgroup,
										  CJob *pjParent) {
	auto pj = psc->m_job_factory->CreateJob(CJob::EjtGroupImplementation);
	// initialize job
	auto pjgi = ConvertJob(pj);
	pjgi->Init(pgroup);
	psc->m_scheduler->Add(pjgi, pjParent);
}