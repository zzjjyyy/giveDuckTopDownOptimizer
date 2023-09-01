//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJobGroupExploration.cpp
//
//	@doc:
//		Implementation of group exploration job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"

#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"

using namespace gpopt;

// State transition diagram for group exploration job state machine;
//
// +------------------------+
// |    estInitialized:     |
// | EevtStartExploration() |
// +------------------------+
//   |
//   | eevStartedExploration
//   v
// +------------------------+   eevNewChildren
// | estExploringChildren:  | -----------------+
// | EevtExploreChildren()  |                  |
// |                        | <----------------+
// +------------------------+
//   |
//   | eevExplored
//   v
// +------------------------+
// |      estCompleted      |
// +------------------------+
//
const CJobGroupExploration::EEvent rgeev2[CJobGroupExploration::estSentinel][CJobGroupExploration::estSentinel] = {
    {CJobGroupExploration::eevSentinel, CJobGroupExploration::eevStartedExploration, CJobGroupExploration::eevSentinel},
    {CJobGroupExploration::eevSentinel, CJobGroupExploration::eevNewChildren, CJobGroupExploration::eevExplored},
    {CJobGroupExploration::eevSentinel, CJobGroupExploration::eevSentinel, CJobGroupExploration::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::CJobGroupExploration
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExploration::CJobGroupExploration() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::~CJobGroupExploration
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExploration::~CJobGroupExploration() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupExploration::Init(CGroup *pgroup) {
	CJobGroup::Init(pgroup);
	m_jsm.Init(rgeev2);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtStartExploration);
	m_jsm.SetAction(estExploringChildren, EevtExploreChildren);
	SetJobQueue(&pgroup->m_explore_job_queue);
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::FScheduleGroupExpressions
//
//	@doc:
//		Schedule exploration jobs for all unexplored group expressions;
//		the function returns true if it could schedule any new jobs
//
//---------------------------------------------------------------------------
bool CJobGroupExploration::FScheduleGroupExpressions(CSchedulerContext *psc) {
	auto last_expr = m_last_scheduled_expr;
	// iterate on expressions and schedule them as needed
	auto itr = PgexprFirstUnsched();
	while (m_pgroup->m_group_exprs.end() != itr) {
		CGroupExpression *expr = *itr;
		if (!expr->FTransitioned(CGroupExpression::estExplored)) {
			CJobGroupExpressionExploration::ScheduleJob(psc, expr, this);
			last_expr = itr;
		}
		// move to next expression
		{
			CGroupProxy gp(m_pgroup);
			++itr;
		}
	}
	bool fNewJobs = (m_last_scheduled_expr != last_expr);
	// set last scheduled expression
	m_last_scheduled_expr = itr;
	return fNewJobs;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::EevtStartExploration
//
//	@doc:
//		Start group exploration
//
//---------------------------------------------------------------------------
CJobGroupExploration::EEvent CJobGroupExploration::EevtStartExploration(CSchedulerContext *psc, CJob *pjOwner) {
	CJobGroup::PrintJob(ConvertJob(pjOwner), "[StartExploration]");

	// get a job pointer
	CJobGroupExploration *pjge = ConvertJob(pjOwner);
	CGroup *pgroup = pjge->m_pgroup;
	// move group to exploration state
	{
		CGroupProxy gp(pgroup);
		gp.SetState(CGroup::estExploring);
	}
	return eevStartedExploration;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::EevtExploreChildren
//
//	@doc:
//		Explore child group expressions
//
//---------------------------------------------------------------------------
CJobGroupExploration::EEvent CJobGroupExploration::EevtExploreChildren(CSchedulerContext *psc, CJob *pjOwner) {
	CJobGroup::PrintJob(ConvertJob(pjOwner), "[ExploreChildren]");

	// get a job pointer
	CJobGroupExploration *pjge = ConvertJob(pjOwner);
	if (pjge->FScheduleGroupExpressions(psc)) {
		// new expressions have been added to group
		return eevNewChildren;
	} else {
		// no new expressions have been added to group, move to explored state
		{
			CGroupProxy gp(pjge->m_pgroup);
			gp.SetState(CGroup::estExplored);
		}
		// if this is the root, complete exploration phase
		if (psc->m_engine->FRoot(pjge->m_pgroup)) {
			psc->m_engine->FinalizeExploration();
		}
		return eevExplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool CJobGroupExploration::FExecute(CSchedulerContext *psc) {
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::ScheduleJob
//
//	@doc:
//		Schedule a new group exploration job
//
//---------------------------------------------------------------------------
void CJobGroupExploration::ScheduleJob(CSchedulerContext *psc, CGroup *pgroup, CJob *pjParent) {
	CJob *pj = psc->m_job_factory->CreateJob(CJob::EjtGroupExploration);
	// initialize job
	CJobGroupExploration *pjge = ConvertJob(pj);
	pjge->Init(pgroup);
	psc->m_scheduler->Add(pjge, pjParent);
}