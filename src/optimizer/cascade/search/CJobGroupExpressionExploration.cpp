//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionExploration.cpp
//
//	@doc:
//		Implementation of group expression exploration job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"

#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

namespace gpopt {
// State transition diagram for group expression exploration job state machine;
//
// +-----------------------+   eevExploringChildren
// |    estInitialized:    | -----------------------+
// | EevtExploreChildren() |                        |
// |                       | <----------------------+
// +-----------------------+
//   |
//   | eevChildrenExplored
//   v
// +-----------------------+   eevExploringSelf
// | estChildrenExplored:  | -----------------------+
// |   EevtExploreSelf()   |                        |
// |                       | <----------------------+
// +-----------------------+
//   |
//   | eevSelfExplored
//   v
// +-----------------------+
// |   estSelfExplored:    |
// |    EevtFinalize()     |
// +-----------------------+
//   |
//   | eevFinalized
//   v
// +-----------------------+
// |     estCompleted      |
// +-----------------------+
//
const CJobGroupExpressionExploration::EEvent
    rgeev3[CJobGroupExpressionExploration::estSentinel][CJobGroupExpressionExploration::estSentinel] = {
        {CJobGroupExpressionExploration::eevExploringChildren, CJobGroupExpressionExploration::eevChildrenExplored,
         CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel},
        {CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevExploringSelf,
         CJobGroupExpressionExploration::eevSelfExplored, CJobGroupExpressionExploration::eevSentinel},
        {CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel,
         CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevFinalized},
        {CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel,
         CJobGroupExpressionExploration::eevSentinel, CJobGroupExpressionExploration::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::CJobGroupExpressionExploration
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::CJobGroupExpressionExploration() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::~CJobGroupExpressionExploration
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::~CJobGroupExpressionExploration() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::Init(duckdb::unique_ptr<CGroupExpression> pgexpr) {
	CJobGroupExpression::Init(pgexpr);
	m_jsm.Init(rgeev3);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtExploreChildren);
	m_jsm.SetAction(estChildrenExplored, EevtExploreSelf);
	m_jsm.SetAction(estSelfExplored, EevtFinalize);
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::ScheduleApplicableTransformations
//
//	@doc:
//		Schedule transformation jobs for all applicable xforms
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::ScheduleApplicableTransformations(duckdb::unique_ptr<CSchedulerContext> psc) {
	// get all applicable xforms
	duckdb::unique_ptr<CXform_set> xform_set = unique_ptr_cast<Operator, LogicalOperator>(m_group_expression->m_operator)->XformCandidates();
	// intersect them with required xforms and schedule jobs
	*xform_set &= *(CXformFactory::XformFactory()->XformExploration());
	*xform_set &= *(psc->m_engine->CurrentStageXforms());
	ScheduleTransformations(psc, xform_set);
	xform_set->reset();
	SetXformsScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::ScheduleChildGroupsJobs
//
//	@doc:
//		Schedule exploration jobs for all child groups
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::ScheduleChildGroupsJobs(duckdb::unique_ptr<CSchedulerContext> psc) {
	ULONG arity = m_group_expression->Arity();
	for (ULONG i = 0; i < arity; i++) {
		CJobGroupExploration::ScheduleJob(psc, (*(m_group_expression))[i], this);
	}
	SetChildrenScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::EevtExploreChildren
//
//	@doc:
//		Explore child groups
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::EEvent
CJobGroupExpressionExploration::EevtExploreChildren(duckdb::unique_ptr<CSchedulerContext> psc,
                                                    CJob *pjOwner) {
	// get a job pointer
	auto pjgee = PjConvert(pjOwner);
#ifdef DEBUG
	CJobGroupExpression::PrintJob(pjgee, "[Expression: ExploreChildren]");
#endif
	if (!pjgee->FChildrenScheduled()) {
		pjgee->m_group_expression->SetState(CGroupExpression::estExploring);
		pjgee->ScheduleChildGroupsJobs(psc);
		return eevExploringChildren;
	} else {
		return eevChildrenExplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::EevtExploreSelf
//
//	@doc:
//		Explore group expression
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::EEvent
CJobGroupExpressionExploration::EevtExploreSelf(duckdb::unique_ptr<CSchedulerContext> psc,
                                                CJob *pjOwner) {
	// get a job pointer
	auto pjgee = PjConvert(pjOwner);
#ifdef DEBUG
	CJobGroupExpression::PrintJob(pjgee, "[Expression: ExploreSelf]");
#endif
	if (!pjgee->FXformsScheduled()) {
		pjgee->ScheduleApplicableTransformations(psc);
		return eevExploringSelf;
	} else {
		return eevSelfExplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::EevtFinalize
//
//	@doc:
//		Finalize exploration
//
//---------------------------------------------------------------------------
CJobGroupExpressionExploration::EEvent
CJobGroupExpressionExploration::EevtFinalize(duckdb::unique_ptr<CSchedulerContext> psc,
                                             CJob *pjOwner) {
	// get a job pointer
	auto pjgee = PjConvert(pjOwner);
	pjgee->m_group_expression->SetState(CGroupExpression::estExplored);
#ifdef DEBUG
	CJobGroupExpression::PrintJob(pjgee, "[Expression: Finalize]");
#endif
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool CJobGroupExpressionExploration::FExecute(duckdb::unique_ptr<CSchedulerContext> psc) {
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionExploration::ScheduleJob
//
//	@doc:
//		Schedule a new group expression exploration job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionExploration::ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
												 duckdb::unique_ptr<CGroupExpression> pgexpr,
												 CJob *pjParent) {
	auto pj = psc->m_job_factory->CreateJob(CJob::EjtGroupExpressionExploration);
	// initialize job
	auto pjege = PjConvert(pj);
	pjege->Init(pgexpr);
	psc->m_scheduler->Add(pjege, pjParent);
}
} // namespace gpopt