//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionImplementation.cpp
//
//	@doc:
//		Implementation of group expression implementation job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"

#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"
#include "duckdb/planner/logical_operator.hpp"

using namespace gpopt;

// State transition diagram for group expression implementation job state machine;
//
// +-------------------------+   eevImplementingChildren
// |     estInitialized:     | --------------------------+
// | EevtImplementChildren() |                           |
// |                         | <-------------------------+
// +-------------------------+
//   |
//   | eevChildrenImplemented
//   v
// +-------------------------+   eevImplementingSelf
// | estChildrenImplemented: | --------------------------+
// |   EevtImplementSelf()   |                           |
// |                         | <-------------------------+
// +-------------------------+
//   |
//   | eevSelfImplemented
//   v
// +-------------------------+
// |   estSelfImplemented:   |
// |     EevtFinalize()      |
// +-------------------------+
//   |
//   | eevFinalized
//   v
// +-------------------------+
// |      estCompleted       |
// +-------------------------+
//
const CJobGroupExpressionImplementation::EEvent
    rgeev6[CJobGroupExpressionImplementation::estSentinel][CJobGroupExpressionImplementation::estSentinel] = {
        {CJobGroupExpressionImplementation::eevImplementingChildren,
         CJobGroupExpressionImplementation::eevChildrenImplemented, CJobGroupExpressionImplementation::eevSentinel,
         CJobGroupExpressionImplementation::eevSentinel},
        {CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevImplementingSelf,
         CJobGroupExpressionImplementation::eevSelfImplemented, CJobGroupExpressionImplementation::eevSentinel},
        {CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel,
         CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevFinalized},
        {CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel,
         CJobGroupExpressionImplementation::eevSentinel, CJobGroupExpressionImplementation::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::CJobGroupExpressionImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::CJobGroupExpressionImplementation() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::~CJobGroupExpressionImplementation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::~CJobGroupExpressionImplementation() {
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionImplementation::Init(duckdb::unique_ptr<CGroupExpression> pgexpr) {
	CJobGroupExpression::Init(pgexpr);
	GPOS_ASSERT(pgexpr->Pop()->FLogical());
	m_job_state_machine.Init(rgeev6);
	// set job actions
	m_job_state_machine.SetAction(estInitialized, EevtImplementChildren);
	m_job_state_machine.SetAction(estChildrenImplemented, EevtImplementSelf);
	m_job_state_machine.SetAction(estSelfImplemented, EevtFinalize);
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::ScheduleApplicableTransformations
//
//	@doc:
//		Schedule transformation jobs for all applicable xforms
//
//---------------------------------------------------------------------------
void
CJobGroupExpressionImplementation::ScheduleApplicableTransformations(duckdb::unique_ptr<CSchedulerContext> psc) {
	// get all applicable xforms
	auto xform_set = ((LogicalOperator *)m_group_expression->m_operator.get())->XformCandidates();
	// intersect them with required xforms and schedule jobs
	*xform_set &= *(CXformFactory::XformFactory()->XformImplementation());
	*xform_set &= *(psc->m_engine->CurrentStageXforms());
	ScheduleTransformations(psc, xform_set);
	SetXformsScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::ScheduleChildGroupsJobs
//
//	@doc:
//		Schedule implementation jobs for all child groups
//
//---------------------------------------------------------------------------
void
CJobGroupExpressionImplementation::ScheduleChildGroupsJobs(duckdb::unique_ptr<CSchedulerContext> psc) {
	ULONG arity = m_group_expression->Arity();
	for (ULONG i = 0; i < arity; i++) {
		CJobGroupImplementation::ScheduleJob(psc, (*(m_group_expression))[i], this);
	}
	SetChildrenScheduled();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::EevtImplementChildren
//
//	@doc:
//		Implement child groups
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::EEvent
CJobGroupExpressionImplementation::EevtImplementChildren(duckdb::unique_ptr<CSchedulerContext> psc,
														 CJob *pjOwner) {
	// get a job pointer
	auto pjgei = PjConvert(pjOwner);
#ifdef DEBUG
	CJobGroupExpression::PrintJob(PjConvert(pjOwner), "[Expression: ImplementChildren]");
#endif
	if (!pjgei->FChildrenScheduled()) {
		pjgei->m_group_expression->SetState(CGroupExpression::estImplementing);
		pjgei->ScheduleChildGroupsJobs(psc);
		return eevImplementingChildren;
	} else {
		return eevChildrenImplemented;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::EevtImplementSelf
//
//	@doc:
//		Implement group expression
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::EEvent
CJobGroupExpressionImplementation::EevtImplementSelf(duckdb::unique_ptr<CSchedulerContext> psc,
                                                     CJob *pjOwner) {
	// get a job pointer
	auto pjgei = PjConvert(pjOwner);
#ifdef DEBUG
	CJobGroupExpression::PrintJob(PjConvert(pjOwner), "[Expression: ImplementSelf]");
#endif
	if (!pjgei->FXformsScheduled()) {
		pjgei->ScheduleApplicableTransformations(psc);
		return eevImplementingSelf;
	} else {
		return eevSelfImplemented;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::EevtFinalize
//
//	@doc:
//		Finalize implementation
//
//---------------------------------------------------------------------------
CJobGroupExpressionImplementation::EEvent
CJobGroupExpressionImplementation::EevtFinalize(duckdb::unique_ptr<CSchedulerContext> psc,
                                                CJob *pjOwner) {
#ifdef DEBUG
	CJobGroupExpression::PrintJob(PjConvert(pjOwner), "[Expression: ImplementFinalize]");
#endif
	// get a job pointer
	auto pjgei = PjConvert(pjOwner);
	pjgei->m_group_expression->SetState(CGroupExpression::estImplemented);
	return eevFinalized;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool
CJobGroupExpressionImplementation::FExecute(duckdb::unique_ptr<CSchedulerContext> psc) {
	return m_job_state_machine.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpressionImplementation::ScheduleJob
//
//	@doc:
//		Schedule a new group expression implementation job
//
//---------------------------------------------------------------------------
void CJobGroupExpressionImplementation::ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
													duckdb::unique_ptr<CGroupExpression> pgexpr,
													CJob *pjParent) {
	auto pj = psc->m_job_factory->CreateJob(CJob::EjtGroupExpressionImplementation);
	// initialize job
	auto pjige = PjConvert(pj);
	pjige->Init(pgexpr);
	psc->m_scheduler->Add(pjige, pjParent);
}