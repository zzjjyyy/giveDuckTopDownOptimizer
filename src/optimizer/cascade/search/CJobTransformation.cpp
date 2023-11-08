//---------------------------------------------------------------------------
//	@filename:
//		CJobTransformation.cpp
//
//	@doc:
//		Implementation of group expression transformation job
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include <iostream>

using namespace gpopt;

// State transition diagram for transformation job state machine;
//
// +-----------------+
// | estInitialized: |
// | EevtTransform() |
// +-----------------+
//   |
//   | eevCompleted
//   v
// +-----------------+
// |  estCompleted   |
// +-----------------+
//
const CJobTransformation::EEvent rgeev5[CJobTransformation::estSentinel][CJobTransformation::estSentinel] =
{
	{ CJobTransformation::eevSentinel, CJobTransformation::eevCompleted},
	{ CJobTransformation::eevSentinel, CJobTransformation::eevSentinel},
};

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::CJobTransformation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobTransformation::CJobTransformation()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::~CJobTransformation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobTransformation::~CJobTransformation()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobTransformation::Init(duckdb::unique_ptr<CGroupExpression> pgexpr,
							  duckdb::unique_ptr<CXform> pxform)
{
	m_pgexpr = pgexpr;
	m_xform = pxform;
	m_jsm.Init(rgeev5);
	// set job actions
	m_jsm.SetAction(estInitialized, EevtTransform);
	// mark as initialized
	CJob::SetInit();
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::EevtTransform
//
//	@doc:
//		Apply transformation action
//
//---------------------------------------------------------------------------
CJobTransformation::EEvent
CJobTransformation::EevtTransform(duckdb::unique_ptr<CSchedulerContext> psc,
								  CJob *pj)
{
#ifdef DEBUG
	std::cout << "Do Transformation" << std::endl;
#endif
	// get a job pointer
	auto pjt = PjConvert(pj);
	auto pgexpr = pjt->m_pgexpr;
	auto pxform = pjt->m_xform;
	// insert transformation results to memo
	auto pxfres = make_uniq<CXformResult>();
	ULONG ulElapsedTime = 0;
	ULONG ulNumberOfBindings = 0;
	pgexpr->Transform(pgexpr, pxform, pxfres, &ulElapsedTime, &ulNumberOfBindings);
	psc->m_engine->InsertXformResult(pgexpr->m_group, pxfres, pxform->ID(), pgexpr, ulElapsedTime, ulNumberOfBindings);
	return eevCompleted;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
bool CJobTransformation::FExecute(duckdb::unique_ptr<CSchedulerContext> psc)
{
	return m_jsm.FRun(psc, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::ScheduleJob
//
//	@doc:
//		Schedule a new transformation job
//
//---------------------------------------------------------------------------
void CJobTransformation::ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
									 duckdb::unique_ptr<CGroupExpression> pgexpr,
									 duckdb::unique_ptr<CXform> pxform,
									 CJob *pjParent)
{
	auto pj = psc->m_job_factory->CreateJob(CJob::EjtTransformation);
	// initialize job
	auto pjt = PjConvert(pj);
	pjt->Init(pgexpr, pxform);
	psc->m_scheduler->Add(pjt, pjParent);
}