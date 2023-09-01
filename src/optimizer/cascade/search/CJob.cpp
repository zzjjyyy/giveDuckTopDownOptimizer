//---------------------------------------------------------------------------
//	@filename:
//		CJob.cpp
//
//	@doc:
//		Implementation of optimizer job base class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CJob::Reset
//
//	@doc:
//		Reset job
//
//---------------------------------------------------------------------------
void CJob::Reset()
{
	m_parent_jobs = NULL;
	m_job_queue = NULL;
	m_reference_cnt = 0;
	m_is_initialized = false;
}

//---------------------------------------------------------------------------
//	@function:
//		CJob::FResumeParent
//
//	@doc:
//		Resume parent jobs after job completion
//
//---------------------------------------------------------------------------
BOOL CJob::FResumeParent() const
{
	// decrement parent's ref counter
	ULONG_PTR ulpRefs = m_parent_jobs->DecrRefs();
	// check if job should be resumed
	return (1 == ulpRefs);
}