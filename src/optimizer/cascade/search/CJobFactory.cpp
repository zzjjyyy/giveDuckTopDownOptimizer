//---------------------------------------------------------------------------
//	@filename:
//		CJobFactory.cpp
//
//	@doc:
//		Implementation of optimizer job base class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::CJobFactory
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobFactory::CJobFactory(ULONG num_jobs)
	: m_num_jobs(num_jobs), m_group_opt_jobs(nullptr), m_group_impl_jobs(nullptr), m_group_explore_jobs(nullptr),
      m_group_expr_opt_jobs(nullptr), m_group_expr_impl_jobs(nullptr), m_group_expr_explore_jobs(nullptr),
      m_transform_jobs(nullptr)
{
	// initialize factories to be used first
	Release(CreateJob(CJob::EjtGroupExploration));
	Release(CreateJob(CJob::EjtGroupExpressionExploration));
	Release(CreateJob(CJob::EjtTransformation));
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::~CJobFactory
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobFactory::~CJobFactory()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::CreateJob
//
//	@doc:
//		Create job of specific type
//
//---------------------------------------------------------------------------
CJob* CJobFactory::CreateJob(CJob::EJobType job_type)
{
	CJob* pj;
	switch (job_type)
	{
		case CJob::EjtTest:
			break;
		case CJob::EjtGroupOptimization:
			pj = PtRetrieve<CJobGroupOptimization>(m_group_opt_jobs);
			break;
		case CJob::EjtGroupImplementation:
			pj = PtRetrieve<CJobGroupImplementation>(m_group_impl_jobs);
			break;
		case CJob::EjtGroupExploration:
			pj = PtRetrieve<CJobGroupExploration>(m_group_explore_jobs);
			break;
		case CJob::EjtGroupExpressionOptimization:
			pj = PtRetrieve<CJobGroupExpressionOptimization>(m_group_expr_opt_jobs);
			break;
		case CJob::EjtGroupExpressionImplementation:
			pj = PtRetrieve<CJobGroupExpressionImplementation>(m_group_expr_impl_jobs);
			break;
		case CJob::EjtGroupExpressionExploration:
			pj = PtRetrieve<CJobGroupExpressionExploration>(m_group_expr_explore_jobs);
			break;
		case CJob::EjtTransformation:
			pj = PtRetrieve<CJobTransformation>(m_transform_jobs);
			break;
		case CJob::EjtInvalid:
			GPOS_ASSERT(!"Invalid job type");
	}
	// prepare task
	pj->Reset();
	pj->SetJobType(job_type);
	return pj;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::Release
//
//	@doc:
//		Release completed job
//
//---------------------------------------------------------------------------
void CJobFactory::Release(CJob*job)
{
	switch (job->Ejt())
	{
		case CJob::EjtTest:
			break;
		case CJob::EjtGroupOptimization:
			Release(CJobGroupOptimization::ConvertJob(job), m_group_opt_jobs);
			break;
		case CJob::EjtGroupImplementation:
			Release(CJobGroupImplementation::PjConvert(job), m_group_impl_jobs);
			break;
		case CJob::EjtGroupExploration:
			Release(CJobGroupExploration::PjConvert(job), m_group_explore_jobs);
			break;
		case CJob::EjtGroupExpressionOptimization:
			Release(CJobGroupExpressionOptimization::PjConvert(job), m_group_expr_opt_jobs);
			break;
		case CJob::EjtGroupExpressionImplementation:
			Release(CJobGroupExpressionImplementation::PjConvert(job), m_group_expr_impl_jobs);
			break;
		case CJob::EjtGroupExpressionExploration:
			Release(CJobGroupExpressionExploration::PjConvert(job), m_group_expr_explore_jobs);
			break;
		case CJob::EjtTransformation:
			Release(CJobTransformation::PjConvert(job), m_transform_jobs);
			break;
		default:
			GPOS_ASSERT(!"Invalid job type");
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJobFactory::Truncate
//
//	@doc:
//		Truncate the container for the specific job type
//
//---------------------------------------------------------------------------
void CJobFactory::Truncate(CJob::EJobType job_type)
{
	// need to suspend cancellation while truncating job pool
	{
		switch (job_type)
		{
			case CJob::EjtTest:
				break;
			case CJob::EjtGroupOptimization:
				TruncatePool(m_group_opt_jobs);
				break;
			case CJob::EjtGroupImplementation:
				TruncatePool(m_group_impl_jobs);
				break;
			case CJob::EjtGroupExploration:
				TruncatePool(m_group_explore_jobs);
				break;
			case CJob::EjtGroupExpressionOptimization:
				TruncatePool(m_group_expr_opt_jobs);
				break;
			case CJob::EjtGroupExpressionImplementation:
				TruncatePool(m_group_expr_impl_jobs);
				break;
			case CJob::EjtGroupExpressionExploration:
				TruncatePool(m_group_expr_explore_jobs);
				break;
			case CJob::EjtTransformation:
				TruncatePool(m_transform_jobs);
				break;
			case CJob::EjtInvalid:
				GPOS_ASSERT(!"Invalid job type");
		}
	}
}