//---------------------------------------------------------------------------
//	@filename:
//		CJobFactory.h
//
//	@doc:
//		Highly concurrent job factory;
//		Uses bulk memory allocation and atomic primitives to
//		create and recycle jobs with minimal sychronization;
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncPool.h"
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionOptimization.h"
#include "duckdb/optimizer/cascade/search/CJobGroupImplementation.h"
#include "duckdb/optimizer/cascade/search/CJobGroupOptimization.h"
#include "duckdb/optimizer/cascade/search/CJobTransformation.h"

namespace gpopt {
using namespace gpos;
//---------------------------------------------------------------------------
//	@class:
//		CJobFactory
//
//	@doc:
//		Highly concurrent job factory
//
//		The factory uses bulk memory allocation to create and recycle jobs with
//		minimal synchronization. The factory maintains a lock-free list defined
//		by the class CSyncPool for each job type. This allows concurrent
//		retrieval of jobs from the lists without the need for synchronization
//		through heavy locking operations.
//		A lock-free list is pre-allocated as an array of given size. The
//		allocation of lock-free lists happens lazily when the first job of a
//		given type is created.
//		Each job is given a unique id. When a job needs to be retrieved from
//		the list, atomic operations are used to reserve a free job object and
//		return it to the caller.
//
//---------------------------------------------------------------------------
class CJobFactory {
public:
	explicit CJobFactory(ULONG num_jobs);
	CJobFactory(const CJobFactory &) = delete;
	~CJobFactory();

	// number of jobs in each pool
	const ULONG m_num_jobs;

	// container for group optimization jobs
	CSyncPool<CJobGroupOptimization> *m_group_opt_jobs;
	// container for group implementation jobs
	CSyncPool<CJobGroupImplementation> *m_group_impl_jobs;
	// container for group exploration jobs
	CSyncPool<CJobGroupExploration> *m_group_explore_jobs;
	// container for group expression optimization jobs
	CSyncPool<CJobGroupExpressionOptimization> *m_group_expr_opt_jobs;
	// container for group expression implementation jobs
	CSyncPool<CJobGroupExpressionImplementation> *m_group_expr_impl_jobs;
	// container for group expression exploration jobs
	CSyncPool<CJobGroupExpressionExploration> *m_group_expr_explore_jobs;
	// container for transformation jobs
	CSyncPool<CJobTransformation> *m_transform_jobs;

public:
	// retrieve job of specific type
	template <class T>
	T *PtRetrieve(CSyncPool<T> *&pool) {
		if (nullptr == pool) {
			pool = new CSyncPool<T>(m_num_jobs);
			T *tmp = new T();
			SIZE_T id_offset = (SIZE_T)(&(tmp->m_id)) - (SIZE_T)tmp;
			pool->Init((gpos::ULONG)id_offset);
		}
		return pool->PtRetrieve();
	}
	// release job
	template <class T>
	void Release(T *pt, CSyncPool<T> *pool) {
		pool->Recycle(pt);
	}
	// truncate job pool
	template <class T>
	void TruncatePool(CSyncPool<T> *&pool) {
		delete pool;
		pool = nullptr;
	}

	// create job of specific type
	CJob *CreateJob(CJob::EJobType job_type);
	// release completed job
	void Release(CJob *job);
	// truncate the container for the specific job type
	void Truncate(CJob::EJobType job_type);

}; // class CJobFactory
} // namespace gpopt