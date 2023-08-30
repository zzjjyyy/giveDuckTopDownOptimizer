//---------------------------------------------------------------------------
//	@filename:
//		CSchedulerContext.h
//
//	@doc:
//		Container for objects associated with scheduling context of a job
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"

#define GPOPT_SCHED_CTXT_MEM_POOL_SIZE (64 * 1024 * 1024)

namespace gpopt {
using namespace gpos;

// prototypes
class CJobFactory;
class CScheduler;
class CEngine;

//---------------------------------------------------------------------------
//	@class:
//		CSchedulerContext
//
//	@doc:
//		Scheduling context
//
//---------------------------------------------------------------------------
class CSchedulerContext {
public:
	CSchedulerContext();
	CSchedulerContext(const CSchedulerContext &) = delete;
	~CSchedulerContext();

	// job factory
	CJobFactory *m_job_factory;
	// scheduler
	CScheduler *m_scheduler;
	// optimization engine
	CEngine *m_engine;
	// flag indicating if context has been initialized
	bool m_initialized;

public:
	// initialization
	void Init(CJobFactory *job_factory, CScheduler *scheduler, CEngine *engine);
}; // class CSchedulerContext
} // namespace gpopt