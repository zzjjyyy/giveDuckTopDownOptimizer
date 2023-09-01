//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2011 Greenplum, Inc.
//
//	@filename:
//		CScheduler.h
//
//	@doc:
//		Scheduler interface for execution of optimization jobs
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncList.h"
#include "duckdb/optimizer/cascade/common/CSyncPool.h"
#include "duckdb/optimizer/cascade/search/CJob.h"

#define OPT_SCHED_QUEUED_RUNNING_RATIO 10
#define OPT_SCHED_CFA                  100

namespace gpopt {
using namespace gpos;

// prototypes
class CSchedulerContext;

//---------------------------------------------------------------------------
//	@class:
//		CScheduler
//
//	@doc:
//		MT-scheduler for optimization jobs
//
//		Maintaining job dependencies and controlling the order of job execution
//		are the main responsibilities of job scheduler.
//		The scheduler maintains a list of jobs to be run. These are the jobs
//		with no dependencies.  Iteratively, the scheduler picks a runnable
//		(with no dependencies) job and calls CJob::FExecute() function using
//		that job object.
//		The function CJob::FExecute() hides the complexity of job execution
//		from the job scheduler. The expectation is that CJob::FExecute()
//		returns TRUE only when job execution is finished.  Otherwise, job
//		execution is not finished and the scheduler moves the job to a pending
//		state.
//
//		On completion of job J1, the scheduler also takes care of notifying all
//		jobs in the job queue attached to J1 that the execution of J1 is now
//		complete. At this point, a queued job can be terminated if it does not
//		have any further dependencies.
//
//---------------------------------------------------------------------------
class CScheduler {
	// friend classes
	friend class CJob;

public:
	// enum for job execution result
	enum EJobResult { EjrRunnable = 0, EjrSuspended, EjrCompleted, EjrSentinel };

public:
	explicit CScheduler(ULONG num_jobs);
	CScheduler(const CScheduler &) = delete;
	virtual ~CScheduler();

	// job wrapper; used for inserting job to waiting list (lock-free)
	struct SJobLink {
		// link id, set by sync set
		ULONG m_id;
		// pointer to job
		CJob *m_job;
		// slink for list of waiting jobs
		SLink m_link;
		// initialize link
		void Init(CJob *pj) {
			m_job = pj;
			m_link.m_prev = m_link.m_next = nullptr;
		}
	};

	// list of jobs waiting to execute
	CSyncList<SJobLink> m_todo_jobs;
	// pool of job link objects
	CSyncPool<SJobLink> m_job_links;
	// current job counters
	ULONG_PTR m_num_total;
	ULONG_PTR m_num_running;
	ULONG_PTR m_num_queued;
	// stats
	ULONG_PTR m_stats_queued;
	ULONG_PTR m_stats_dequeued;
	ULONG_PTR m_stats_suspended;
	ULONG_PTR m_stats_completed;
	ULONG_PTR m_stats_completed_queued;
	ULONG_PTR m_stats_resumed;

public:
	// keep executing jobs (if any)
	void ExecuteJobs(CSchedulerContext *psc);
	// process job execution results
	void ProcessJobResult(CJob *pj, CSchedulerContext *psc, bool fCompleted);
	// retrieve next job to run
	CJob *RetrieveJob();
	// schedule job for execution
	void Schedule(CJob *pj);
	// prepare for job execution
	void PreExecute(CJob *pj);
	// execute job
	bool FExecute(CJob *pj, CSchedulerContext *psc);
	// process job execution outcome
	EJobResult JobPostExecute(CJob *pj, bool fCompleted);
	// resume parent job
	void ResumeParent(CJob *pj);
	// check if all jobs have completed
	bool IsEmpty() const {
		return (0 == m_num_total);
	}
	// main job processing task
	static void *Run(void *);
	// transition job to completed
	void Complete(CJob *pj);
	// transition queued job to completed
	void CompleteQueued(CJob *pj);
	// transition job to suspended
	void Suspend(CJob *pj);
	// add new job for scheduling
	void Add(CJob *pj, CJob *pjParent);
	// resume suspended job
	void Resume(CJob *pj);
}; // class CScheduler
} // namespace gpopt