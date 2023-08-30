//---------------------------------------------------------------------------
//	@filename:
//		CScheduler.cpp
//
//	@doc:
//		Implementation of optimizer job scheduler
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CScheduler.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"

#include <assert.h>

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CScheduler::CScheduler
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScheduler::CScheduler(ULONG num_jobs)
    : m_job_links(num_jobs), m_num_total(0), m_num_running(0), m_num_queued(0), m_stats_queued(0), m_stats_dequeued(0),
      m_stats_suspended(0), m_stats_completed(0), m_stats_completed_queued(0), m_stats_resumed(0) {
	SJobLink *job_link = new SJobLink();
	SIZE_T id_offset = (SIZE_T)(&(job_link->m_id)) - (SIZE_T)job_link;
	SIZE_T link_offset = (SIZE_T)(&(job_link->m_link)) - (SIZE_T)job_link;
	delete job_link;
	// initialize pool of job links
	m_job_links.Init((gpos::ULONG)id_offset);
	// initialize list of waiting new jobs
	m_todo_jobs.Init((gpos::ULONG)link_offset);
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::~CScheduler
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScheduler::~CScheduler() {
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Run
//
//	@doc:
//		Main job processing task
//
//---------------------------------------------------------------------------
void *CScheduler::Run(void *pv) {
	CSchedulerContext *scheduler_context = reinterpret_cast<CSchedulerContext *>(pv);
	scheduler_context->m_scheduler->ExecuteJobs(scheduler_context);
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::ExecuteJobs
//
//	@doc:
// 		Job processing loop;
//		keeps executing jobs as long as there is work queued;
//
//---------------------------------------------------------------------------
void CScheduler::ExecuteJobs(CSchedulerContext *psc) {
	CJob *job;
	ULONG count = 0;
	// keep retrieving jobs
	while (nullptr != (job = RetrieveJob())) {
		// prepare for job execution
		PreExecute(job);
		// execute job
		bool is_completed = FExecute(job, psc);
		// process job result
		switch (JobPostExecute(job, is_completed)) {
		case EjrCompleted:
			// job is completed
			Complete(job);
			psc->m_job_factory->Release(job);
			break;
		case EjrRunnable:
			// child jobs have completed, job can immediately resume
			Resume(job);
			continue;
		case EjrSuspended:
			// job is suspended until child jobs complete
			Suspend(job);
			break;
		default:
			assert(false);
		}
		if (++count == OPT_SCHED_CFA) {
			count = 0;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Add
//
//	@doc:
//		Add new job for execution
//
//---------------------------------------------------------------------------
void CScheduler::Add(CJob *pj, CJob *pjParent) {
	// increment ref counter for parent job
	if (nullptr != pjParent) {
		pjParent->IncRefs();
	}
	// set current job as parent of its child
	pj->SetParent(pjParent);
	// increment total number of jobs
	m_num_total++;
	Schedule(pj);
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Resume
//
//	@doc:
//		Resume suspended job
//
//---------------------------------------------------------------------------
void CScheduler::Resume(CJob *pj) {
	Schedule(pj);
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Schedule
//
//	@doc:
//		Schedule job for execution
//
//---------------------------------------------------------------------------
void CScheduler::Schedule(CJob *pj) {
	// get job link
	SJobLink *pjl = m_job_links.PtRetrieve();
	pjl->Init(pj);
	// add to waiting list
	m_todo_jobs.Push(pjl);
	// increment number of queued jobs
	m_num_queued++;
	// update statistics
	m_stats_queued++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::PreExecute
//
//	@doc:
// 		Prepare for job execution
//
//---------------------------------------------------------------------------
void CScheduler::PreExecute(CJob *pj) {
	// increment number of running jobs
	m_num_running++;
	// increment job ref counter
	pj->IncRefs();
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::FExecute
//
//	@doc:
//		Execution function using job queue
//
//---------------------------------------------------------------------------
bool CScheduler::FExecute(CJob *pj, CSchedulerContext *psc) {
	bool is_completed = true;
	CJobQueue *job_queue = pj->Pjq();
	// check if job is associated to a job queue
	if (nullptr == job_queue) {
		is_completed = pj->FExecute(psc);
	} else {
		switch (job_queue->EjqrAdd(pj)) {
		case CJobQueue::EjqrMain:
			// main job, runs job operation
			is_completed = pj->FExecute(psc);
			if (is_completed) {
				// notify queued jobs
				job_queue->NotifyCompleted(psc);
			} else {
				// task is suspended
				(void)pj->UlpDecrRefs();
			}
			break;
		case CJobQueue::EjqrQueued:
			// queued job
			is_completed = false;
			break;
		case CJobQueue::EjqrCompleted:
			break;
		}
	}
	return is_completed;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::JobPostExecute
//
//	@doc:
// 		Process job execution outcome
//
//---------------------------------------------------------------------------
CScheduler::EJobResult CScheduler::JobPostExecute(CJob *pj, bool is_completed) {
	// decrement job ref counter
	ULONG_PTR ulRefs = pj->UlpDecrRefs();
	// decrement number of running jobs
	m_num_running--;
	// check if job completed
	if (is_completed) {
		return EjrCompleted;
	}
	// check if all children have completed
	if (1 == ulRefs) {
		return EjrRunnable;
	}
	return EjrSuspended;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::RetrieveJob
//
//	@doc:
//		Retrieve next runnable job from queue
//
//---------------------------------------------------------------------------
CJob *CScheduler::RetrieveJob() {
	// retrieve runnable job from lists of waiting jobs
	SJobLink *pjl = m_todo_jobs.Pop();
	CJob *pj = nullptr;
	if (NULL != pjl) {
		pj = pjl->m_job;
		// decrement number of queued jobs
		m_num_queued--;
		// update statistics
		m_stats_dequeued++;
		// recycle job link
		m_job_links.Recycle(pjl);
	}
	return pj;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Suspend
//
//	@doc:
//		Transition job to suspended
//
//---------------------------------------------------------------------------
void CScheduler::Suspend(CJob *) {
	m_stats_suspended++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::Complete
//
//	@doc:
//		Transition job to completed
//
//---------------------------------------------------------------------------
void CScheduler::Complete(CJob *pj) {
	ResumeParent(pj);
	// update statistics
	m_num_total--;
	m_stats_completed++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::CompleteQueued
//
//	@doc:
//		Transition queued job to completed
//
//---------------------------------------------------------------------------
void CScheduler::CompleteQueued(CJob *pj) {
	ResumeParent(pj);
	// update statistics
	m_num_total--;
	m_stats_completed++;
	m_stats_completed_queued++;
}

//---------------------------------------------------------------------------
//	@function:
//		CScheduler::ResumeParent
//
//	@doc:
//		Resume parent job
//
//---------------------------------------------------------------------------
void CScheduler::ResumeParent(CJob *pj) {
	CJob *pjParent = pj->PjParent();
	if (nullptr != pjParent) {
		// notify parent job
		if (pj->FResumeParent()) {
			// reschedule parent
			Resume(pjParent);
			// update statistics
			m_stats_resumed++;
		}
	}
}
} // namespace gpopt