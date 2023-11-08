//---------------------------------------------------------------------------
//	@filename:
//		CJob.h
//
//	@doc:
//		Interface class for optimization job abstraction
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"

namespace gpopt {
using namespace gpos;

// prototypes
class CJobQueue;
class CScheduler;
class CSchedulerContext;

//---------------------------------------------------------------------------
//	@class:
//		CJob
//
//	@doc:
//		Superclass of all optimization jobs
//
//		The creation of different types of jobs happens inside CJobFactory
//		class such that each job is given a unique id.
//
//		Job Dependencies:
//		Each job has one parent given by the member variable CJob::m_parent_jobs.
//		Thus, the dependency graph that defines dependencies is effectively a
//		tree.	The root optimization is scheduled in CEngine::ScheduleMainJob()
//
//		A job can have any number of dependent (child) jobs. Execution within a
//		job cannot proceed as long as there is one or more dependent jobs that
//		are not finished yet.  Pausing a child job does not also allow the
//		parent job to proceed. The number of job dependencies (children) is
//		maintained by the member variable CJob::m_reference_cnt. The increment and
//		decrement of number of children are atomic operations performed by
//		CJob::IncRefs() and CJob::DecrRefs() functions, respectively.
//
//		Job Queue:
//		Each job maintains a job queue CJob::m_job_queue of other identical jobs that
//		are created while a given job is executing. For example, when exploring
//		a group, a group exploration job J1 would be executing. Concurrently,
//		another group exploration job J2 (for the same group) may be triggered
//		by another worker. The job J2 would be added in a pending state to the
//		job queue of J1. When J1 terminates, all jobs in its queue are notified
//		to pick up J1 results.
//
//		Job reentrance:
//		All optimization jobs are designed to be reentrant. This means that
//		there is a mechanism to support pausing a given job J1, moving
//		execution to another job J2, and then possibly returning to J1 to
//		resume execution. The re-entry point to J1 must be the same as the
//		point where J1 was paused. This mechanism is implemented using a state
//		machine.
//
//		Job Execution:
//		Each job defines two enumerations: EState to define the different
//		states during job execution and EEvent to define the different events
//		that cause moving from one state to another. These two enumerations are
//		used to define job state machine m_job_state_machine, which is an object of
//		CJobStateMachine class. Note that the states, events and state machines
//		are job-specific. This is why each job class has its own definitions of
//		the job states, events & state machine.
//
//		See CJobStateMachine.h for more information about how jobs are executed
//		using the state machine. Also see CScheduler.h for more information
//		about how job are scheduled.
//
//---------------------------------------------------------------------------
class CJob {
	// friends
	friend class CJobFactory;
	friend class CJobQueue;
	friend class CScheduler;

public:
	// job type
	enum EJobType {
		EjtTest = 0,
		EjtGroupOptimization,
		EjtGroupImplementation,
		EjtGroupExploration,
		EjtGroupExpressionOptimization,
		EjtGroupExpressionImplementation,
		EjtGroupExpressionExploration,
		EjtTransformation,
		EjtInvalid,
		EjtSentinel = EjtInvalid
	};

public:
	CJob()
		: m_parent_jobs(nullptr),
		  m_job_queue(nullptr),
		  m_reference_cnt(0),
		  m_id(0),
		  m_is_initialized(false) {
	}

	CJob(const CJob &) = delete;
	
	virtual ~CJob() = default;

	// parent job
	CJob *m_parent_jobs;

	// assigned job queue
	CJobQueue *m_job_queue;

	// reference counter
	ULONG_PTR m_reference_cnt;

	// job id - set by job factory
	ULONG m_id;

	// job type
	EJobType m_job_type;

	// flag indicating if job is initialized
	bool m_is_initialized;
	
	// link for job queueing
	SLink m_link_queue;

public:
	// notify parent of job completion;
	// return true if parent is runnable;
	bool FResumeParent() const;

	//-------------------------------------------------------------------
	// Interface for CJobFactory
	//-------------------------------------------------------------------
	// set type
	void SetJobType(EJobType ejt) {
		m_job_type = ejt;
	}

	//-------------------------------------------------------------------
	// Interface for CScheduler
	//-------------------------------------------------------------------
	// parent accessor
	CJob *PJobParent() const {
		return m_parent_jobs;
	}

	// set parent
	void SetParent(CJob *pj) {
		m_parent_jobs = pj;
	}

	// increment reference counter
	void IncRefs() {
		m_reference_cnt++;
	}

	// decrement reference counter
	ULONG_PTR DecrRefs() {
		return m_reference_cnt--;
	}

	// id accessor
	ULONG Id() const {
		return m_id;
	}
	
	// check if job is initialized
	bool FInit() const {
		return m_is_initialized;
	}

	// mark job as initialized
	void SetInit() {
		m_is_initialized = true;
	}

	// type accessor
	EJobType JobType() const {
		return m_job_type;
	}

	// job queue accessor
	CJobQueue *JobQueue() const {
		return m_job_queue;
	}

	// set job queue
	void SetJobQueue(CJobQueue *pjq) {
		m_job_queue = pjq;
	}

public:
	// reset job
	virtual void Reset();

	// actual job execution given a scheduling context
	// returns true if job completes, false if it is suspended
	virtual bool
	FExecute(duckdb::unique_ptr<CSchedulerContext> psc) {
		return true;
	}

	// cleanup internal state
	virtual void Cleanup() {};
}; // class CJob
} // namespace gpopt