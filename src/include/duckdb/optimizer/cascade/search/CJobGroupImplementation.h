//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupImplementation.h
//
//	@doc:
//		Implement group job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupImplementation_H
#define GPOPT_CJobGroupImplementation_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

using namespace gpos;

extern clock_t start_implementation;
	
extern clock_t end_implementation;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CJobGroupImplementation
//
//	@doc:
//		Group implementation job
//
//		Responsible for creating the physical implementations of all
//		expressions in a given group. This happens by firing implementation
//		transformations that perform physical implementation (e.g.,
//		implementing InnerJoin as HashJoin)
//
//---------------------------------------------------------------------------
class CJobGroupImplementation : public CJobGroup
{
public:
	// transition events of group implementation
	enum EEvent
	{ eevExploring, eevExplored, eevImplementing, eevImplemented, eevSentinel };

	// states of group implementation job
	enum EState
	{ estInitialized = 0, estImplementingChildren, estCompleted, estSentinel };

	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

public:
	// job state machine
	JSM m_jsm;

public:
	// ctor
	CJobGroupImplementation();
	
	// private copy ctor
	CJobGroupImplementation(const CJobGroupImplementation &) = delete;
	
	// dtor
	~CJobGroupImplementation();

public:
	// initialize job
	void Init(duckdb::unique_ptr<CGroup> pgroup);

	// get first unscheduled expression
	virtual list<duckdb::unique_ptr<CGroupExpression>>::iterator
	PgexprFirstUnsched() {
		return CJobGroup::PgexprFirstUnschedLogical();
	}

	// schedule implementation jobs for of all new group expressions
	virtual bool
	FScheduleGroupExpressions(duckdb::unique_ptr<CSchedulerContext> psc);

	// schedule a new group implementation job
	static void
	ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
				duckdb::unique_ptr<CGroup> pgroup,
				CJob *pjParent);

	// job's function
	bool
	FExecute(duckdb::unique_ptr<CSchedulerContext> psc) override;

	// conversion function
	static CJobGroupImplementation*
	ConvertJob(CJob *pj)
	{
		return dynamic_cast<CJobGroupImplementation*>(pj);
	}

	// start implementation action
	static EEvent
	EevtStartImplementation(duckdb::unique_ptr<CSchedulerContext> psc,
							CJob *pj);

	// implement child group expressions action
	static EEvent
	EevtImplementChildren(duckdb::unique_ptr<CSchedulerContext> psc,
						  CJob *pj);
};	// class CJobGroupImplementation
}  // namespace gpopt
#endif