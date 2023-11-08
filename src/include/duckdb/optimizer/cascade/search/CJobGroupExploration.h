//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExploration.h
//
//	@doc:
//		Group exploration job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupExploration_H
#define GPOPT_CJobGroupExploration_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

extern clock_t start_exploration;

extern clock_t end_exploration;

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExploration
//
//	@doc:
//		Group exploration job
//
//		Responsible for creating the logical rewrites of all expressions in a
//		given group. This happens by firing exploration transformations that
//		perform logical rewriting (e.g., rewriting InnerJoin(A,B) as
//		InnerJoin(B,A))
//
//---------------------------------------------------------------------------
class CJobGroupExploration : public CJobGroup {
public:
	// transition events of group exploration
	enum EEvent { eevStartedExploration, eevNewChildren, eevExplored, eevSentinel };

	// states of group exploration job
	enum EState { estInitialized = 0, estExploringChildren, estCompleted, estSentinel };

public:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

public:
	// ctor
	CJobGroupExploration();

	// no copy ctor
	CJobGroupExploration(const CJobGroupExploration &) = delete;

	// dtor
	~CJobGroupExploration();

	// initialize job
	void Init(duckdb::unique_ptr<CGroup> pgroup);

	// get first unscheduled expression
	virtual list<duckdb::unique_ptr<CGroupExpression>>::iterator PgexprFirstUnsched() override {
		return CJobGroup::PgexprFirstUnschedLogical();
	}

	// schedule exploration jobs for of all new group expressions
	virtual bool
	FScheduleGroupExpressions(duckdb::unique_ptr<CSchedulerContext> psc);

	// schedule a new group exploration job
	static void
	ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
				duckdb::unique_ptr<CGroup> pgroup,
				CJob *pjParent);

	// job's function
	bool
	FExecute(duckdb::unique_ptr<CSchedulerContext> psc);

	// conversion function
	static CJobGroupExploration*
	ConvertJob(CJob *pj) {
		return dynamic_cast< CJobGroupExploration*>(pj);
	}

	// start exploration action
	static EEvent
	EevtStartExploration(duckdb::unique_ptr<CSchedulerContext> psc,
						 CJob *pj);

	// explore child group expressions action
	static EEvent
	EevtExploreChildren(duckdb::unique_ptr<CSchedulerContext> psc,
						CJob *pj);
}; // class CJobGroupExploration
} // namespace gpopt
#endif