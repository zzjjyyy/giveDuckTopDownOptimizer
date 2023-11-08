//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionExploration.h
//
//	@doc:
//		Explore group expression job
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroupExpressionExploration_H
#define GPOPT_CJobGroupExpressionExploration_H

#include "duckdb/optimizer/cascade/search/CJobGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExpressionExploration
//
//	@doc:
//		Explore group expression optimization job
//
//		Responsible for creating the logical rewrites of a given group
//		expression. Note that a group exploration job entails running a group
//		expression exploration job for each group expression in the underlying
//		group.
//
//---------------------------------------------------------------------------
class CJobGroupExpressionExploration : public CJobGroupExpression
{
public:
	// transition events of group expression exploration
	enum EEvent
	{ eevExploringChildren, eevChildrenExplored, eevExploringSelf, eevSelfExplored, eevFinalized, eevSentinel };

	// states of group expression exploration
	enum EState
	{ estInitialized = 0, estChildrenExplored, estSelfExplored, estCompleted, estSentinel };

public:
	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

	// job state machine
	JSM m_jsm;

public:
	// schedule transformation jobs for applicable xforms
	virtual void
	ScheduleApplicableTransformations(duckdb::unique_ptr<CSchedulerContext> psc);

	// schedule exploration jobs for all child groups
	virtual void ScheduleChildGroupsJobs(duckdb::unique_ptr<CSchedulerContext> psc);

	// explore child groups action
	static EEvent
	EevtExploreChildren(duckdb::unique_ptr<CSchedulerContext> psc,
						CJob *pj);

	// explore group expression action
	static EEvent
	EevtExploreSelf(duckdb::unique_ptr<CSchedulerContext> psc,
					CJob *pj);

	// finalize action
	static EEvent
	EevtFinalize(duckdb::unique_ptr<CSchedulerContext> psc,
				 CJob *pj);

public:
	// ctor
	CJobGroupExpressionExploration();
	
	// no copy ctor
	CJobGroupExpressionExploration(const CJobGroupExpressionExploration &) = delete;
	
	// dtor
	virtual ~CJobGroupExpressionExploration();

	// initialize job
	void Init(duckdb::unique_ptr<CGroupExpression> pgexpr);

	// schedule a new group expression exploration job
	static void
	ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
				duckdb::unique_ptr<CGroupExpression> pgexpr,
				CJob *pjParent);

	// job's main function
	bool
	FExecute(duckdb::unique_ptr<CSchedulerContext> psc) override;

	// conversion function
	static CJobGroupExpressionExploration*
	PjConvert(CJob *pj)
	{
		return dynamic_cast<CJobGroupExpressionExploration*>(pj);
	}
};	// class CJobGroupExpressionExploration
}  // namespace gpopt
#endif