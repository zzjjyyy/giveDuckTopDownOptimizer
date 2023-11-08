//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpressionOptimization.h
//
//	@doc:
//		Explore group expression job
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobStateMachine.h"

namespace gpopt {
using namespace gpos;

// prototypes
class CCostContext;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExpressionOptimization
//
//	@doc:
//		Group expression optimization job
//
//		Responsible for finding the best plan rooted by a given group
//		expression, such that the identified plan satisfies given required plan
//		properties. Note that a group optimization job entails running a group
//		expression optimization job for each group expression in the underlying
//		group.
//
//---------------------------------------------------------------------------
class CJobGroupExpressionOptimization : public CJobGroupExpression {
public:
	// transition events of group expression optimization
	enum EEvent {
		eevOptimizingChildren,
		eevChildrenOptimized,
		eevCheckingEnfdProps,
		eevOptimizingSelf,
		eevSelfOptimized,
		eevFinalized,
		eevSentinel
	};

	// states of group expression optimization
	enum EState {
		estInitialized = 0,
		estOptimizingChildren,
		estChildrenOptimized,
		estEnfdPropsChecked,
		estSelfOptimized,
		estCompleted,
		estSentinel
	};

	// shorthand for job state machine
	typedef CJobStateMachine<EState, estSentinel, EEvent, eevSentinel> JSM;

public:
	CJobGroupExpressionOptimization();

	~CJobGroupExpressionOptimization() override;

	CJobGroupExpressionOptimization(const CJobGroupExpressionOptimization &) = delete;

	// job state machine
	JSM m_job_state_machine;

	// optimization context of the job
	duckdb::unique_ptr<COptimizationContext> m_opt_context;

	// optimization request number
	ULONG m_opt_request_num;

	// array of child groups optimization contexts
	duckdb::vector<duckdb::unique_ptr<COptimizationContext>> m_children_opt_contexts;

	// array of derived properties of optimal implementations of child groups
	duckdb::vector<duckdb::unique_ptr<CDerivedProperty>> m_children_derived_properties;

	// counter of next child group to be optimized
	ULONG m_children_index;

	// number of children
	ULONG m_arity;

	// flag to indicate if optimizing a child has failed
	bool m_child_optimization_failed;

	// flag to indicate if current job optimizes a Sequence operator that captures a CTE
	bool m_optimize_CTE_sequence;

	// a handle object for required plan properties computation
	duckdb::unique_ptr<CExpressionHandle> m_plan_properties_handler;

	// a handle object for required relational property computation
	duckdb::unique_ptr<CExpressionHandle> m_relation_properties_handler;

public:
	// job's function
	bool
	FExecute(duckdb::unique_ptr<CSchedulerContext> psc) override;

	// initialize job
	void Init(duckdb::unique_ptr<CGroupExpression> pgexpr,
			  duckdb::unique_ptr<COptimizationContext> poc,
			  ULONG opt_request_num);

	// initialization routine for child groups optimization
	void InitChildGroupsOptimization(duckdb::unique_ptr<CSchedulerContext> psc);

	// derive plan properties and stats of the child previous to the one being optimized
	void DerivePrevChildProps(duckdb::unique_ptr<CSchedulerContext> scheduler_context);

	// compute required plan properties for current child
	void ComputeCurrentChildRequirements(duckdb::unique_ptr<CSchedulerContext> psc);

	// initialize action
	static EEvent
	EevtInitialize(duckdb::unique_ptr<CSchedulerContext> scheduler_context,
				   CJob *pjOwner);

	// optimize child groups action
	static EEvent
	EevtOptimizeChildren(duckdb::unique_ptr<CSchedulerContext> psc,
						 CJob *pj);

	// add enforcers to the owning group
	static EEvent
	EevtAddEnforcers(duckdb::unique_ptr<CSchedulerContext> psc,
					 CJob *pj);

	// optimize group expression action
	static EEvent
	EevtOptimizeSelf(duckdb::unique_ptr<CSchedulerContext> psc,
					 CJob *pjOwner);

	// finalize action
	static EEvent
	EevtFinalize(duckdb::unique_ptr<CSchedulerContext> psc,
				 CJob *pj);

	// schedule a new group expression optimization job
	static void ScheduleJob(duckdb::unique_ptr<CSchedulerContext> psc,
							duckdb::unique_ptr<CGroupExpression> pgexpr,
							duckdb::unique_ptr<COptimizationContext> poc,
							ULONG ulOptReq,
	                        CJob *job_parent);

	// schedule transformation jobs for applicable xforms
	virtual void ScheduleApplicableTransformations(duckdb::unique_ptr<CSchedulerContext> psc) {};

	// schedule optimization jobs for all child groups
	virtual void
	ScheduleChildGroupsJobs(duckdb::unique_ptr<CSchedulerContext> psc);

	// cleanup internal state
	virtual void Cleanup();

	// conversion function
	static CJobGroupExpressionOptimization*
	ConvertJob(CJob *pj) {
		return dynamic_cast<CJobGroupExpressionOptimization*>(pj);
	}
}; // class CJobGroupExpressionOptimization
} // namespace gpopt