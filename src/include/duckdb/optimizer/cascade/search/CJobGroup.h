//---------------------------------------------------------------------------
//	@filename:
//		CJobGroup.h
//
//	@doc:
//		Superclass of group jobs
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobGroup_H
#define GPOPT_CJobGroup_H

#include "duckdb/common/printer.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJob.h"

using namespace gpos;

namespace gpopt {
// prototypes
class CGroup;
class CGroupExpression;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroup
//
//	@doc:
//		Abstract superclass of all group optimization jobs
//
//---------------------------------------------------------------------------
class CJobGroup : public CJob {
public:
	// target group
	CGroup *m_pgroup;

	// last scheduled group expression
	list<CGroupExpression *>::iterator m_last_scheduled_expr;

	// ctor
	CJobGroup() : m_pgroup(NULL) {
	}

	// no copy ctor
	CJobGroup(const CJobGroup &) = delete;

	// dtor
	virtual ~CJobGroup() {};

	// initialize job
	void Init(CGroup *pgroup);

	// get first unscheduled logical expression
	virtual list<CGroupExpression *>::iterator PgexprFirstUnschedLogical();

	// get first unscheduled non-logical expression
	virtual list<CGroupExpression *>::iterator PgexprFirstUnschedNonLogical();

	// get first unscheduled expression
	virtual list<CGroupExpression *>::iterator PgexprFirstUnsched() = 0;

	// schedule jobs for of all new group expressions
	virtual bool FScheduleGroupExpressions(CSchedulerContext *psc) = 0;

	// job's function
	bool FExecute(CSchedulerContext *psc) override = 0;

public:
	static void PrintJob(CJobGroup *job, std::string info) {
		CGroup *group = job->m_pgroup;
		CGroupExpression *expr = group->m_group_exprs.front();
		Operator *op = expr->m_operator.get();

		std::string op_names = "Logical Type: " + LogicalOperatorToString(op->logical_type);
		size_t group_id = group->m_id;

		duckdb::Printer::Print(info + " " + op_names + "\tGroup Id " + std::to_string(group_id));
	}
}; // class CJobGroup
} // namespace gpopt
#endif