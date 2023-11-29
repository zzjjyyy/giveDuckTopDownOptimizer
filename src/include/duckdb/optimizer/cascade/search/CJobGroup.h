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
	duckdb::unique_ptr<CGroup> m_pgroup;

	// last scheduled group expression
	list<duckdb::unique_ptr<CGroupExpression>>::iterator m_last_scheduled_expr;

	// ctor
	CJobGroup() : m_pgroup(NULL) {
	}

	// no copy ctor
	CJobGroup(const CJobGroup &) = delete;

	// dtor
	virtual ~CJobGroup() {};

	// initialize job
	void Init(duckdb::unique_ptr<CGroup> pgroup);

	// get first unscheduled logical expression
	virtual list<duckdb::unique_ptr<CGroupExpression>>::iterator
	PgexprFirstUnschedLogical();

	// get first unscheduled non-logical expression
	virtual list<duckdb::unique_ptr<CGroupExpression>>::iterator
	PgexprFirstUnschedNonLogical();

	// get first unscheduled expression
	virtual list<duckdb::unique_ptr<CGroupExpression>>::iterator
	PgexprFirstUnsched() = 0;

	// schedule jobs for of all new group expressions
	virtual bool
	FScheduleGroupExpressions(duckdb::unique_ptr<CSchedulerContext> psc) = 0;

	// job's function
	bool
	FExecute(duckdb::unique_ptr<CSchedulerContext> psc) override = 0;

public:
	static void PrintJob(CJobGroup* job, std::string info) {
		auto group = job->m_pgroup;
		auto expr = group->m_group_exprs.front();
		auto op = expr->m_operator;
		std::string op_names;
		if(op->logical_type == LogicalOperatorType::LOGICAL_INVALID) {
			op_names = "Physical Type: " + PhysicalOperatorToString(op->physical_type);
		}
		if(op->physical_type == PhysicalOperatorType::INVALID) {
			op_names = "Logical Type: " + LogicalOperatorToString(op->logical_type);
		}
		size_t group_id = group->m_id;
		duckdb::Printer::Print(info + " " + op_names + "\tGroup Id " + std::to_string(group_id));
	}
}; // class CJobGroup
} // namespace gpopt
#endif