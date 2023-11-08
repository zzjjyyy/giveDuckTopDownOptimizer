//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpression.h
//
//	@doc:
//		Superclass of group expression jobs
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

namespace gpopt {
// prototypes
class CGroup;
class CGroupExpression;

//---------------------------------------------------------------------------
//	@class:
//		CJobGroupExpression
//
//	@doc:
//		Abstract superclass of all group expression optimization jobs
//
//---------------------------------------------------------------------------
class CJobGroupExpression : public gpopt::CJob {

public:
	CJobGroupExpression() : m_group_expression(nullptr) {
	}

	CJobGroupExpression(const CJobGroupExpression &) = delete;
	
	~CJobGroupExpression() override {
	}

	// true if job has scheduled child group jobs
	bool m_children_scheduled;

	// true if job has scheduled transformation jobs
	bool m_xforms_scheduled;

	// target group expression
	duckdb::unique_ptr<CGroupExpression> m_group_expression;

public:
	// has job scheduled child groups ?
	bool FChildrenScheduled() const {
		return m_children_scheduled;
	}

	// set children scheduled
	void SetChildrenScheduled() {
		m_children_scheduled = true;
	}

	// has job scheduled xform groups ?
	bool FXformsScheduled() const {
		return m_xforms_scheduled;
	}

	// set xforms scheduled
	void SetXformsScheduled() {
		m_xforms_scheduled = true;
	}

	virtual void
	Init(duckdb::unique_ptr<CGroupExpression> pgexpr);

	// schedule transformation jobs for applicable xforms
	virtual void
	ScheduleApplicableTransformations(duckdb::unique_ptr<CSchedulerContext> psc) = 0;

	// schedule jobs for all child groups
	virtual void
	ScheduleChildGroupsJobs(duckdb::unique_ptr<CSchedulerContext> psc) = 0;

	// schedule transformation jobs for the given set of xforms
	void ScheduleTransformations(duckdb::unique_ptr<CSchedulerContext> psc,
								 duckdb::unique_ptr<CXform_set> xform_set);

	// job's function
	bool
	FExecute(duckdb::unique_ptr<CSchedulerContext> psc) override = 0;

	static void PrintJob(CJobGroupExpression *job, std::string info) {
		auto group = job->m_group_expression->m_group;
		auto expr = group->m_group_exprs.front();
		auto op = expr->m_operator;

		std::string op_names = "Logical Type: " + LogicalOperatorToString(op->logical_type);
		size_t group_id = group->m_id;

		duckdb::Printer::Print(info + " " + op_names + "\tGroup Id " + std::to_string(group_id));
	}
}; // class CJobGroupExpression
} // namespace gpopt