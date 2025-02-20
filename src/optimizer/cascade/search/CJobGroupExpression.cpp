//---------------------------------------------------------------------------
//	@filename:
//		CJobGroupExpression.cpp
//
//	@doc:
//		Implementation of group expression job superclass
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroupExpression.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpression::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void gpopt::CJobGroupExpression::Init(CGroupExpression* pgexpr)
{
	m_children_scheduled = false;
	m_xforms_scheduled = false;
	m_group_expression = pgexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExpression::ScheduleTransformations
//
//	@doc:
//		Schedule transformation jobs for the given set of xforms
//
//---------------------------------------------------------------------------
void gpopt::CJobGroupExpression::ScheduleTransformations(CSchedulerContext* psc, CXform_set * xform_set)
{
	// iterate on xforms
	for(size_t i = 0; i < CXform::EXformId::ExfSentinel; i++)
	{
		if (xform_set->test(i))
		{
			CXform* pxform = CXformFactory::XformFactory()->Xform(static_cast<CXform::EXformId>(i));
			CJobTransformation::ScheduleJob(psc, m_group_expression, pxform, this);
		}
	}
}