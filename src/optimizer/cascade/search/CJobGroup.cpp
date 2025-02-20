//---------------------------------------------------------------------------
//	@filename:
//		CJobGroup.cpp
//
//	@doc:
//		Implementation of group job superclass
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CJobGroup.h"

#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionExploration.h"
#include "duckdb/optimizer/cascade/search/CJobGroupExpressionImplementation.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CJobGroup::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void CJobGroup::Init(CGroup* pgroup)
{
	m_pgroup = pgroup;
	m_last_scheduled_expr = m_pgroup->m_group_exprs.end();
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroup::PgexprFirstUnschedNonLogical
//
//	@doc:
//		Get first non-logical group expression with an unscheduled job
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CJobGroup::PgexprFirstUnschedNonLogical()
{
	list<CGroupExpression*>::iterator itr;
	{
		CGroupProxy gp(m_pgroup);
		if (m_pgroup->m_group_exprs.end() == m_last_scheduled_expr)
		{
			// get first group expression
			itr = gp.PgexprSkipLogical(m_pgroup->m_group_exprs.begin());
		}
		else
		{
			itr = m_last_scheduled_expr;
			// get group expression next to last scheduled one
			itr = gp.PgexprSkipLogical(++itr);
		}
	}
	return itr;
}

//---------------------------------------------------------------------------
//	@function:
//		CJobGroup::PgexprFirstUnschedLogical
//
//	@doc:
//		Get first logical group expression with an unscheduled job
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CJobGroup::PgexprFirstUnschedLogical()
{
	list<CGroupExpression*>::iterator itr;
	{
		CGroupProxy gp(m_pgroup);
		if (m_pgroup->m_group_exprs.end() == m_last_scheduled_expr)
		{
			// get first group expression
			itr = m_pgroup->m_group_exprs.begin();
		}
		else
		{
			// get group expression next to last scheduled one
			itr = m_last_scheduled_expr;
			++itr;
		}
	}
	return itr;
}