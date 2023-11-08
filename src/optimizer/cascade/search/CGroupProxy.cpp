//---------------------------------------------------------------------------
//	@filename:
//		CGroupProxy.cpp
//
//	@doc:
//		Implementation of proxy object for group access
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::CGroupProxy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroupProxy::CGroupProxy(duckdb::unique_ptr<CGroup> pgroup)
	: m_pgroup(pgroup)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::~CGroupProxy
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroupProxy::~CGroupProxy()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::Insert
//
//	@doc:
//		Insert group expression into group
//
//---------------------------------------------------------------------------
void CGroupProxy::Insert(duckdb::unique_ptr<CGroupExpression> pgexpr)
{
	pgexpr->Init(m_pgroup, m_pgroup->m_num_exprs++);
	m_pgroup->Insert(pgexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::MoveDuplicateGExpr
//
//	@doc:
//		Move duplicate group expression to duplicates list
//
//---------------------------------------------------------------------------
void CGroupProxy::MoveDuplicateGExpr(duckdb::unique_ptr<CGroupExpression> pgexpr)
{
	m_pgroup->MoveDuplicateGExpr(pgexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::InitProperties
//
//	@doc:
//		Initialize group's properties
//
//---------------------------------------------------------------------------
void CGroupProxy::InitProperties(duckdb::unique_ptr<CDerivedProperty> pdp)
{
	m_pgroup->InitProperties(pdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::FirstGroupExpr
//
//	@doc:
//		Retrieve first group expression iterator;
//
//---------------------------------------------------------------------------
list<duckdb::unique_ptr<CGroupExpression>>::iterator CGroupProxy::PgexprFirst()
{
	return m_pgroup->FirstGroupExpr();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprSkip
//
//	@doc:
//		Skip group expressions starting from the given expression;
//		the type of group expressions to skip is determined by the passed
//		flag
//
//---------------------------------------------------------------------------
list<duckdb::unique_ptr<CGroupExpression>>::iterator
CGroupProxy::PgexprSkip(list<duckdb::unique_ptr<CGroupExpression>>::iterator pgexprStart, bool fSkipLogical)
{
	auto iter = pgexprStart;
	while (m_pgroup->m_group_exprs.end() != iter && fSkipLogical == (*iter)->m_operator->FLogical())
	{
		++iter;
	}
	return iter;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprSkipLogical
//
//	@doc:
//		Retrieve the first non-logical group expression including the given
//		expression;
//
//---------------------------------------------------------------------------
list<duckdb::unique_ptr<CGroupExpression>>::iterator
CGroupProxy::PgexprSkipLogical(list<duckdb::unique_ptr<CGroupExpression>>::iterator pgexpr)
{
	return PgexprSkip(pgexpr, true);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprNextLogical
//
//	@doc:
//		Find the first logical group expression including the given expression
//
//---------------------------------------------------------------------------
list<duckdb::unique_ptr<CGroupExpression>>::iterator
CGroupProxy::PgexprNextLogical(list<duckdb::unique_ptr<CGroupExpression>>::iterator pgexpr)
{
	return PgexprSkip(pgexpr, false);
}