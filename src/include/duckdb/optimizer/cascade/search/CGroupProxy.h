//---------------------------------------------------------------------------
//	@filename:
//		CGroupProxy.h
//
//	@doc:
//		Lock mechanism for access to a given group
//---------------------------------------------------------------------------
#ifndef GPOPT_CGroupProxy_H
#define GPOPT_CGroupProxy_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"

namespace gpopt
{
using namespace gpos;

// forward declarations
class CGroupExpression;
class CDerivedProperty;
class COptimizationContext;

//---------------------------------------------------------------------------
//	@class:
//		CGroupProxy
//
//	@doc:
//		Exclusive access to a given group
//
//---------------------------------------------------------------------------
class CGroupProxy
{
public:
	// group we're operating on
	duckdb::unique_ptr<CGroup> m_pgroup;

public:
	// ctor
	explicit CGroupProxy(duckdb::unique_ptr<CGroup> pgroup);

	// dtor
	~CGroupProxy();

public:
	// set group id
	void SetId(ULONG id)
	{
		m_pgroup->SetId(id);
	}

	// set group state
	void SetState(CGroup::EState estNewState)
	{
		m_pgroup->SetState(estNewState);
	}

	// skip group expressions starting from the given expression;
	list<duckdb::unique_ptr<CGroupExpression>>::iterator
	PgexprSkip(list<duckdb::unique_ptr<CGroupExpression>>::iterator pgexprStart, bool fSkipLogical);

	// insert group expression
	void Insert(duckdb::unique_ptr<CGroupExpression> pgexpr);

	// move duplicate group expression to duplicates list
	void MoveDuplicateGExpr(duckdb::unique_ptr<CGroupExpression> pgexpr);

	// initialize group's properties;
	void InitProperties(duckdb::unique_ptr<CDerivedProperty> ppdp);

	// retrieve first group expression
	list<duckdb::unique_ptr<CGroupExpression>>::iterator PgexprFirst();

	// get the first non-logical group expression following the given expression
	list<duckdb::unique_ptr<CGroupExpression>>::iterator PgexprSkipLogical(list<duckdb::unique_ptr<CGroupExpression>>::iterator pgexpr);

	// get the next logical group expression following the given expression
	list<duckdb::unique_ptr<CGroupExpression>>::iterator PgexprNextLogical(list<duckdb::unique_ptr<CGroupExpression>>::iterator pgexpr);

	// lookup best expression under optimization context
	duckdb::unique_ptr<CGroupExpression> PgexprLookup(duckdb::unique_ptr<COptimizationContext> poc) const;
};	// class CGroupProxy
}  // namespace gpopt
#endif