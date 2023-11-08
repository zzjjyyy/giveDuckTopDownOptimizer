//---------------------------------------------------------------------------
//	@filename:
//		CBinding.h
//
//	@doc:
//		Binding mechanism to extract expression from Memo according to pattern
//---------------------------------------------------------------------------
#ifndef GPOPT_CBinding_H
#define GPOPT_CBinding_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include <memory>
#include <list>

using namespace gpos;
using namespace duckdb;
using namespace std;

namespace gpopt
{
// fwd declaration
class CGroupExpression;
class CGroup;

//---------------------------------------------------------------------------
//	@class:
//		CBinding
//
//	@doc:
//		Binding class used to iteratively generate expressions from the
//		memo so that they match a given pattern
//
//---------------------------------------------------------------------------
class CBinding
{
public:
	// initialize cursors of child expressions
	bool FInitChildCursors(duckdb::unique_ptr<CGroupExpression> pgexpr,
						   duckdb::unique_ptr<Operator> pexprPattern,
						   duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr);

	// advance cursors of child expressions
	bool FAdvanceChildCursors(duckdb::unique_ptr<CGroupExpression> pgexpr,
							  duckdb::unique_ptr<Operator> pexprPattern,
							  duckdb::unique_ptr<Operator> pexprLast,
							  duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr);

	// move cursor
	list<duckdb::unique_ptr<CGroupExpression>>::iterator
	PgexprNext(duckdb::unique_ptr<CGroup> pgroup,
			   duckdb::unique_ptr<CGroupExpression> pgexpr) const;

	// expand n-th child of pattern
	duckdb::unique_ptr<Operator>
	PexprExpandPattern(duckdb::unique_ptr<Operator> pexpr,
					   ULONG ulPos, ULONG arity);

	// get binding for children
	bool FExtractChildren(duckdb::unique_ptr<CGroupExpression> pgexpr,
						  duckdb::unique_ptr<Operator> pexprPattern,
						  duckdb::unique_ptr<Operator>pexprLast,
						  duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexprChildren);

	// extract binding from a group
	// Need to delete
	// Operator* PexprExtract(CGroup* pgroup, Operator* pexprPattern, Operator* pexprLast);
	duckdb::unique_ptr<Operator>
	PexprExtract(duckdb::unique_ptr<CGroup> pgroup,
				 duckdb::unique_ptr<Operator> pexprPattern,
				 duckdb::unique_ptr<Operator> pexprLast);
	
	// extract binding from group expression
	// Need to delete
	// Operator* PexprExtract(CGroupExpression* pgexpr, Operator* pexprPatetrn, Operator* pexprLast);
	duckdb::unique_ptr<Operator>
	PexprExtract(duckdb::unique_ptr<CGroupExpression> pgexpr,
				 duckdb::unique_ptr<Operator> pexprPattern,
				 duckdb::unique_ptr<Operator> pexprLast);

	// build expression
	// Need to delete
	// Operator* PexprFinalize(CGroupExpression* pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexprChildren);
	duckdb::unique_ptr<Operator>
	PexprFinalize(duckdb::unique_ptr<CGroupExpression> pgexpr,
				  duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr);

public:
	// ctor
	CBinding()
	{
	}
	
	// no copy ctor
	CBinding(const CBinding &) = delete;
	
	// dtor
	~CBinding()
	{
	}
};	// class CBinding
}  // namespace gpopt
#endif