//---------------------------------------------------------------------------
//	@filename:
//		CXformPushGbBelowJoin.h
//
//	@doc:
//		Push group by below join transform
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushGbBelowJoin_H
#define GPOPT_CXformPushGbBelowJoin_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushGbBelowJoin
//
//	@doc:
//		Push group by below join transform
//
//---------------------------------------------------------------------------
class CXformPushGbBelowJoin : public CXformExploration
{
public:
	// ctor
	explicit CXformPushGbBelowJoin();

	// ctor
	explicit CXformPushGbBelowJoin(duckdb::unique_ptr<Operator> pexprPattern);

	// no copy ctor
	CXformPushGbBelowJoin(const CXformPushGbBelowJoin &) = delete;

	// dtor
	virtual ~CXformPushGbBelowJoin()
	{
	}

	// ident accessors
	EXformId ID() const override {
		return ExfPushGbBelowJoin;
	}

	const CHAR *Name() const override {
		return "CXformPushGbBelowJoin";
	}

	// compute xform promise for a given expression handle
	EXformPromise XformPromise(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(duckdb::unique_ptr<CXformContext> pxfctxt,
				   duckdb::unique_ptr<CXformResult> pxfres,
				   duckdb::unique_ptr<Operator> pexpr) const override;
};	// class CXformPushGbBelowJoin
}  // namespace gpopt
#endif	// !GPOPT_CXformPushGbBelowJoin_H