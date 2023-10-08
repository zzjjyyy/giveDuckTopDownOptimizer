//---------------------------------------------------------------------------
//	@filename:
//		CXformLogicalAggregateImplementation.h
//
//	@doc:
//		Implementation of Logical Aggregate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLogicalAggregateImplementation_H
#define GPOPT_CXformLogicalAggregateImplementation_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformImplementation.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGet2TableScan
//
//	@doc:
//		Transform Get to TableScan
//
//---------------------------------------------------------------------------
class CXformLogicalAggregateImplementation : public CXformImplementation {
public:
	// ctor
	explicit CXformLogicalAggregateImplementation();

	CXformLogicalAggregateImplementation(const CXformLogicalAggregateImplementation &) = delete;

	// dtor
	~CXformLogicalAggregateImplementation() override = default;
	// ident accessors
	EXformId ID() const override {
		return ExfLogicalAggregateImplementation;
	}
	// return a string for xform name
	const CHAR *Name() const override {
		return "CXformLogicalAggregateImplementation";
	}
	// compute xform promise for a given expression handle
	EXformPromise XformPromise(CExpressionHandle &expression_handle) const override;
	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres, Operator *pexpr) const override;
};
} // namespace gpopt
#endif