//---------------------------------------------------------------------------
//	@filename:
//		CXformOrderImplementation.h
//
//	@doc:
//		Transform Logical Projection to Physical Projection
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformOrderImplementation_H
#define GPOPT_CXformOrderImplementation_H

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
class CXformOrderImplementation : public CXformImplementation {
public:
	explicit CXformOrderImplementation();
	
	CXformOrderImplementation(const CXformOrderImplementation &) = delete;

	~CXformOrderImplementation() override = default;

	// ident accessors
	EXformId ID() const override {
		return ExfOrderImplementation;
	}

	// return a string for xform name
	const CHAR *Name() const override {
		return "CXformOrderImplementation";
	}

	// compute xform promise for a given expression handle
	EXformPromise
	XformPromise(CExpressionHandle &handle) const override;

	// actual transform
	void Transform(duckdb::unique_ptr<CXformContext> pxfctxt,
				   duckdb::unique_ptr<CXformResult> pxfres,
				   duckdb::unique_ptr<Operator> pexpr) const override;
};
} // namespace gpopt
#endif