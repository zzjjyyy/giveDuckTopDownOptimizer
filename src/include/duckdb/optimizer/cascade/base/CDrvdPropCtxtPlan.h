//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtPlan.h
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxtPlan_H
#define GPOPT_CDrvdPropCtxtPlan_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropPlan.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropertyContext.h"

using namespace gpos;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxtPlan
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
class CDrvdPropCtxtPlan : public CDerivedPropertyContext {
public:
	// ctor
	CDrvdPropCtxtPlan(bool fUpdateCTEMap = true);

	// no copy ctor
	CDrvdPropCtxtPlan(const CDrvdPropCtxtPlan &) = delete;

	// dtor
	~CDrvdPropCtxtPlan() override;

public:
	// copy function
	duckdb::unique_ptr<CDerivedPropertyContext> PdpctxtCopy() const override;

	// add props to context
	void AddProps(duckdb::unique_ptr<CDerivedProperty> pdp) override;

public:
	// conversion function
	static CDrvdPropCtxtPlan* PdpctxtplanConvert(CDerivedPropertyContext *pdpctxt)
	{
		return reinterpret_cast<CDrvdPropCtxtPlan*>(pdpctxt);
	}
};	// class CDrvdPropCtxtPlan
}  // namespace gpopt
#endif