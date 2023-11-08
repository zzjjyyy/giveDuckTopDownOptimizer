//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtPlan.cpp
//
//	@doc:
//		Derived plan properties context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropPlan.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::CDrvdPropCtxtPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropCtxtPlan::CDrvdPropCtxtPlan(BOOL fUpdateCTEMap)
	: CDerivedPropertyContext()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::~CDrvdPropCtxtPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropCtxtPlan::~CDrvdPropCtxtPlan()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::PdpctxtCopy
//
//	@doc:
//		Copy function
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CDerivedPropertyContext> CDrvdPropCtxtPlan::PdpctxtCopy() const
{
	duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan = make_uniq<CDrvdPropCtxtPlan>();
	return pdpctxtplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::AddProps
//
//	@doc:
//		Add props to context
//
//---------------------------------------------------------------------------
void CDrvdPropCtxtPlan::AddProps(duckdb::unique_ptr<CDerivedProperty> pdp)
{
	return;
}