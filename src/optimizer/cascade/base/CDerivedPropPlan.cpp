//---------------------------------------------------------------------------
//	@filename:
//		CDerivedPropPlan.cpp
//
//	@doc:
//		Derived plan properties
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDerivedPropPlan.h"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COrderProperty.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropPlan.h"

namespace gpopt {
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPropPlan::CDerivedPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDerivedPropPlan::CDerivedPropPlan() : m_sort_order(NULL) {
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPropPlan::CDerivedPropPlanlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDerivedPropPlan::~CDerivedPropPlan() {
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPropPlan::Derive
//
//	@doc:
//		Derive plan props
//
//---------------------------------------------------------------------------
void CDerivedPropPlan::Derive(gpopt::CExpressionHandle &exprhdl, CDerivedPropertyContext *pdpctxt) {
	// call property derivation functions on the operator
	m_sort_order = ((PhysicalOperator *)exprhdl.Pop())->PosDerive(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPropPlan::DrvdPlanProperty
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDerivedPropPlan *CDerivedPropPlan::DrvdPlanProperty(CDerivedProperty *pdp) {
	return (CDerivedPropPlan *)pdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPropPlan::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL CDerivedPropPlan::FSatisfies(const CRequiredPropPlan *prop_plan) const {
	return m_sort_order->FSatisfies(prop_plan->m_sort_order->m_order_spec);
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPropPlan::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG CDerivedPropPlan::HashValue() const {
	ULONG ulHash = m_sort_order->HashValue();
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
ULONG CDerivedPropPlan::Equals(const CDerivedPropPlan *pdpplan) const {
	return m_sort_order->Matches(pdpplan->m_sort_order);
}
} // namespace gpopt