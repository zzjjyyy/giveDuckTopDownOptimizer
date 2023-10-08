//---------------------------------------------------------------------------
//	@filename:
//		CDerivedPhysicalProp.cpp
//
//	@doc:
//		Derived plan properties
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDerivedPropPlan.h"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COrderProperty.h"
#include "duckdb/optimizer/cascade/base/CRequiredPhysicalProp.h"

namespace gpopt {
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPhysicalProp::CDerivedPhysicalProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDerivedPhysicalProp::CDerivedPhysicalProp() : m_sort_order(NULL) {
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPhysicalProp::CDerivedPropPlanlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDerivedPhysicalProp::~CDerivedPhysicalProp() {
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPhysicalProp::Derive
//
//	@doc:
//		Derive plan props
//
//---------------------------------------------------------------------------
void CDerivedPhysicalProp::Derive(gpopt::CExpressionHandle &exprhdl, CDerivedPropertyContext *pdpctxt) {
	// call property derivation functions on the operator
	m_sort_order = ((PhysicalOperator *)exprhdl.Pop())->PosDerive(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPhysicalProp::DrvdPlanProperty
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDerivedPhysicalProp *CDerivedPhysicalProp::DrvdPlanProperty(CDerivedProperty *pdp) {
	return (CDerivedPhysicalProp *)pdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPhysicalProp::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL CDerivedPhysicalProp::FSatisfies(const CRequiredPhysicalProp *prop_plan) const {
	return m_sort_order->FSatisfies(prop_plan->m_sort_order->m_order_spec);
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPhysicalProp::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
size_t CDerivedPhysicalProp::HashValue() const {
	size_t ulHash = m_sort_order->HashValue();
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedPhysicalProp::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
ULONG CDerivedPhysicalProp::Equals(const CDerivedPhysicalProp *pdpplan) const {
	return m_sort_order->Matches(pdpplan->m_sort_order);
}
} // namespace gpopt