//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropPlan.cpp
//
//	@doc:
//		Derived plan properties
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COrderProperty.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropPlan.h"

namespace gpopt
{
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CDrvdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::CDrvdPropPlan()
	: m_pos(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::~CDrvdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::~CDrvdPropPlan()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Derive
//
//	@doc:
//		Derive plan props
//
//---------------------------------------------------------------------------
void CDrvdPropPlan::Derive(gpopt::CExpressionHandle& exprhdl, CDrvdPropCtxt* pdpctxt)
{
	// call property derivation functions on the operator
	m_pos = ((PhysicalOperator*)exprhdl.Pop())->PosDerive(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Pdpplan
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropPlan* CDrvdPropPlan::Pdpplan(CDrvdProp* pdp)
{
	return (CDrvdPropPlan*)pdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL CDrvdPropPlan::FSatisfies(const CRequiredPropPlan *prpp) const
{
	return m_pos->FSatisfies(prpp->m_required_sort_order->m_pos);
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG CDrvdPropPlan::HashValue() const
{
	ULONG ulHash = m_pos->HashValue();
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
ULONG CDrvdPropPlan::Equals(const CDrvdPropPlan *pdpplan) const
{
	return m_pos->Matches(pdpplan->m_pos);
}
}