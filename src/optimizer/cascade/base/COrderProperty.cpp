//---------------------------------------------------------------------------
//	@filename:
//		COrderProperty.cpp
//
//	@doc:
//		Implementation of enforceable order property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/COrderProperty.h"

#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropPlan.h"

namespace gpopt
{
using namespace duckdb;
using namespace gpos;

// initialization of static variables
const CHAR*COrderProperty::m_szOrderMatching[EomSentinel] = {"satisfy"};

//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::COrderProperty
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderProperty::COrderProperty(COrderSpec* pos, EOrderMatching eom)
	: m_pos(pos), m_eom(eom)
{
}


//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::COrderPropertyerty
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderProperty::~COrderProperty()
{
}


//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::FCompatible
//
//	@doc:
//		Check if the given order specification is compatible with the
//		order specification of this object for the specified matching type
//
//---------------------------------------------------------------------------
bool COrderProperty::FCompatible(COrderSpec* pos) const
{
	switch (m_eom)
	{
		case EomSatisfy:
			return pos->FSatisfies(m_pos);
		case EomSentinel:
			break;
	}
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::HashValue
//
//	@doc:
// 		Hash function
//
//---------------------------------------------------------------------------
ULONG COrderProperty::HashValue() const
{
	return gpos::CombineHashes(m_eom + 1, m_pos->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::Epet
//
//	@doc:
// 		Get order enforcing type for the given operator
//
//---------------------------------------------------------------------------
COrderProperty::EPropEnforcingType COrderProperty::Epet(CExpressionHandle &exprhdl, PhysicalOperator* popPhysical, bool fOrderReqd) const
{
	if (fOrderReqd)
	{
		return popPhysical->EpetOrder(exprhdl, this->m_pos->m_pdrgpoe);
	}
	return EpetUnnecessary;
}
}