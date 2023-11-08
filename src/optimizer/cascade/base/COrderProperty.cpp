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
#include "duckdb/optimizer/cascade/base/CRequiredPhysicalProp.h"

namespace gpopt {
using namespace duckdb;
using namespace gpos;

// initialization of static variables
const CHAR *COrderProperty::m_szOrderMatching[EomSentinel] = {"satisfy"};

//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::COrderProperty
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderProperty::COrderProperty(duckdb::unique_ptr<COrderSpec> pos, EOrderMatching eom)
	: m_order_spec(pos), m_order_match_type(eom) {
}

//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::COrderPropertyerty
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderProperty::~COrderProperty() {
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
bool COrderProperty::FCompatible(duckdb::unique_ptr<COrderSpec> pos) const {
	switch (m_order_match_type) {
	case EomSatisfy:
		return pos->FSatisfies(m_order_spec);
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
size_t COrderProperty::HashValue() const {
	return gpos::CombineHashes(m_order_match_type + 1, m_order_spec->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		COrderProperty::EorderEnforcingType
//
//	@doc:
// 		Get order enforcing type for the given operator
//
//---------------------------------------------------------------------------
COrderProperty::EPropEnforcingType
COrderProperty::EorderEnforcingType(CExpressionHandle &exprhdl,
								    duckdb::unique_ptr<PhysicalOperator> popPhysical,
									bool fOrderReqd) const {
	if (fOrderReqd) {
		return popPhysical->EnforcingTypeOrder(exprhdl, this->m_order_spec->order_nodes);
	}
	return EpetUnnecessary;
}
} // namespace gpopt