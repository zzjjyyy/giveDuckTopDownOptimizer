//---------------------------------------------------------------------------
//	@filename:
//		COrderProperty.h
//
//	@doc:
//		Enforceable order property
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnfdOrder_H
#define GPOPT_CEnfdOrder_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"

namespace duckdb {
class PhysicalOperator;
}

namespace gpopt {
using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@class:
//		COrderProperty
//
//	@doc:
//		Enforceable order property;
//
//---------------------------------------------------------------------------
class COrderProperty {
public:
	enum EPropEnforcingType { EpetRequired, EpetOptional, EpetProhibited, EpetUnnecessary, EpetSentinel };

public:
	// type of order matching function
	enum EOrderMatching { EomSatisfy = 0, EomSentinel };

public:
	// required sort order
	duckdb::unique_ptr<COrderSpec> m_order_spec;

	// order matching type
	EOrderMatching m_order_match_type;

	// names of order matching types
	static const CHAR *m_szOrderMatching[EomSentinel];

public:
	// ctor
	COrderProperty(duckdb::unique_ptr<COrderSpec> pos, EOrderMatching eom);

	// no copy ctor
	COrderProperty(const COrderProperty &) = delete;

	// dtor
	virtual ~COrderProperty();

	// hash function
	virtual size_t HashValue() const;

	// check if the given order specification is compatible with the
	// order specification of this object for the specified matching type
	bool FCompatible(duckdb::unique_ptr<COrderSpec> pos) const;

	// get order enforcing type for the given operator
	EPropEnforcingType
	EorderEnforcingType(CExpressionHandle &exprhdl,
						duckdb::unique_ptr<PhysicalOperator> popPhysical,
						bool fOrderReqd) const;

	// check if operator requires an enforcer under given enforceable property
	// based on the derived enforcing type
	static bool FEnforce(EPropEnforcingType epet) {
		return COrderProperty::EpetOptional == epet || COrderProperty::EpetRequired == epet;
	}

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(duckdb::unique_ptr<CRequiredPhysicalProp> prpp,
						 duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr,
	                     duckdb::unique_ptr<Operator> pexprChild,
						 COrderProperty::EPropEnforcingType epet,
	                     CExpressionHandle &exprhdl) {
		if (FEnforce(epet)) {
			m_order_spec->AppendEnforcers(exprhdl, prpp, pdrgpexpr, std::move(pexprChild));
		}
	}

	// matching function
	bool Matches(duckdb::unique_ptr<COrderProperty> peo) {
		return m_order_match_type == peo->m_order_match_type && m_order_spec->Matches(peo->m_order_spec);
	}

	// check if operator requires optimization under given enforceable property
	// based on the derived enforcing type
	static bool FOptimize(EPropEnforcingType epet) {
		return COrderProperty::EpetOptional == epet || COrderProperty::EpetUnnecessary == epet;
	}
}; // class COrderProperty
} // namespace gpopt
#endif