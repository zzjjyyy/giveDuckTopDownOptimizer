//---------------------------------------------------------------------------
//	@filename:
//		CRequiredPropPlan.h
//
//	@doc:
//		Derived required relational properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdPropPlan_H
#define GPOPT_CReqdPropPlan_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COrderProperty.h"
#include "duckdb/optimizer/cascade/base/CRequiredProperty.h"
#include "duckdb/planner/expression.hpp"

namespace gpopt {
using namespace duckdb;
using namespace gpos;

// forward declaration
class CDrvdPropRelational;
class CDrvdPropPlan;
class COrderProperty;
class CExpressionHandle;

//---------------------------------------------------------------------------
//	@class:
//		CRequiredPropPlan
//
//	@doc:
//		Required plan properties container.
//
//---------------------------------------------------------------------------
class CRequiredPropPlan : public CRequiredProperty {
public:
	// required columns
	duckdb::vector<ColumnBinding> m_required_cols;
	// required sort order
	COrderProperty *m_required_sort_order;

public:
	// default ctor
	CRequiredPropPlan() : m_required_sort_order(NULL) {
	}

	// ctor
	CRequiredPropPlan(duckdb::vector<ColumnBinding> pcrs, COrderProperty *peo);

	// copy ctor
	CRequiredPropPlan(const CRequiredPropPlan &other) = delete;

	// dtor
	virtual ~CRequiredPropPlan();

	// type of properties
	virtual bool FPlan() const override {
		return true;
	}

	// required properties computation function
	virtual void Compute(CExpressionHandle &exprhdl, CRequiredProperty *prpInput, ULONG child_index,
	                     duckdb::vector<CDrvdProp *> pdrgpdpCtxt, ULONG ulOptReq) override;

	// required columns computation function
	void ComputeReqdCols(CExpressionHandle &exprhdl, CRequiredProperty *prpInput, ULONG child_index,
	                     duckdb::vector<CDrvdProp *> pdrgpdpCtxt);

	// equality function
	bool Equals(CRequiredPropPlan *prpp) const;

	// hash function
	ULONG HashValue() const;

	// check if plan properties are satisfied by the given derived properties
	bool FSatisfied(CDrvdPropRelational *pdprel, CDrvdPropPlan *pdpplan) const;

	// check if plan properties are compatible with the given derived properties
	bool FCompatible(CExpressionHandle &exprhdl, PhysicalOperator *popPhysical, CDrvdPropRelational *pdprel,
	                 CDrvdPropPlan *pdpplan) const;

	// check if expression attached to handle provides required columns by all plan properties
	bool FProvidesReqdCols(CExpressionHandle &exprhdl, ULONG ulOptReq) const;

	// shorthand for conversion
	static CRequiredPropPlan *Prpp(CRequiredProperty *prp) {
		return (CRequiredPropPlan *)prp;
	}

	// generate empty required properties
	static CRequiredPropPlan *PrppEmpty();

	// hash function used for cost bounding
	static ULONG UlHashForCostBounding(CRequiredPropPlan *prpp);

	// equality function used for cost bounding
	static bool FEqualForCostBounding(CRequiredPropPlan *prppFst, CRequiredPropPlan *prppSnd);
	// map input required and derived plan properties into new required plan properties
	// static CRequiredPropPlan* PrppRemapForCTE(CRequiredPropPlan *prppInput, CDrvdPropPlan *pdpplanInput);
}; // class CRequiredPropPlan
} // namespace gpopt
#endif