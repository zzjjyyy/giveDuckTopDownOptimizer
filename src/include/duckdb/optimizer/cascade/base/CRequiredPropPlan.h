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
class CDerivedPropRelation;
class CDerivedPropPlan;
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
	duckdb::vector<ColumnBinding> m_cols;
	// required sort order
	COrderProperty *m_sort_order;

public:
	// default ctor
	CRequiredPropPlan() : m_sort_order(nullptr) {
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
	virtual void Compute(CExpressionHandle &expr_handle, CRequiredProperty *property, ULONG child_index,
	                     duckdb::vector<CDerivedProperty *> children_derived_prop, ULONG num_opt_request) override;

	// required columns computation function
	void ComputeReqdCols(CExpressionHandle &exprhdl, CRequiredProperty *prpInput, ULONG child_index,
	                     duckdb::vector<CDerivedProperty *> pdrgpdpCtxt);

	// equality function
	bool Equals(CRequiredPropPlan *prpp) const;

	// hash function
	ULONG HashValue() const;

	// check if plan properties are satisfied by the given derived properties
	bool FSatisfied(CDerivedPropRelation *rel, CDerivedPropPlan *plan) const;

	// check if plan properties are compatible with the given derived properties
	bool FCompatible(CExpressionHandle &exprhdl, PhysicalOperator *popPhysical, CDerivedPropRelation *pdprel,
	                 CDerivedPropPlan *pdpplan) const;

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
	// static CRequiredPropPlan* PrppRemapForCTE(CRequiredPropPlan *prppInput, CDerivedPropPlan *pdpplanInput);
}; // class CRequiredPropPlan
} // namespace gpopt
#endif