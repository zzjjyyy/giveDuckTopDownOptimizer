//---------------------------------------------------------------------------
//	@filename:
//		CRequiredPhysicalProp.h
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
class CDerivedLogicalProp;
class CDerivedPhysicalProp;
class COrderProperty;
class CExpressionHandle;

//---------------------------------------------------------------------------
//	@class:
//		CRequiredPhysicalProp
//
//	@doc:
//		Required plan properties container.
//
//---------------------------------------------------------------------------
class CRequiredPhysicalProp : public CRequiredProperty {
public:
	// required columns
	duckdb::vector<ColumnBinding> m_cols;
	// required sort order
	COrderProperty *m_sort_order;

public:
	// default ctor
	CRequiredPhysicalProp() : m_sort_order(nullptr) {
	}

	// ctor
	CRequiredPhysicalProp(duckdb::vector<ColumnBinding> pcrs, COrderProperty *peo);

	// copy ctor
	CRequiredPhysicalProp(const CRequiredPhysicalProp &other) = delete;

	// dtor
	virtual ~CRequiredPhysicalProp();

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
	bool Equals(CRequiredPhysicalProp *prpp) const;

	// hash function
	size_t HashValue() const;

	// check if plan properties are satisfied by the given derived properties
	bool FSatisfied(CDerivedLogicalProp *rel, CDerivedPhysicalProp *plan) const;

	// check if plan properties are compatible with the given derived properties
	bool FCompatible(CExpressionHandle &exprhdl, PhysicalOperator *popPhysical, CDerivedLogicalProp *pdprel,
	                 CDerivedPhysicalProp *pdpplan) const;

	// check if expression attached to handle provides required columns by all plan properties
	bool FProvidesReqdCols(CExpressionHandle &exprhdl, ULONG ulOptReq) const;

	// shorthand for conversion
	static CRequiredPhysicalProp *Prpp(CRequiredProperty *prp) {
		return (CRequiredPhysicalProp *)prp;
	}

	// generate empty required properties
	static CRequiredPhysicalProp *PrppEmpty();

	// hash function used for cost bounding
	static ULONG UlHashForCostBounding(CRequiredPhysicalProp *prpp);

	// equality function used for cost bounding
	static bool FEqualForCostBounding(CRequiredPhysicalProp *prppFst, CRequiredPhysicalProp *prppSnd);
	// map input required and derived plan properties into new required plan properties
	// static CRequiredPhysicalProp* PrppRemapForCTE(CRequiredPhysicalProp *prppInput, CDerivedPhysicalProp *pdpplanInput);
}; // class CRequiredPhysicalProp
} // namespace gpopt
#endif