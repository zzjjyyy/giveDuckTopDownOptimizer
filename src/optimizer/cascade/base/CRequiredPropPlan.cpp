//---------------------------------------------------------------------------
//	@filename:
//		CRequiredPropPlan.cpp
//
//	@doc:
//		Required plan properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CRequiredPropPlan.h"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropPlan.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/common/CPrintablePointer.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

namespace gpopt {
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::CRequiredPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRequiredPropPlan::CRequiredPropPlan(duckdb::vector<ColumnBinding> pcrs, COrderProperty *peo)
    : m_cols(pcrs), m_sort_order(peo) {
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::CRequiredPropertyPlanertyPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CRequiredPropPlan::~CRequiredPropPlan() {
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::ComputeReqdCols
//
//	@doc:
//		Compute required columns
//
//---------------------------------------------------------------------------
void CRequiredPropPlan::ComputeReqdCols(CExpressionHandle &exprhdl, CRequiredProperty *prpInput, ULONG child_index,
                                        duckdb::vector<CDerivedProperty *> pdrgpdpCtxt) {
	CRequiredPropPlan *prppInput = CRequiredPropPlan::Prpp(prpInput);
	PhysicalOperator *popPhysical = (PhysicalOperator *)exprhdl.Pop();
	m_cols = popPhysical->PcrsRequired(exprhdl, prppInput->m_cols, child_index, pdrgpdpCtxt, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CRequiredPropPlan::Compute(CExpressionHandle &expr_handle, CRequiredProperty *property, ULONG child_index,
                                duckdb::vector<CDerivedProperty *> children_derived_prop, ULONG num_opt_request) {
	CRequiredPropPlan *property_plan = CRequiredPropPlan::Prpp(property);
	PhysicalOperator *physical_op = (PhysicalOperator *)expr_handle.Pop();
	ComputeReqdCols(expr_handle, property, child_index, children_derived_prop);
	m_sort_order =
	    new COrderProperty(physical_op->RequiredSortSpec(expr_handle, property_plan->m_sort_order->m_order_spec,
	                                                     child_index, children_derived_prop, num_opt_request),
	    physical_op->OrderMatching(property_plan, child_index, children_derived_prop, num_opt_request));
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::Equals
//
//	@doc:
//		Check if expression attached to handle provides required columns
//		by all plan properties
//
//---------------------------------------------------------------------------
bool CRequiredPropPlan::FProvidesReqdCols(CExpressionHandle &exprhdl, ULONG ulOptReq) const {
	// check if operator provides required columns
	if (!((PhysicalOperator *)exprhdl.Pop())->FProvidesReqdCols(exprhdl, m_cols, ulOptReq)) {
		return false;
	}
	duckdb::vector<ColumnBinding> pcrsOutput = exprhdl.DeriveOutputColumns();
	// check if property spec members use columns from operator output
	bool fProvidesReqdCols = true;
	COrderSpec *pps = m_sort_order->m_order_spec;
	if (NULL == pps) {
		return fProvidesReqdCols;
	}
	duckdb::vector<ColumnBinding> pcrsUsed = pps->PcrsUsed();
	duckdb::vector<ColumnBinding> v;
	for (auto &child : pcrsUsed) {
		fProvidesReqdCols = false;
		for (auto &sub_child : pcrsOutput) {
			if (child == sub_child) {
				fProvidesReqdCols = true;
				break;
			}
		}
		if (!fProvidesReqdCols) {
			return fProvidesReqdCols;
		}
	}
	return fProvidesReqdCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool CRequiredPropPlan::Equals(CRequiredPropPlan *prpp) const {
	return CUtils::Equals(m_cols, prpp->m_cols) && m_sort_order->Matches(prpp->m_sort_order);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::HashValue
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
ULONG CRequiredPropPlan::HashValue() const {
	ULONG ulHash = 0;
	for (ULONG m = 0; m < m_cols.size(); m++) {
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue(&m_cols[m]));
	}
	ulHash = gpos::CombineHashes(ulHash, m_sort_order->HashValue());
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::FSatisfied
//
//	@doc:
//		Check if plan properties are satisfied by the given derived properties
//
//---------------------------------------------------------------------------
bool CRequiredPropPlan::FSatisfied(CDerivedPropRelation *rel, CDerivedPropPlan *plan) const {
	// first, check satisfiability of relational properties
	if (!rel->FSatisfies(this)) {
		return false;
	}
	// otherwise, check satisfiability of all plan properties
	return plan->FSatisfies(this);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::FCompatible
//
//	@doc:
//		Check if plan properties are compatible with the given derived properties
//
//---------------------------------------------------------------------------
bool CRequiredPropPlan::FCompatible(CExpressionHandle &exprhdl, PhysicalOperator *popPhysical,
                                    CDerivedPropRelation *pdprel, CDerivedPropPlan *pdpplan) const {
	// first, check satisfiability of relational properties, including required columns
	if (!pdprel->FSatisfies(this)) {
		return false;
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::PrppEmpty
//
//	@doc:
//		Generate empty required properties
//
//---------------------------------------------------------------------------
CRequiredPropPlan *CRequiredPropPlan::PrppEmpty() {
	duckdb::vector<ColumnBinding> pcrs;
	COrderSpec *pos = new COrderSpec();
	COrderProperty *peo = new COrderProperty(pos, COrderProperty::EomSatisfy);
	return new CRequiredPropPlan(pcrs, peo);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::UlHashForCostBounding
//
//	@doc:
//		Hash function used for cost bounding
//
//---------------------------------------------------------------------------
ULONG CRequiredPropPlan::UlHashForCostBounding(CRequiredPropPlan *prpp) {
	duckdb::vector<ColumnBinding> v = prpp->m_cols;
	ULONG ulHash = 0;
	for (size_t m = 0; m < v.size(); m++) {
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue(&v[m]));
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropPlan::FEqualForCostBounding
//
//	@doc:
//		Equality function used for cost bounding
//
//---------------------------------------------------------------------------
bool CRequiredPropPlan::FEqualForCostBounding(CRequiredPropPlan *prppFst, CRequiredPropPlan *prppSnd) {
	return CUtils::Equals(prppFst->m_cols, prppSnd->m_cols);
}
} // namespace gpopt