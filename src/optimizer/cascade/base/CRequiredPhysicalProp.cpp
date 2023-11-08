//---------------------------------------------------------------------------
//	@filename:
//		CRequiredPhysicalProp.cpp
//
//	@doc:
//		Required plan properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CRequiredPhysicalProp.h"

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
//		CRequiredPhysicalProp::CRequiredPhysicalProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRequiredPhysicalProp::CRequiredPhysicalProp(duckdb::vector<ColumnBinding> pcrs, duckdb::unique_ptr<COrderProperty> peo)
    : m_cols(pcrs), m_sort_order(peo) {
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::CRequiredPropertyPlanertyPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CRequiredPhysicalProp::~CRequiredPhysicalProp() {
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::ComputeReqdCols
//
//	@doc:
//		Compute required columns
//
//---------------------------------------------------------------------------
void CRequiredPhysicalProp::ComputeReqdCols(CExpressionHandle &exprhdl,
											duckdb::unique_ptr<CRequiredProperty> prpInput,
											ULONG child_index,
                                        	duckdb::vector<duckdb::unique_ptr<CDerivedProperty>> pdrgpdpCtxt) {
	auto prppInput = CRequiredPhysicalProp::Prpp(prpInput);
	auto popPhysical = unique_ptr_cast<Operator, PhysicalOperator>(exprhdl.Pop());
	m_cols = popPhysical->PcrsRequired(exprhdl, prppInput->m_cols, child_index, pdrgpdpCtxt, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CRequiredPhysicalProp::Compute(CExpressionHandle &expr_handle,
									duckdb::unique_ptr<CRequiredProperty> property,
									ULONG child_index,
                                	duckdb::vector<duckdb::unique_ptr<CDerivedProperty>> children_derived_prop,
									ULONG num_opt_request) {
	auto property_plan = CRequiredPhysicalProp::Prpp(property);
	auto physical_op = unique_ptr_cast<Operator, PhysicalOperator>(expr_handle.Pop());
	ComputeReqdCols(expr_handle, property, child_index, children_derived_prop);
	m_sort_order =
	    make_uniq<COrderProperty>(physical_op->RequiredSortSpec(expr_handle,
																property_plan->m_sort_order->m_order_spec,
	                                                     		child_index,
																children_derived_prop,
																num_opt_request),
	    physical_op->OrderMatching(property_plan, child_index, children_derived_prop, num_opt_request));
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::Equals
//
//	@doc:
//		Check if expression attached to handle provides required columns
//		by all plan properties
//
//---------------------------------------------------------------------------
bool CRequiredPhysicalProp::FProvidesReqdCols(CExpressionHandle &exprhdl, ULONG ulOptReq) const {
	// check if operator provides required columns
	if (!unique_ptr_cast<Operator, PhysicalOperator>(exprhdl.Pop())->FProvidesReqdCols(exprhdl, m_cols, ulOptReq)) {
		return false;
	}
	auto pcrsOutput = exprhdl.DeriveOutputColumns();
	// check if property spec members use columns from operator output
	bool fProvidesReqdCols = true;
	auto pps = m_sort_order->m_order_spec;
	if (NULL == pps) {
		return fProvidesReqdCols;
	}
	auto pcrsUsed = pps->PcrsUsed();
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
//		CRequiredPhysicalProp::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool CRequiredPhysicalProp::Equals(duckdb::unique_ptr<CRequiredPhysicalProp> prpp) const {
	return CUtils::Equals(m_cols, prpp->m_cols) && m_sort_order->Matches(prpp->m_sort_order);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::HashValue
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
size_t CRequiredPhysicalProp::HashValue() const {
	size_t ulHash = 0;
	for (size_t m = 0; m < m_cols.size(); m++) {
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue(&m_cols[m]));
	}
	ulHash = gpos::CombineHashes(ulHash, m_sort_order->HashValue());
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::FSatisfied
//
//	@doc:
//		Check if plan properties are satisfied by the given derived properties
//
//---------------------------------------------------------------------------
bool
CRequiredPhysicalProp::FSatisfied(duckdb::unique_ptr<CRequiredPhysicalProp> this_physical_prop,
							      duckdb::unique_ptr<CDerivedLogicalProp> rel,
								  duckdb::unique_ptr<CDerivedPhysicalProp> plan) const {
	// first, check satisfiability of relational properties
	if (!rel->FSatisfies(this_physical_prop)) {
		return false;
	}
	// otherwise, check satisfiability of all plan properties
	return plan->FSatisfies(this_physical_prop);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::FCompatible
//
//	@doc:
//		Check if plan properties are compatible with the given derived properties
//
//---------------------------------------------------------------------------
bool CRequiredPhysicalProp::FCompatible(duckdb::unique_ptr<CRequiredPhysicalProp> this_req_physical_prop,
										CExpressionHandle &exprhdl,
									    duckdb::unique_ptr<PhysicalOperator> popPhysical,
                                        duckdb::unique_ptr<CDerivedLogicalProp> pdprel,
										duckdb::unique_ptr<CDerivedPhysicalProp> pdpplan) const {
	// first, check satisfiability of relational properties, including required columns
	if (!pdprel->FSatisfies(this_req_physical_prop)) {
		return false;
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::PrppEmpty
//
//	@doc:
//		Generate empty required properties
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CRequiredPhysicalProp> CRequiredPhysicalProp::PrppEmpty() {
	duckdb::vector<ColumnBinding> pcrs;
	auto pos = make_uniq<COrderSpec>();
	auto peo = make_uniq<COrderProperty>(pos, COrderProperty::EomSatisfy);
	return make_uniq<CRequiredPhysicalProp>(pcrs, peo);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::UlHashForCostBounding
//
//	@doc:
//		Hash function used for cost bounding
//
//---------------------------------------------------------------------------
ULONG CRequiredPhysicalProp::UlHashForCostBounding(duckdb::unique_ptr<CRequiredPhysicalProp> prpp) {
	auto v = prpp->m_cols;
	ULONG ulHash = 0;
	for (size_t m = 0; m < v.size(); m++) {
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue(&v[m]));
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPhysicalProp::FEqualForCostBounding
//
//	@doc:
//		Equality function used for cost bounding
//
//---------------------------------------------------------------------------
bool CRequiredPhysicalProp::FEqualForCostBounding(duckdb::unique_ptr<CRequiredPhysicalProp> prppFst,
												  duckdb::unique_ptr<CRequiredPhysicalProp> prppSnd) {
	return CUtils::Equals(prppFst->m_cols, prppSnd->m_cols);
}
} // namespace gpopt