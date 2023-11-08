//---------------------------------------------------------------------------
//	@filename:
//		CReqPropRelational.cpp
//
//	@doc:
//		Required relational properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropRelational.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::CRequiredLogicalProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRequiredLogicalProp::CRequiredLogicalProp() {
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::CRequiredLogicalProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRequiredLogicalProp::CRequiredLogicalProp(duckdb::vector<ColumnBinding> pcrs) {
	for (auto &child : pcrs) {
		m_pcrsStat.emplace_back(child.table_index, child.column_index);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::CRequiredPropRelationalonal
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CRequiredLogicalProp::~CRequiredLogicalProp() {
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CRequiredLogicalProp::Compute(CExpressionHandle &exprhdl,
								   duckdb::unique_ptr<CRequiredProperty> prpInput,
								   ULONG child_index,
                                   duckdb::vector<duckdb::unique_ptr<CDerivedProperty>> pdrgpdpCtxt,
								   ULONG ulOptReq) {
	// CRequiredLogicalProp* prprelInput = CRequiredLogicalProp::GetReqdRelationalProps(prpInput);
	m_pcrsStat = unique_ptr_cast<Operator, LogicalOperator>(exprhdl.Pop())->GetColumnBindings();
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::GetReqdRelationalProps
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CRequiredLogicalProp>
CRequiredLogicalProp::GetReqdRelationalProps(duckdb::unique_ptr<CRequiredProperty> prp) {
	return unique_ptr_cast<CRequiredProperty, CRequiredLogicalProp>(prp);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::PrprelDifference
//
//	@doc:
//		Return difference from given properties
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CRequiredLogicalProp>
CRequiredLogicalProp::PrprelDifference(duckdb::unique_ptr<CRequiredLogicalProp> prprel) {
	duckdb::vector<ColumnBinding> pcrs;
	duckdb::vector<ColumnBinding> v2 = prprel->PcrsStat();
	std::set_difference(m_pcrsStat.begin(), m_pcrsStat.end(), v2.begin(), v2.end(), pcrs.begin());
	return make_uniq<CRequiredLogicalProp>(pcrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::IsEmpty
//
//	@doc:
//		Return true if property container is empty
//
//---------------------------------------------------------------------------
bool CRequiredLogicalProp::IsEmpty() const {
	return m_pcrsStat.size() == 0;
}