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
void CRequiredLogicalProp::Compute(CExpressionHandle &exprhdl, CRequiredProperty *prpInput, ULONG child_index,
                                      duckdb::vector<CDerivedProperty *> pdrgpdpCtxt, ULONG ulOptReq) {
	// CRequiredLogicalProp* prprelInput = CRequiredLogicalProp::GetReqdRelationalProps(prpInput);
	m_pcrsStat = ((LogicalOperator *)exprhdl.Pop())->GetColumnBindings();
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::GetReqdRelationalProps
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CRequiredLogicalProp *CRequiredLogicalProp::GetReqdRelationalProps(CRequiredProperty *prp) {
	return (CRequiredLogicalProp *)prp;
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredLogicalProp::PrprelDifference
//
//	@doc:
//		Return difference from given properties
//
//---------------------------------------------------------------------------
CRequiredLogicalProp *CRequiredLogicalProp::PrprelDifference(CRequiredLogicalProp *prprel) {
	duckdb::vector<ColumnBinding> pcrs;
	duckdb::vector<ColumnBinding> v2 = prprel->PcrsStat();
	std::set_difference(m_pcrsStat.begin(), m_pcrsStat.end(), v2.begin(), v2.end(), pcrs.begin());
	return new CRequiredLogicalProp(pcrs);
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