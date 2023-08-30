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
//		CRequiredPropRelational::CRequiredPropRelational
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRequiredPropRelational::CRequiredPropRelational()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropRelational::CRequiredPropRelational
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CRequiredPropRelational::CRequiredPropRelational(duckdb::vector<ColumnBinding> pcrs)
{
	for(auto &child : pcrs)
	{
		m_pcrsStat.emplace_back(child.table_index, child.column_index);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropRelational::CRequiredPropRelationalonal
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CRequiredPropRelational::~CRequiredPropRelational()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropRelational::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CRequiredPropRelational::Compute(CExpressionHandle &exprhdl, CRequiredProperty * prpInput, ULONG child_index, duckdb::vector<CDerivedProperty *> pdrgpdpCtxt, ULONG ulOptReq)
{
	// CRequiredPropRelational* prprelInput = CRequiredPropRelational::GetReqdRelationalProps(prpInput);
	m_pcrsStat = ((LogicalOperator*)exprhdl.Pop())->GetColumnBindings();
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropRelational::GetReqdRelationalProps
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CRequiredPropRelational *CRequiredPropRelational::GetReqdRelationalProps(CRequiredProperty * prp)
{
	return (CRequiredPropRelational *)prp;
}


//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropRelational::PrprelDifference
//
//	@doc:
//		Return difference from given properties
//
//---------------------------------------------------------------------------
CRequiredPropRelational *CRequiredPropRelational::PrprelDifference(CRequiredPropRelational * prprel)
{
	duckdb::vector<ColumnBinding> pcrs;
	duckdb::vector<ColumnBinding> v2 = prprel->PcrsStat();
	std::set_difference(m_pcrsStat.begin(), m_pcrsStat.end(), v2.begin(), v2.end(), pcrs.begin());
	return new CRequiredPropRelational(pcrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CRequiredPropRelational::IsEmpty
//
//	@doc:
//		Return true if property container is empty
//
//---------------------------------------------------------------------------
bool CRequiredPropRelational::IsEmpty() const
{
	return m_pcrsStat.size() == 0;
}