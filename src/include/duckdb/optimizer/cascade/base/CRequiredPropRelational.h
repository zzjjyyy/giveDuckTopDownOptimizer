//---------------------------------------------------------------------------
//	@filename:
//		CRequiredPropRelational.h
//
//	@doc:
//		Derived required relational properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdPropRelational_H
#define GPOPT_CReqdPropRelational_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CRequiredProperty.h"
#include "duckdb/planner/expression.hpp"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

// forward declaration
class CExpressionHandle;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CRequiredPropRelational
//
//	@doc:
//		Required relational properties container.
//
//---------------------------------------------------------------------------
class CRequiredPropRelational : public CRequiredProperty {
public:
	// required stat columns
	duckdb::vector<ColumnBinding> m_pcrsStat;

public:
	// default ctor
	CRequiredPropRelational();
	
	// private copy ctor
	CRequiredPropRelational(const CRequiredPropRelational &) = delete;
	
	// ctor
	explicit CRequiredPropRelational(duckdb::vector<ColumnBinding> pcrs);

	// dtor
	virtual ~CRequiredPropRelational();

	// type of properties
	virtual bool FRelational() const override
	{
		return true;
	}

	// stat columns accessor
	duckdb::vector<ColumnBinding> PcrsStat() const
	{
		return m_pcrsStat;
	}

	// required properties computation function
	virtual void Compute(CExpressionHandle &exprhdl, CRequiredProperty * prpInput, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt, ULONG ulOptReq) override;

	// return difference from given properties
	CRequiredPropRelational * PrprelDifference(CRequiredPropRelational * prprel);

	// return true if property container is empty
	bool IsEmpty() const;

	// shorthand for conversion
	static CRequiredPropRelational * GetReqdRelationalProps(CRequiredProperty * prp);
};	// class CRequiredPropRelational

}  // namespace gpopt


#endif
