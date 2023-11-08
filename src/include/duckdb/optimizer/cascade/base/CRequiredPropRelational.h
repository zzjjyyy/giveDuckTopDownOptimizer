//---------------------------------------------------------------------------
//	@filename:
//		CRequiredLogicalProp.h
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
//		CRequiredLogicalProp
//
//	@doc:
//		Required relational properties container.
//
//---------------------------------------------------------------------------
class CRequiredLogicalProp : public CRequiredProperty {
public:
	// required stat columns
	duckdb::vector<ColumnBinding> m_pcrsStat;

public:
	// default ctor
	CRequiredLogicalProp();
	
	// private copy ctor
	CRequiredLogicalProp(const CRequiredLogicalProp &) = delete;
	
	// ctor
	explicit CRequiredLogicalProp(duckdb::vector<ColumnBinding> pcrs);

	// dtor
	virtual ~CRequiredLogicalProp();

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
	virtual void
	Compute(CExpressionHandle &exprhdl,
			duckdb::unique_ptr<CRequiredProperty> prpInput,
			ULONG child_index,
			duckdb::vector<duckdb::unique_ptr<CDerivedProperty>> pdrgpdpCtxt,
			ULONG ulOptReq) override;

	// return difference from given properties
	duckdb::unique_ptr<CRequiredLogicalProp>
	PrprelDifference(duckdb::unique_ptr<CRequiredLogicalProp> prprel);

	// return true if property container is empty
	bool IsEmpty() const;

	// shorthand for conversion
	static duckdb::unique_ptr<CRequiredLogicalProp>
	GetReqdRelationalProps(duckdb::unique_ptr<CRequiredProperty> prp);
};	// class CRequiredLogicalProp

}  // namespace gpopt


#endif
