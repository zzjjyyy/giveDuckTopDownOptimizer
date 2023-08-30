//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtRelational.h
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of relational properties
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxtRelational_H
#define GPOPT_CDrvdPropCtxtRelational_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropertyContext.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxtRelational
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of relational properties
//
//---------------------------------------------------------------------------
class CDrvdPropCtxtRelational : public CDerivedPropertyContext {
public:
	// copy function
	virtual CDerivedPropertyContext * PdpctxtCopy() const
	{
		return new CDrvdPropCtxtRelational();
	}

	// add props to context
	virtual void AddProps(CDerivedProperty * pdp)
	{
		// derived relational context is currently empty
	}

public:
	// ctor
	CDrvdPropCtxtRelational()
		: CDerivedPropertyContext()
	{
	}
	
	// no copy ctor
	CDrvdPropCtxtRelational(const CDrvdPropCtxtRelational &) = delete;

	// dtor
	virtual ~CDrvdPropCtxtRelational()
	{
	}

	// conversion function
	static CDrvdPropCtxtRelational* PdpctxtrelConvert(CDerivedPropertyContext * pdpctxt)
	{
		return static_cast<CDrvdPropCtxtRelational*>(pdpctxt);
	}
};	// class CDrvdPropCtxtRelational
}  // namespace gpopt
#endif