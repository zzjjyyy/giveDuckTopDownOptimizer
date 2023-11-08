//---------------------------------------------------------------------------
//	@filename:
//		CDerivedPropertyContext.h
//
//	@doc:
//		Base class for derived properties context;
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxt_H
#define GPOPT_CDrvdPropCtxt_H

#include "duckdb/optimizer/cascade/base.h"

#include <memory>

using namespace gpos;
using namespace std;

namespace gpopt {
// fwd declarations
class CDerivedPropertyContext;
class CDerivedProperty;

//---------------------------------------------------------------------------
//	@class:
//		CDerivedPropertyContext
//
//	@doc:
//		Container of information passed among expression nodes during
//		property derivation
//
//---------------------------------------------------------------------------
class CDerivedPropertyContext {
public:
	// ctor
	CDerivedPropertyContext() {
	}

	// no copy ctor
	CDerivedPropertyContext(const CDerivedPropertyContext &) = delete;

	// dtor
	virtual ~CDerivedPropertyContext() {
	}

public:
	// copy function
	virtual duckdb::unique_ptr<CDerivedPropertyContext> PdpctxtCopy() const = 0;

	// add props to context
	virtual void AddProps(duckdb::unique_ptr<CDerivedProperty> pdp) = 0;

public:
	// copy function
	static duckdb::unique_ptr<CDerivedPropertyContext>
	PdpctxtCopy(duckdb::unique_ptr<CDerivedPropertyContext> pdpctxt) {
		if (nullptr == pdpctxt) {
			return nullptr;
		}
		return pdpctxt->PdpctxtCopy();
	}

	// add derived props to context
	static void
	AddDerivedProps(duckdb::unique_ptr<CDerivedProperty> pdp,
					duckdb::unique_ptr<CDerivedPropertyContext> pdpctxt) {
		if (nullptr != pdpctxt) {
			pdpctxt->AddProps(pdp);
		}
	}
}; // class CDerivedPropertyContext
} // namespace gpopt
#endif