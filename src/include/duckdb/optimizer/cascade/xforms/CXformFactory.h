//---------------------------------------------------------------------------
//	@filename:
//		CXformFactory.h
//
//	@doc:
//		Management of global xform set
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformFactory
//
//	@doc:
//		Factory class to manage xforms
//
//---------------------------------------------------------------------------
class CXformFactory {
public:
	// range of all xforms
	duckdb::unique_ptr<CXform> m_xform_range[CXform::ExfSentinel];
	// name -> xform map
	unordered_map<CHAR *, duckdb::unique_ptr<CXform>> m_xform_dict;
	// bitset of exploration xforms
	duckdb::unique_ptr<CXform_set> m_exploration_xforms;
	// bitset of implementation xforms
	duckdb::unique_ptr<CXform_set> m_implementation_xforms;
	// global instance
	static duckdb::unique_ptr<CXformFactory> m_xform_factory;

public:
	// ctor
	explicit CXformFactory();

	// no copy ctor
	CXformFactory(const CXformFactory &) = delete;

	// dtor
	~CXformFactory();

public:
	// actual adding of xform
	void Add(duckdb::unique_ptr<CXform> xform);

	// create all xforms
	void Instantiate();

	// accessor by xform id
	duckdb::unique_ptr<CXform> Xform(CXform::EXformId xform_id) const;

	// accessor by xform name
	duckdb::unique_ptr<CXform> Xform(const CHAR *xform_name) const;

	// accessor of exploration xforms
	duckdb::unique_ptr<CXform_set>
	XformExploration() const {
		return m_exploration_xforms;
	}
	
	// accessor of implementation xforms
	duckdb::unique_ptr<CXform_set>
	XformImplementation() const {
		return m_implementation_xforms;
	}

	// global accessor
	static duckdb::unique_ptr<CXformFactory>
	XformFactory() {
		return m_xform_factory;
	}

	// initialize global factory instance
	static GPOS_RESULT Init();
	
	// destroy global factory instance
	void Shutdown();
}; // class CXformFactory
} // namespace gpopt