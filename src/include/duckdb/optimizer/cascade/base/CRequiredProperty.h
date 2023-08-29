//---------------------------------------------------------------------------
//	@filename:
//		CRequiredProperty.h
//
//	@doc:
//		Base class for all required properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdProp_H
#define GPOPT_CReqdProp_H

#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"

#include <memory>

namespace gpopt {
using namespace gpos;
using namespace duckdb;

// forward declarations
class CExpressionHandle;
class Operator;
class CRequiredProperty;

//---------------------------------------------------------------------------
//	@class:
//		CRequiredProperty
//
//	@doc:
//		Abstract base class for all required properties. Individual property
//		components are added separately. CRequiredProperty is memory pool-agnostic.
//
//		All derived property classes implement a pure virtual function
//		CRequiredProperty::Compute(). This function is responsible for filling in the
//		different properties in the property container.
//
//		During query optimization, the optimizer looks for a physical plan that
//		satisfies a number of requirements. These requirements may be specified
//		on the query level such as an ORDER BY clause that specifies a required
//		sort order to be satisfied by query results (see Query Context
//		section).  Alternatively, the requirements may be triggered when
//		optimizing a plan subtree. For example, a Hash Join operator may
//		require its children to have hash distributions aligned with the
//		columns mentioned in join condition.
//
//		In either case, each operator receives requirements from the parent and
//		combines these requirements with local requirements to generate new
//		requirements (which may be empty) from each of its children.
//		Recursively, this operation keeps going until the requirements are
//		propagated to all nodes in a given plan in a top-down fashion.  The
//		initial set of requirements from the query are constructed in
//		CQueryContext::QueryContextGenerate().
//
//		Thus each operator needs to implement a virtual requirement function
//		to be called by this mechanism. For example, CPhysical::PcrsChildReqd()
//		is used to compute the required columns by a given operator from its
//		children.
//
//---------------------------------------------------------------------------
class CRequiredProperty {
public:
	// types of required properties
	enum EPropType { EptRelational, EptPlan, EptSentinel };

public:
	// ctor
	CRequiredProperty();

	// no copy ctor
	CRequiredProperty(const CRequiredProperty &) = delete;

	// dtor
	virtual ~CRequiredProperty();

	// is it a relational property?
	virtual bool FRelational() const {
		return false;
	}

	// is it a plan property?
	virtual bool FPlan() const {
		return false;
	}

	// required properties computation function
	virtual void Compute(CExpressionHandle &exprhdl, CRequiredProperty *prpInput, ULONG child_index,
	                     duckdb::vector<CDrvdProp *> pdrgpdpCtxt, ULONG ulOptRe) = 0;
}; // class CRequiredProperty
} // namespace gpopt
#endif