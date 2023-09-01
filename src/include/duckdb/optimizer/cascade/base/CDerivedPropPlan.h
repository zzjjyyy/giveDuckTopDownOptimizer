//---------------------------------------------------------------------------
//	@filename:
//		CDerivedPhysicalProp.h
//
//	@doc:
//		Derived physical properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropPlan_H
#define GPOPT_CDrvdPropPlan_H

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedProperty.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"

namespace gpopt {
using namespace duckdb;
using namespace gpos;

// fwd declaration
class COrderSpec;
class CRequiredPhysicalProp;
class CDerivedPhysicalProp;

//---------------------------------------------------------------------------
//	@class:
//		CDerivedPhysicalProp
//
//	@doc:
//		Derived plan properties container.
//
//		These are properties that are expression-specific and they depend on
//		the physical implementation. This includes sort order, distribution,
//		rewindability, partition propagation spec and CTE map.
//
//---------------------------------------------------------------------------
class CDerivedPhysicalProp : public CDerivedProperty {
public:
	CDerivedPhysicalProp();
	CDerivedPhysicalProp(const CDerivedPhysicalProp &) = delete;
	virtual ~CDerivedPhysicalProp();

	// derived sort order
	COrderSpec *m_sort_order;

	// derived cte map
	// CCTEMap* m_pcm;

	// copy CTE producer plan properties from given context to current object
	// void CopyCTEProducerPlanProps(CDerivedPropertyContext* pdpctxt, Operator* pop);

public:
	// type of properties
	CDerivedProperty::EPropType PropertyType() override {
		return CDerivedProperty::EPropType::EptPlan;
	}

	// derivation function
	void Derive(gpopt::CExpressionHandle &pop, CDerivedPropertyContext *pdpctxt) override;

	// short hand for conversion
	static CDerivedPhysicalProp *DrvdPlanProperty(CDerivedProperty *pdp);

	// cte map
	// CCTEMap* GetCostModel() const
	// {
	//	return m_pcm;
	// }

	// hash function
	virtual ULONG HashValue() const;
	// equality function
	virtual ULONG Equals(const CDerivedPhysicalProp *pdpplan) const;
	// check for satisfying required plan properties
	virtual BOOL FSatisfies(const CRequiredPhysicalProp *prpp) const override;
}; // class CDerivedPhysicalProp
} // namespace gpopt
#endif