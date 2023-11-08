//---------------------------------------------------------------------------
//	@filename:
//		CPartialPlan.h
//
//	@doc:
//
//		A partial plan is a group expression where none (or not all) of its
//		optimal child plans are discovered yet,
//		by assuming the smallest possible cost of unknown child plans, a partial
//		plan's cost gives a lower bound on the cost of the corresponding complete plan,
//		this information is used to prune the optimization search space during branch
//		and bound search
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartialPlan_H
#define GPOPT_CPartialPlan_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CRequiredProperty.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"

#include <memory>

namespace gpopt
{
using namespace gpos;

class CGroupExpression;
class CCostContext;

//---------------------------------------------------------------------------
//	@class:
//		CPartialPlan
//
//	@doc:
//		Description of partial plans created during optimization
//
//---------------------------------------------------------------------------
class CPartialPlan
{
public:
	// equality function
	bool operator==(const CPartialPlan &pppSnd) const;
	
public:
	// root group expression
	duckdb::unique_ptr<CGroupExpression> m_pgexpr;

	// required plan properties of root operator
	duckdb::unique_ptr<CRequiredPhysicalProp> m_prpp;

	// cost context of known child plan -- can be null if no child plans are known
	duckdb::unique_ptr<CCostContext> m_pccChild;

	// index of known child plan
	ULONG m_ulChildIndex;

public:
	// ctor
	CPartialPlan(duckdb::unique_ptr<CGroupExpression> pgexpr,
				 duckdb::unique_ptr<CRequiredPhysicalProp> prpp,
				 duckdb::unique_ptr<CCostContext> pccChild,
				 ULONG child_index);
	
	// no copy ctor
	CPartialPlan(const CPartialPlan &) = delete;
	
	// dtor
	virtual ~CPartialPlan();

public:
	// extract costing info from children
	void ExtractChildrenCostingInfo(duckdb::unique_ptr<ICostModel> pcm,
									CExpressionHandle &exprhdl,
									duckdb::unique_ptr<ICostModel::SCostingInfo> pci);

	// compute partial plan cost
	double CostCompute();

	size_t HashValue() const;

	// hash function used for cost bounding
	static size_t
	HashValue(const duckdb::unique_ptr<CPartialPlan> ppp);

	// equality function used for for cost bounding
	static bool
	Equals(const duckdb::unique_ptr<CPartialPlan> pppFst,
		   const duckdb::unique_ptr<CPartialPlan> pppSnd);
};	// class CPartialPlan
}  // namespace gpopt
#endif