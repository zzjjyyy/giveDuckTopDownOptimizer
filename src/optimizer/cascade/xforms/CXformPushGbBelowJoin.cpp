//---------------------------------------------------------------------------
//	@filename:
//		CXformPushGbBelowJoin.cpp
//
//	@doc:
//		Implementation of pushing group by below join transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformPushGbBelowJoin.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/optimizer/cascade/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::CXformPushGbBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbBelowJoin::CXformPushGbBelowJoin()
	: CXformExploration(make_uniq<LogicalAggregate>(0, 0, duckdb::vector<duckdb::unique_ptr<Expression>>()))
{
    duckdb::unique_ptr<LogicalComparisonJoin> child = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    child->AddChild(make_uniq<CPatternLeaf>());
    child->AddChild(make_uniq<CPatternLeaf>());
	this->m_operator->AddChild(std::move(child));
}

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::CXformPushGbBelowJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushGbBelowJoin::CXformPushGbBelowJoin(duckdb::unique_ptr<Operator> pexprPattern)
	: CXformExploration(std::move(pexprPattern))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		we only push down global aggregates
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformPushGbBelowJoin::XformPromise(CExpressionHandle &exprhdl) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushGbBelowJoin::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformPushGbBelowJoin::Transform(duckdb::unique_ptr<CXformContext> pxfctxt,
				   					  duckdb::unique_ptr<CXformResult> pxfres,
				   					  duckdb::unique_ptr<Operator> pexpr) const
{
    auto op_agg = unique_ptr_cast<Operator, LogicalAggregate>(pexpr);
    if (op_agg->groups.empty()) {
        return;
    }
	duckdb::unique_ptr<Operator> pexprResult = CXformUtils::PexprPushGbBelowJoin(pexpr);
	if (nullptr != pexprResult)
	{
		// add alternative to results
		pxfres->Add(std::move(pexprResult));
	}
}