//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformInnerJoin2HashJoin.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/joinside.hpp"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::CXformGet2TableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoin2HashJoin::CXformInnerJoin2HashJoin()
	: CXformImplementation(make_uniq<LogicalComparisonJoin>(JoinType::INNER))
{
	this->m_operator->AddChild(make_uniq<CPatternLeaf>());
	this->m_operator->AddChild(make_uniq<CPatternLeaf>());
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::XformPromise
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformInnerJoin2HashJoin::XformPromise(CExpressionHandle &exprhdl) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformInnerJoin2HashJoin::Transform(duckdb::unique_ptr<CXformContext> pxfctxt,
										 duckdb::unique_ptr<CXformResult> pxfres,
										 duckdb::unique_ptr<Operator> pexpr) const
{
	auto popJoin = unique_ptr_cast<Operator, LogicalComparisonJoin>(pexpr);
    PerfectHashJoinStats perfect_join_stats;
	duckdb::vector<JoinCondition> v;
	// Need to delete
	// for(auto &child : popJoin->conditions) {
	for(auto child : popJoin->conditions) {
		JoinCondition jc;
		// Need to delete
		// jc.left = child.left->Copy();
		// jc.right = child.right->Copy();
		jc.left = child.left;
		jc.right = child.right;
		jc.comparison = child.comparison;
		v.push_back(std::move(jc));
	}
	// create alternative expression
	// Need to delete
	// duckdb::unique_ptr<PhysicalHashJoin> pexprAlt = make_uniq<PhysicalHashJoin>(*popJoin, popJoin->children[0]->Copy(), popJoin->children[1]->Copy(), 
    duckdb::unique_ptr<PhysicalHashJoin> pexprAlt = make_uniq<PhysicalHashJoin>(*popJoin, popJoin->children[0], popJoin->children[1], 
	                                                                     		std::move(v), popJoin->join_type,
                                                                        		popJoin->left_projection_map, popJoin->right_projection_map,
                                                                        		popJoin->delim_types, popJoin->estimated_cardinality,
																				perfect_join_stats);
	// Cardinality Estimation
	pexprAlt->CE();
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}