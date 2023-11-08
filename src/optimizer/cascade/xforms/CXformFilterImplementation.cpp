//---------------------------------------------------------------------------
//	@filename:
//		CXformFilterImplementation.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformFilterImplementation.h"

#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CXformFilterImplementation::CXformFilterImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformFilterImplementation::CXformFilterImplementation() : CXformImplementation(make_uniq<LogicalFilter>()) {
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
CXform::EXformPromise
CXformFilterImplementation::XformPromise(CExpressionHandle &expression_handle) const {
	return CXform::ExfpMedium;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformFilterImplementation::Transform(duckdb::unique_ptr<CXformContext> xform_context,
										   duckdb::unique_ptr<CXformResult> xform_result,
                                           duckdb::unique_ptr<Operator> expression) const {
	auto operator_filter = unique_ptr_cast<Operator, LogicalFilter>(expression);
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for (auto &child : operator_filter->expressions) {
		// Need to delete
		// v.push_back(child->Copy());
		v.push_back(child);
	}
	// create alternative expression
	duckdb::unique_ptr<PhysicalFilter> alternative_expression =
	    make_uniq<PhysicalFilter>(operator_filter->types, std::move(v), operator_filter->estimated_cardinality);
	// Need to delete
	// for (auto &child : expression->children) {
	for (auto child : expression->children) {
		// Need to delete
		// alternative_expression->AddChild(child->Copy());
		alternative_expression->AddChild(child);
	}
	// Cardinality Estimation
	alternative_expression->CE();
	// add alternative to transformation result
	xform_result->Add(std::move(alternative_expression));
}
} // namespace gpopt