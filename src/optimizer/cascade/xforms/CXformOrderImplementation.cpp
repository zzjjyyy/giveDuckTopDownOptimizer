//---------------------------------------------------------------------------
//	@filename:
//		CXformOrderImplementation.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformOrderImplementation.h"

#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/operator/logical_order.hpp"

namespace gpopt {

//---------------------------------------------------------------------------
//	@function:
//		CXformOrderImplementation::CXformOrderImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformOrderImplementation::CXformOrderImplementation()
    : CXformImplementation(make_uniq<LogicalOrder>(duckdb::vector<BoundOrderByNode>())) {
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
CXform::EXformPromise CXformOrderImplementation::XformPromise(CExpressionHandle &handle) const {
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
void CXformOrderImplementation::Transform(duckdb::unique_ptr<CXformContext> pxfctxt,
				   						  duckdb::unique_ptr<CXformResult> result,
				   						  duckdb::unique_ptr<Operator> op) const {
	D_ASSERT(op->children.size() == 1);
	// Need to delete
	// auto child = op->children[0]->Copy();
	auto child = op->children[0];
	auto order = unique_ptr_cast<Operator, LogicalOrder>(op);
	if (!order->orders.empty()) {
		// projection based on children's output.
		duckdb::vector<idx_t> projections;
		if (order->projections.empty()) {
			for (idx_t i = 0; i < child->types.size(); i++) {
				projections.push_back(i);
			}
		} else {
			projections = order->projections;
		}

		// create physical order
		duckdb::vector<BoundOrderByNode> orders;
		for (auto &order_node : order->orders) {
			orders.push_back(order_node);
		}

		auto physical_order = make_uniq<PhysicalOrder>(order->types, std::move(orders), std::move(projections),
		                                               order->estimated_cardinality);
		physical_order->is_enforced = false;
		physical_order->AddChild(std::move(child));
		result->Add(std::move(physical_order));
	} else {
		result->Add(std::move(child));
	}
}
} // namespace gpopt