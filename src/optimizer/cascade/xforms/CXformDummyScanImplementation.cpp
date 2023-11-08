//
// Created by admin on 8/13/23.
//

#include "duckdb/optimizer/cascade/xforms/CXformDummyScanImplementation.h"

#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace gpopt {
CXform::EXformPromise
CXformDummyScanImplementation::XformPromise(CExpressionHandle &expression_handle) const {
	return CXform::ExfpHigh;
}

void CXformDummyScanImplementation::Transform(duckdb::unique_ptr<CXformContext> xform_context,
											  duckdb::unique_ptr<CXformResult> xform_result,
                                              duckdb::unique_ptr<Operator> expression) const {
	auto logical_dummy_scan = unique_ptr_cast<Operator, LogicalDummyScan>(expression);
	auto alternative_expression =
	    make_uniq<PhysicalDummyScan>(logical_dummy_scan->types, logical_dummy_scan->estimated_cardinality);
	alternative_expression->v_column_binding = logical_dummy_scan->GetColumnBindings();
	xform_result->Add(std::move(alternative_expression));
}
} // namespace gpopt
