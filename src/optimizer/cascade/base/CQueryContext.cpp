//---------------------------------------------------------------------------
//	@filename:
//		CQueryContext.cpp
//
//	@doc:
//		Implementation of optimization context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CQueryContext.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/planner/operator/logical_order.hpp"

namespace gpopt {
using namespace duckdb;
//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::CQueryContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CQueryContext::CQueryContext(duckdb::unique_ptr<Operator> expr, CRequiredPropPlan *property,
                             duckdb::vector<ColumnBinding> col_ids, duckdb::vector<std::string> col_names,
                             bool derive_stats)
    : m_required_plan_property(property), m_required_output_cols(col_ids), m_derivation_stats(derive_stats) {
	duckdb::vector<ColumnBinding> output_and_ordering_cols;
	duckdb::vector<ColumnBinding> order_spec = property->m_sort_order->m_sort_order->PcrsUsed();
	output_and_ordering_cols.insert(output_and_ordering_cols.end(), col_ids.begin(), col_ids.end());
	output_and_ordering_cols.insert(output_and_ordering_cols.end(), order_spec.begin(), order_spec.end());
	for (auto &child : col_names) {
		m_output_col_names.push_back(child);
	}
	/* I comment here */
	// m_expr = CExpressionPreprocessor::PexprPreprocess(expr, output_and_ordering_cols);
	m_expr = std::move(expr);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::~CQueryContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CQueryContext::~CQueryContext() {
	// m_expr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::PopTop
//
//	@doc:
// 		 Return top level operator in the given expression
//
//---------------------------------------------------------------------------
LogicalOperator *CQueryContext::PopTop(LogicalOperator *pexpr) {
	// skip CTE anchors if any
	LogicalOperator *pexprCurr = pexpr;
	while (LogicalOperatorType::LOGICAL_CTE_REF == pexprCurr->logical_type) {
		pexprCurr = (LogicalOperator *)pexprCurr->children[0].get();
	}
	return pexprCurr;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::QueryContextGenerate
//
//	@doc:
// 		Generate the query context for the given expression and array of
//		output column ref ids
//
//---------------------------------------------------------------------------
CQueryContext *CQueryContext::QueryContextGenerate(duckdb::unique_ptr<Operator> expr, duckdb::vector<ULONG *> col_ids,
                                                   duckdb::vector<std::string> col_names, bool derive_stats) {
	duckdb::vector<ColumnBinding> required_cols;
	duckdb::vector<ColumnBinding> sort_order;
	// Collect required properties (property_plan) at the top level:
	COrderSpec *pos = new COrderSpec();
	// Ensure order meet 'satisfy' matching at the top level
	if (LogicalOperatorType::LOGICAL_ORDER_BY == expr->logical_type) {
		duckdb::unique_ptr<LogicalOrder> logical_order = unique_ptr_cast<Operator, LogicalOrder>(std::move(expr));
		// top level operator is an order by, copy order spec to query context
		for (auto &child : logical_order->orders) {
			pos->orderby_node.emplace_back(child.type, child.null_order, child.expression->Copy());
		}
		expr = std::move(logical_order->children[0]);
	}
	COrderProperty *order_property = new COrderProperty(pos, COrderProperty::EomSatisfy);
	CRequiredPropPlan *property_plan = new CRequiredPropPlan(required_cols, order_property);
	// Finally, create the CQueryContext
	return new CQueryContext(std::move(expr), property_plan, sort_order, col_names, derive_stats);
}
} // namespace gpopt