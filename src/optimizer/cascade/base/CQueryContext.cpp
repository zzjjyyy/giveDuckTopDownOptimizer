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
#include "duckdb/planner/operator/logical_projection.hpp"

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
CQueryContext::CQueryContext(duckdb::unique_ptr<Operator> expr, CRequiredPhysicalProp *property,
                             duckdb::vector<ColumnBinding> col_ids, duckdb::vector<std::string> col_names,
                             bool derive_stats)
    : m_required_plan_property(property), m_required_output_cols(col_ids), m_derivation_stats(derive_stats) {
	duckdb::vector<ColumnBinding> output_and_ordering_cols;
	duckdb::vector<ColumnBinding> order_spec = property->m_sort_order->m_order_spec->PcrsUsed();
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
	COrderSpec *spec = new COrderSpec();

	// remove orderbys in this logical plan, and add order requirements to the order spec.
	if (expr->logical_type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		LogicalOrder *order = (LogicalOrder *)expr.get();
		for (auto &child : order->orders)
			spec->order_nodes.emplace_back(child.type, child.null_order, child.expression->Copy());
		expr = std::move(expr->children[0]);
	}
	//	for (size_t i = 0; i < expr->children.size(); i++)
	//		RemoveOrderBy(spec, expr.get(), expr->children[i].get(), i);
	//  Printer::Print("Logical Plan Without OrderBy\n");
	//	((LogicalOperator *)expr.get())->Print();

	// construct the physical property
	COrderProperty *order_property = new COrderProperty(spec, COrderProperty::EomSatisfy);
	CRequiredPhysicalProp *physical_property = new CRequiredPhysicalProp(required_cols, order_property);

	return new CQueryContext(std::move(expr), physical_property, sort_order, col_names, derive_stats);
}

void CQueryContext::RemoveOrderBy(COrderSpec *spec, Operator *parent, Operator *op, size_t child_idx) {
	if (LogicalOperatorType::LOGICAL_ORDER_BY == op->logical_type) {
		D_ASSERT(op->children.size() == 1);
		auto orderby = (LogicalOrder *)op;
		for (auto &it : orderby->orders) {
			spec->order_nodes.emplace_back(it.type, it.null_order, it.expression->Copy());
		}
		parent->children[child_idx] = std::move(orderby->children[0]);
		op = parent->children[child_idx].get();
	}

	for (size_t i = 0; i < op->children.size(); i++)
		RemoveOrderBy(spec, op, op->children[i].get(), i);
}
} // namespace gpopt