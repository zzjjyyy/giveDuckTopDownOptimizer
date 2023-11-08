//---------------------------------------------------------------------------
//	@filename:
//		CXformResult.cpp
//
//	@doc:
//		Implementation of result container
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformResult.h"

#include "duckdb/optimizer/cascade/base.h"

namespace gpopt {

//---------------------------------------------------------------------------
//	@function:
//		CXformResult::Add
//
//	@doc:
//		add alternative
//
//---------------------------------------------------------------------------
void CXformResult::Add(duckdb::unique_ptr<Operator> expression) {
	// Need to delete
	// m_alternative_expressions.push_back(std::move(expression));
	m_alternative_expressions.push_back(expression);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformResult::NextExpression
//
//	@doc:
//		retrieve next alternative
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<Operator> CXformResult::NextExpression() {
	duckdb::unique_ptr<Operator> expression = nullptr;
	if (m_expression < m_alternative_expressions.size()) {
		// Need to delete
		// expression = std::move(m_alternative_expressions[m_expression]);
		expression = m_alternative_expressions[m_expression];
	}
	m_expression++;
	return expression;
}
} // namespace gpopt