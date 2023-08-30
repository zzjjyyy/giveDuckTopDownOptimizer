//---------------------------------------------------------------------------
//	@filename:
//		CQueryContext.h
//
//	@doc:
//		A container for query-specific input objects to the optimizer
//---------------------------------------------------------------------------
#ifndef CQueryContext_H
#define CQueryContext_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropPlan.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropRelational.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/planner/logical_operator.hpp"

using namespace gpos;
using namespace duckdb;

namespace gpopt {
//---------------------------------------------------------------------------
//	@class:
//		CQueryContext
//
//	@doc:
//		Query specific information that optimizer receives as input
//		representing the requirements that need to be satisfied by the final
//		plan. This includes:
//		- Input logical expression
//		- Required columns
//		- Required plan (physical) properties at the top level of the query.
//		  This will include sort order, rewindability, etc requested by the entire
//		  query.
//
//		The function CQueryContext::QueryContextGenerate() is the main routine that
//		generates a query context object for a given logical expression and
//		required output columns. See there for more details of how
//		CQueryContext is constructed.
//
//		NB: One instance of CQueryContext is created per query. It is then used
//		to initialize the CEngine.
//
//
//---------------------------------------------------------------------------
class CQueryContext {
public:
	CQueryContext(duckdb::unique_ptr<Operator> expr, CRequiredPropPlan *property, duckdb::vector<ColumnBinding> col_ids,
	              duckdb::vector<std::string> col_names, bool derive_stats);
	CQueryContext(const CQueryContext &) = delete;
	virtual ~CQueryContext();

	// required plan properties in optimizer's produced plan
	CRequiredPropPlan *m_required_plan_property;

	// required array of output columns
	duckdb::vector<ColumnBinding> m_required_output_cols;
	// array of output column names
	duckdb::vector<std::string> m_output_col_names;
	// logical expression tree to be optimized
	duckdb::unique_ptr<Operator> m_expr;
	// should statistics derivation take place
	bool m_derivation_stats;

	// return top level operator in the given expression
	static LogicalOperator *PopTop(LogicalOperator *expr);

public:
	bool FDeriveStats() const {
		return m_derivation_stats;
	}

	//---------------------------------------------------------------------------
	//  @function:
	//    QueryContextGenerate
	//
	//  @doc:
	//    Generate the query context for the given expression and array of output column ref ids.
	//
	//  @inputs:
	//    Operator* expr, expression representing the query
	//    ULongPtrArray* col_ids, array of output column reference id
	//    CMDNameArray* col_names, array of output column names
	//
	//  @output:
	//    CQueryContext
	//
	//---------------------------------------------------------------------------
	static CQueryContext *QueryContextGenerate(duckdb::unique_ptr<Operator> expr, duckdb::vector<ULONG *> col_ids,
	                                           duckdb::vector<std::string> col_names, bool derive_stats);
}; // class CQueryContext
} // namespace gpopt
#endif
