#include "duckdb/optimizer/cascade/search/CGroupExpressionPtrCmp.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpopt {
size_t CGroupExpressionPtrCmp::operator()(const duckdb::unique_ptr<CGroupExpression> gexpr1,
                                          const duckdb::unique_ptr<CGroupExpression> gexpr2) const {
		return gexpr1.get() == gexpr2.get();
	}
}