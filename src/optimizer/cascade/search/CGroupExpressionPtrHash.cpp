#include "duckdb/optimizer/cascade/search/CGroupExpressionPtrHash.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpopt {
std::hash<CGroupExpression*> hash_CGroupExpressionPtr;

size_t CGroupExpressionPtrHash::operator()(const duckdb::unique_ptr<CGroupExpression> gexpr) const {
		// uint64_t add = reinterpret_cast<uint64_t>(gexpr.get());
		size_t ulHash = hash_CGroupExpressionPtr(gexpr.get());
		return ulHash;
	}
}