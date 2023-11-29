#include "duckdb/optimizer/cascade/search/CGroupExpressionHash.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpopt {
size_t CGroupExpressionHash::operator()(const duckdb::unique_ptr<CGroupExpression> gexpr) const {
		size_t ulHash = gexpr->m_operator->HashValue();
		size_t arity = gexpr->m_child_groups.size();
		for (size_t i = 0; i < arity; i++) {
			ulHash = CombineHashes(ulHash, gexpr->m_child_groups[i]->HashValue());
		}
		return ulHash;
	}
}