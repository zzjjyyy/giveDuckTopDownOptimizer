//---------------------------------------------------------------------------
//	@filename:
//		CGroupExpressionPtrHash.h
//
//	@doc:
//		
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"

namespace gpopt {
class CGroupExpression;

class CGroupExpressionPtrHash {
public:
	size_t operator()(const duckdb::unique_ptr<CGroupExpression> gexpr) const;
};
}