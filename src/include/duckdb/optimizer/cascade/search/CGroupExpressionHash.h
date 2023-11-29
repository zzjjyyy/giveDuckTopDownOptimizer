//---------------------------------------------------------------------------
//	@filename:
//		CGroupExpressionHash.h
//
//	@doc:
//		Equivalent of CExpression inside Memo structure
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"

namespace gpopt {
class CGroupExpression;

class CGroupExpressionHash {
public:
	size_t operator()(const duckdb::unique_ptr<CGroupExpression> gexpr) const;
};
}