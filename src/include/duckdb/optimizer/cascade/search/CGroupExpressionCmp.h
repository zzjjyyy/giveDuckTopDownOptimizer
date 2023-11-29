//---------------------------------------------------------------------------
//	@filename:
//		CGroupExpressionCmp.h
//
//	@doc:
//		Equivalent of CExpression inside Memo structure
//---------------------------------------------------------------------------
#pragma once

#include "duckdb/optimizer/cascade/base.h"

namespace gpopt{
class CGroupExpression;

class CGroupExpressionCmp
{
public:
	size_t operator()(const duckdb::unique_ptr<CGroupExpression> gexpr1,
                      const duckdb::unique_ptr<CGroupExpression> gexpr2) const;
};
}