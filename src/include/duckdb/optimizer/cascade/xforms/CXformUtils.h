//---------------------------------------------------------------------------
//	@filename:
//		CXformUtils.h
//
//	@doc:
//		
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformUtils_H
#define GPOPT_CXformUtils_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"

namespace gpopt
{
using namespace gpos;

class CXformUtils
{
public:
    static duckdb::unique_ptr<Operator> PexprPushGbBelowJoin(Operator *expr);

    static bool IsPK(duckdb::vector<ColumnBinding> v, Operator *m_operator);

    static bool FCanPushGbAggBelowJoin(duckdb::vector<ColumnBinding> pcrsGrpCols,
									   duckdb::vector<ColumnBinding> pcrsJoinOuterChildOutput,
									   duckdb::vector<ColumnBinding> pcrsJoinScalarUsedFromOuter,
									   duckdb::vector<ColumnBinding> pcrsGrpByOutput,
									   duckdb::vector<ColumnBinding> pcrsGrpByUsed,
									   duckdb::vector<ColumnBinding> pcrsFKey);

    static duckdb::unique_ptr<LogicalAggregate> PopGbAggPushableBelowJoin(LogicalAggregate *popGbAggOld,
									   									  duckdb::vector<ColumnBinding> pcrsOutputOuter,
									   									  duckdb::vector<ColumnBinding> pcrsGrpCols);
};

}
#endif