//---------------------------------------------------------------------------
//	@filename:
//		CUtils.h
//
//	@doc:
//		Function for convenience
//---------------------------------------------------------------------------
#ifndef GPOPT_CUTILS_H
#define GPOPT_CUTILS_H

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace gpopt
{
using namespace duckdb;
using namespace gpos;

class CUtils
{
public:
    //---------------------------------------------------------------------------
    //	@function:
    //		CUtils::IsDisjoint
    //
    //	@doc:
    //		Determine if disjoint
    //
    //---------------------------------------------------------------------------
    static bool
    IsDisjoint(duckdb::vector<ColumnBinding> pcrs1,
               duckdb::vector<ColumnBinding> pcrs2);
    
    // add an equivalence class (col ref set) to the array. If the new equiv
	// class contains columns from existing equiv classes, then these are merged
    static duckdb::vector<duckdb::vector<ColumnBinding>>
    AddEquivClassToArray(duckdb::vector<ColumnBinding> pcrsNew,
                         duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrs);

    // merge 2 arrays of equivalence classes
	static duckdb::vector<duckdb::vector<ColumnBinding>>
    PdrgpcrsMergeEquivClasses(duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsFst,
                              duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrsSnd);

    static bool
    Equals(duckdb::vector<ColumnBinding> first,
           duckdb::vector<ColumnBinding> second);

    static bool
    ContainsAll(duckdb::vector<ColumnBinding> first,
                duckdb::vector<ColumnBinding> second);

    static bool
    ContainsAll(duckdb::vector<BoundOrderByNode> &parent,
                duckdb::vector<BoundOrderByNode> &child);

    // check if a given operator is an enforcer
	static bool FEnforcer(duckdb::unique_ptr<Operator> pop);

    // return the number of occurrences of the given expression in the given
	// array of expressions
	static ULONG
    UlOccurrences(duckdb::unique_ptr<Operator> pexpr,
                  duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr);

    static bool
    FMatchChildrenOrdered(duckdb::unique_ptr<Operator> pexprLeft,
                          duckdb::unique_ptr<Operator> pexprRight);

    static bool
    FMatchChildrenUnordered(duckdb::unique_ptr<Operator> pexprLeft,
                            duckdb::unique_ptr<Operator> pexprRight);

    static bool
    Equals(duckdb::unique_ptr<Operator> pexprLeft,
           duckdb::unique_ptr<Operator> pexprRight);

    static bool
    Equals(duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexprLeft,
           duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexprRight);

    // check existence of subqueries or Apply operators in deep expression tree
    static bool
    FHasSubquery(duckdb::unique_ptr<Operator> pexpr,
                 bool fCheckRoot);

    // comparison function for pointers
	static int PtrCmp(const duckdb::unique_ptr<void> p1, const duckdb::unique_ptr<void> p2)
	{
		ULONG_PTR ulp1 = *(ULONG_PTR*) p1.get();
		ULONG_PTR ulp2 = *(ULONG_PTR*) p2.get();
		if (ulp1 < ulp2)
		{
			return -1;
		}
		else if (ulp1 > ulp2)
		{
			return 1;
		}
		return 0;
	}

    template<class T>
    static int SharedPtrCmp(shared_ptr<T> p1, shared_ptr<T> p2)
    {
        if (p1.get() < p2.get())
		{
			return -1;
		}
		else if (p1.get() > p2.get())
		{
			return 1;
		}
		return 0;
    }

    static bool FPhysicalJoin(Operator* op) {
        if(op->physical_type == PhysicalOperatorType::IE_JOIN
           || op->physical_type == PhysicalOperatorType::HASH_JOIN
           || op->physical_type == PhysicalOperatorType::DELIM_JOIN
           || op->physical_type == PhysicalOperatorType::INDEX_JOIN
           || op->physical_type == PhysicalOperatorType::POSITIONAL_JOIN
           || op->physical_type == PhysicalOperatorType::NESTED_LOOP_JOIN
           || op->physical_type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
           || op->physical_type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN) {
            return true;
        }
        return false;
    }
};
}
#endif