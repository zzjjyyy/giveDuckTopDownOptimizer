//---------------------------------------------------------------------------
//	@filename:
//		CExpressionHandle.h
//
//	@doc:
//		Handle to convey context wherever an expression is used in a shallow
//		context, i.e. operator and the properties of its children but no
//		access to the children is needed.
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionHandle_H
#define GPOPT_CExpressionHandle_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"
#include "duckdb/optimizer/cascade/base/CDerivedProperty.h"
#include "duckdb/optimizer/cascade/base/CRequiredProperty.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/planner/logical_operator.hpp"

namespace gpopt {
// fwd declaration
class CDerivedPhysicalProp;
class CPropConstraint;
class CCostContext;

using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@class:
//		CExpressionHandle
//
//	@doc:
//		Context for expression; abstraction for group expressions and
//		stand-alone expressions/DAGs;
//		a handle is attached to either an expression or a group expression
//
//---------------------------------------------------------------------------
class CExpressionHandle {
	friend class CExpression;

private:
	// attached expression
	duckdb::unique_ptr<Operator> m_pop;
	// attached expression
	duckdb::unique_ptr<Expression> m_expr;
	// attached group expression
	duckdb::unique_ptr<CGroupExpression> m_pgexpr;
	// attached cost context
	duckdb::unique_ptr<CCostContext> m_pcc;
	// derived plan properties of the gexpr attached by a CostContext under
	// the default CDrvdPropCtxtPlan. See DerivePlanPropsForCostContext()
	// NB: does NOT support on-demand property derivation
	duckdb::unique_ptr<CDerivedProperty> m_derived_prop_pplan;
	// required properties of attached expr/gexpr;
	// set during required property computation
	duckdb::unique_ptr<CRequiredProperty> m_required_property;
	// array of children's required properties
	duckdb::vector<duckdb::unique_ptr<CRequiredProperty>> m_children_required_properties;

public:
	// return an array of stats objects starting from the first stats object referenced by child
	// IStatisticsArray *PdrgpstatOuterRefs(IStatisticsArray *statistics_array, ULONG child_index);

	// check if stats are derived for attached expression and its children
	// bool FStatsDerived() const;

	// copy stats from attached expression/group expression to local stats members
	// void CopyStats();

	// return True if handle is attached to a leaf pattern
	bool FAttachedToLeafPattern() const;

	// stat derivation at root operator where handle is attached
	// void DeriveRootStats(IStatisticsArray *stats_ctxt);

public:
	// ctor
	explicit CExpressionHandle();

	// private copy ctor
	CExpressionHandle(const CExpressionHandle &) = delete;

	// dtor
	~CExpressionHandle();

public:
	// attach handle to a given operator tree
	void Attach(duckdb::unique_ptr<Operator> pop);

	// attach handle to a given expression
	void Attach(duckdb::unique_ptr<Expression> expr);

	// attach handle to a given group expression
	void Attach(duckdb::unique_ptr<CGroupExpression> pgexpr);

	// attach handle to a given cost context
	void Attach(duckdb::unique_ptr<CCostContext> pcc);

public:
	// recursive property derivation,
	void DeriveProps(duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxt);

	// derive the properties of the plan carried by attached cost context,
	// using default CDrvdPropCtxtPlan
	void DerivePlanPropsForCostContext();

	// initialize required properties container
	void InitReqdProps(duckdb::unique_ptr<CRequiredProperty> prpInput);

	// compute required properties of the n-th child
	void ComputeChildReqdProps(ULONG child_index, duckdb::vector<duckdb::unique_ptr<CDerivedProperty>> derived_property_children, ULONG num_opt_request);

	// copy required properties of the n-th child
	void CopyChildReqdProps(ULONG child_index, duckdb::unique_ptr<CRequiredProperty> prp);

	// compute required columns of the n-th child
	void ComputeChildReqdCols(ULONG child_index, duckdb::vector<duckdb::unique_ptr<CDerivedProperty>> pdrgpdpCtxt);

	// required properties computation of all children
	void ComputeReqdProps(duckdb::unique_ptr<CRequiredProperty> prpInput, ULONG ulOptReq);

	// derived relational props of n-th child
	duckdb::unique_ptr<CDerivedLogicalProp> GetRelationalProperties(ULONG child_index) const;

	// derived plan props of n-th child
	duckdb::unique_ptr<CDerivedPhysicalProp> Pdpplan(ULONG child_index) const;

	// derived properties of attached expr/gexpr
	duckdb::unique_ptr<CDerivedProperty> DerivedProperty() const;

	// derived relational properties of attached expr/gexpr
	duckdb::unique_ptr<CDerivedLogicalProp> GetRelationalProperties() const;

	// check if given child is a scalar
	bool FScalarChild(ULONG child_index) const;

	// required relational props of n-th child
	duckdb::unique_ptr<CRequiredLogicalProp> GetReqdRelationalProps(ULONG child_index) const;

	// required plan props of n-th child
	duckdb::unique_ptr<CRequiredPhysicalProp> RequiredPropPlan(ULONG child_index) const;

	// arity function
	ULONG Arity(int x = 0) const;

	// index of the last non-scalar child
	ULONG UlLastNonScalarChild() const;

	// index of the first non-scalar child
	ULONG UlFirstNonScalarChild() const;

	// number of non-scalar children
	ULONG UlNonScalarChildren() const;

	// accessor for operator
	duckdb::unique_ptr<Operator> Pop() const;

	// accessor for child operator
	duckdb::unique_ptr<Operator> Pop(ULONG child_index) const;

	// accessor for grandchild operator
	duckdb::unique_ptr<Operator>
	PopGrandchild(ULONG child_index,
				  ULONG grandchild_index,
				  duckdb::unique_ptr<CCostContext> *grandchildContext) const;

	// accessor for expression
	duckdb::unique_ptr<Expression> Pexpr() const {
		return m_expr;
	}

	// accessor for group expression
	duckdb::unique_ptr<CGroupExpression> group_expr() const {
		return m_pgexpr;
	}

	// check for outer references
	bool HasOuterRefs() {
		return (0 < DeriveOuterReferences().size());
	}

	// check for outer references in the given child
	bool HasOuterRefs(ULONG child_index) {
		return (0 < DeriveOuterReferences(child_index).size());
	}

	// get next child index based on child optimization order, return true if such index could be found
	bool FNextChildIndex(ULONG *pulChildIndex // output: index to be changed
	) const;

	// return the index of first child to be optimized
	ULONG UlFirstOptimizedChildIndex() const;

	// return the index of last child to be optimized
	ULONG UlLastOptimizedChildIndex() const;

	// return the index of child to be optimized next to the given child
	ULONG UlNextOptimizedChildIndex(ULONG child_index) const;

	// return the index of child optimized before the given child
	ULONG UlPreviousOptimizedChildIndex(ULONG child_index) const;

	// check whether an expression's children have a volatile function
	bool FChildrenHaveVolatileFuncScan();

	// return a representative (inexact) scalar child at given index
	duckdb::unique_ptr<Expression> PexprScalarRepChild(ULONG child_index) const;

	// return a representative (inexact) scalar expression attached to handle
	duckdb::unique_ptr<Expression> PexprScalarRep() const;

	// return an exact scalar child at given index or return null if not possible
	duckdb::unique_ptr<Expression> PexprScalarExactChild(ULONG child_index, bool error_on_null_return = false) const;

	// return an exact scalar expression attached to handle or null if not possible
	duckdb::unique_ptr<Expression> PexprScalarExact() const;

	// return the columns used by a logical operator internally as well
	// as columns used by all its scalar children
	duckdb::vector<ColumnBinding> PcrsUsedColumns();

	duckdb::vector<ColumnBinding> DeriveOuterReferences();
	duckdb::vector<ColumnBinding> DeriveOuterReferences(ULONG child_index);

	duckdb::vector<ColumnBinding> DeriveOutputColumns();
	duckdb::vector<ColumnBinding> DeriveOutputColumns(ULONG child_index);

	duckdb::vector<ColumnBinding> DeriveNotNullColumns();
	duckdb::vector<ColumnBinding> DeriveNotNullColumns(ULONG child_index);

	bool DeriveHasSubquery() {
		return false;
	}
	bool DeriveHasSubquery(ULONG child_index) {
		return false;
	}

	duckdb::vector<ColumnBinding> DeriveCorrelatedApplyColumns();
	duckdb::vector<ColumnBinding> DeriveCorrelatedApplyColumns(ULONG child_index);

	duckdb::unique_ptr<CKeyCollection> DeriveKeyCollection();
	duckdb::unique_ptr<CKeyCollection> DeriveKeyCollection(ULONG child_index);

	duckdb::unique_ptr<CPropConstraint> DerivePropertyConstraint();
	duckdb::unique_ptr<CPropConstraint> DerivePropertyConstraint(ULONG child_index);

	ULONG DeriveJoinDepth();
	ULONG DeriveJoinDepth(ULONG child_index);

	duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> Pdrgpfd();
	duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> Pdrgpfd(ULONG child_index);
}; // class CExpressionHandle

} // namespace gpopt

#endif