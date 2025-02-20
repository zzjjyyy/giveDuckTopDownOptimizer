//---------------------------------------------------------------------------
//	@filename:
//		CExpressionHandle.cpp
//
//	@doc:
//		Handle to an expression to abstract topology;
//
//		The handle provides access to an expression and the properties
//		of its children; regardless of whether the expression is a group
//		expression or a stand-alone tree;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CRequiredPhysicalProp.h"
#include "duckdb/optimizer/cascade/base/CRequiredPropRelational.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"
#include "duckdb/planner/logical_operator.hpp"

#include <assert.h>

using namespace duckdb;
using namespace std;

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CExpressionHandle
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CExpressionHandle::CExpressionHandle()
    : m_pop(nullptr), m_expr(nullptr), m_pgexpr(nullptr), m_pcc(nullptr), m_derived_prop_pplan(nullptr),
      m_required_property(nullptr) {
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::~CExpressionHandle
//
//	@doc:
//		dtor
//
//		Since handles live on the stack this dtor will be called during
//		exceptions, hence, need to be defensive
//
//---------------------------------------------------------------------------
CExpressionHandle::~CExpressionHandle() {
	m_pgexpr = nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given operator tree
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(Operator *pop) {
	m_pop = pop;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given expression
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(Expression *pexpr) {
	m_expr = pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given group expression
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(CGroupExpression *pgexpr) {
	// increment ref count on group expression
	m_pgexpr = pgexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given cost context
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(CCostContext *pcc) {
	m_pcc = pcc;
	Attach(pcc->m_group_expression);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveProps
//
//	@doc:
//		Recursive property derivation
//
//---------------------------------------------------------------------------
void CExpressionHandle::DeriveProps(CDerivedPropertyContext *pdpctxt) {
	if (nullptr != m_pgexpr) {
		return;
	}
	if (nullptr != m_pop->Pdp(m_pop->Ept())) {
		return;
	}
	// copy stats of attached expression
	// CopyStats();
	m_pop->PdpDerive((CDrvdPropCtxtPlan *)pdpctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FAttachedToLeafPattern
//
//	@doc:
//		Return True if handle is attached to a leaf pattern
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FAttachedToLeafPattern() const {
	return 0 == Arity() && nullptr != m_pop && nullptr != m_pop->m_group_expression;
}

// Derive the properties of the plan carried by attached cost context.
// Note that this re-derives the plan properties, instead of using those
// present in the gexpr, for cost contexts only and under the default
// CDrvdPropCtxtPlan.
// On the other hand, the properties in the gexpr may have been derived in
// other non-default contexts (e.g with cte info).
void CExpressionHandle::DerivePlanPropsForCostContext() {
	CDrvdPropCtxtPlan *pdpctxtplan = new CDrvdPropCtxtPlan();
	// CopyStats();
	// create/derive local properties
	m_derived_prop_pplan = m_pgexpr->m_operator->CreateDerivedProperty();
	m_derived_prop_pplan->Derive(*this, pdpctxtplan);
	delete pdpctxtplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::InitReqdProps
//
//	@doc:
//		Init required properties containers
//
//
//---------------------------------------------------------------------------
void CExpressionHandle::InitReqdProps(CRequiredProperty *prpInput) {
	// set required properties of attached expr/gexpr
	m_required_property = prpInput;
	// compute required properties of children
	// initialize array with input requirements,
	// the initial requirements are only place holders in the array
	// and they are replaced when computing the requirements of each child
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		m_children_required_properties.push_back(m_required_property);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeChildReqdProps
//
//	@doc:
//		Compute required properties of the n-th child
//---------------------------------------------------------------------------
void CExpressionHandle::ComputeChildReqdProps(ULONG child_index,
                                              duckdb::vector<CDerivedProperty *> derived_property_children,
                                              ULONG num_opt_request) {
	// compute required properties based on child type
	CRequiredProperty *property = Pop()->CreateRequiredProperty();
	property->Compute(*this, m_required_property, child_index, derived_property_children, num_opt_request);
	// replace required properties of given child
	m_children_required_properties[child_index] = property;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CopyChildReqdProps
//
//	@doc:
//		Copy required properties of the n-th child
//
//---------------------------------------------------------------------------
void CExpressionHandle::CopyChildReqdProps(ULONG child_index, CRequiredProperty *prp) {
	m_children_required_properties[child_index] = prp;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeChildReqdCols
//
//	@doc:
//		Compute required columns of the n-th child
//
//---------------------------------------------------------------------------
void CExpressionHandle::ComputeChildReqdCols(ULONG child_index, duckdb::vector<CDerivedProperty *> pdrgpdpCtxt) {
	CRequiredProperty *prp = Pop()->CreateRequiredProperty();
	CRequiredPhysicalProp::Prpp(prp)->ComputeReqdCols(*this, m_required_property, child_index, pdrgpdpCtxt);
	// replace required properties of given child
	m_children_required_properties[child_index] = prp;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeReqdProps
//
//	@doc:
//		Set required properties of attached expr/gexpr, and compute required
//		properties of all children
//
//---------------------------------------------------------------------------
void CExpressionHandle::ComputeReqdProps(CRequiredProperty *prpInput, ULONG ulOptReq) {
	InitReqdProps(prpInput);
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		duckdb::vector<CDerivedProperty *> v;
		ComputeChildReqdProps(ul, v, ulOptReq);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FScalarChild
//
//	@doc:
//		Check if a given child is a scalar expression/group
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FScalarChild(ULONG child_index) const {
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Arity
//
//	@doc:
//		Return number of children of attached expression/group expression
//		x = 0 for operator, x = 1 for expressions
//---------------------------------------------------------------------------
ULONG CExpressionHandle::Arity(int x) const {
	if (x == 0) {
		if (nullptr != Pop()) {
			return Pop()->Arity();
		}
	} else if (x == 1) {
		if (0 != Pop()->expressions.size()) {
			return Pop()->expressions.size();
		}
	}
	return m_pgexpr->Arity();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlLastNonScalarChild
//
//	@doc:
//		Return the index of the last non-scalar child. This is only valid if
//		Arity() is greater than 0
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlLastNonScalarChild() const {
	const ULONG arity = Arity();
	if (0 == arity) {
		return gpos::ulong_max;
	}

	ULONG ulLastNonScalarChild = arity - 1;
	while (0 < ulLastNonScalarChild && FScalarChild(ulLastNonScalarChild)) {
		ulLastNonScalarChild--;
	}

	if (!FScalarChild(ulLastNonScalarChild)) {
		// we need to check again that index points to a non-scalar child
		// since operator's children may be all scalar (e.g. index-scan)
		return ulLastNonScalarChild;
	}

	return gpos::ulong_max;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlFirstNonScalarChild
//
//	@doc:
//		Return the index of the first non-scalar child. This is only valid if
//		Arity() is greater than 0
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlFirstNonScalarChild() const {
	const ULONG arity = Arity();
	if (0 == arity) {
		return gpos::ulong_max;
	}

	ULONG ulFirstNonScalarChild = 0;
	while (ulFirstNonScalarChild < arity - 1 && FScalarChild(ulFirstNonScalarChild)) {
		ulFirstNonScalarChild++;
	}

	if (!FScalarChild(ulFirstNonScalarChild)) {
		// we need to check again that index points to a non-scalar child
		// since operator's children may be all scalar (e.g. index-scan)
		return ulFirstNonScalarChild;
	}

	return gpos::ulong_max;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlNonScalarChildren
//
//	@doc:
//		Return number of non-scalar children
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlNonScalarChildren() const {
	const ULONG arity = Arity();
	ULONG ulNonScalarChildren = 0;
	for (ULONG ul = 0; ul < arity; ul++) {
		if (!FScalarChild(ul)) {
			ulNonScalarChildren++;
		}
	}

	return ulNonScalarChildren;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetRelationalProperties
//
//	@doc:
//		Retrieve derived relational props of n-th child; Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CDerivedLogicalProp *CExpressionHandle::GetRelationalProperties(ULONG child_index) const {
	if (nullptr != Pop()) {
		// handle is used for required property computation
		if (Pop()->FPhysical()) {
			// relational props were copied from memo, return props directly
			return Pop()->children[child_index]->m_derived_logical_property;
		}
		// return props after calling derivation function
		return CDerivedLogicalProp::GetRelationalProperties(Pop()->children[child_index]->PdpDerive());
	}
	// handle is used for deriving plan properties, get relational props from child group
	CDerivedLogicalProp *drvdProps =
	    CDerivedLogicalProp::GetRelationalProperties((*m_pgexpr)[child_index]->m_derived_properties);
	return drvdProps;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetRelationalProperties
//
//	@doc:
//		Retrieve relational properties of attached expr/gexpr;
//
//---------------------------------------------------------------------------
CDerivedLogicalProp *CExpressionHandle::GetRelationalProperties() const {
	if (nullptr != Pop()) {
		if (Pop()->FPhysical()) {
			// relational props were copied from memo, return props directly
			CDerivedLogicalProp *drvdProps = Pop()->m_derived_logical_property;
			return drvdProps;
		}
		// return props after calling derivation function
		return CDerivedLogicalProp::GetRelationalProperties(Pop()->PdpDerive());
	}
	// get relational props from group
	CDerivedLogicalProp *drvdProps =
	    CDerivedLogicalProp::GetRelationalProperties(m_pgexpr->m_group->m_derived_properties);
	return drvdProps;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DrvdPlanProperty
//
//	@doc:
//		Retrieve derived plan props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CDerivedPhysicalProp *CExpressionHandle::Pdpplan(ULONG child_index) const {
	if (nullptr != m_pop) {
		return CDerivedPhysicalProp::DrvdPlanProperty(m_pop->children[child_index]->Pdp(CDerivedProperty::EptPlan));
	}
	CDerivedPhysicalProp *pdpplan = m_pcc->m_optimization_contexts[child_index]->m_best_cost_context->m_derived_prop_plan;
	return pdpplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetReqdRelationalProps
//
//	@doc:
//		Retrieve required relational props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CRequiredLogicalProp *CExpressionHandle::GetReqdRelationalProps(ULONG child_index) const {
	CRequiredProperty *prp = m_children_required_properties[child_index];
	return CRequiredLogicalProp::GetReqdRelationalProps(prp);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::RequiredPropPlan
//
//	@doc:
//		Retrieve required relational props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CRequiredPhysicalProp *CExpressionHandle::RequiredPropPlan(ULONG child_index) const {
	CRequiredProperty *prp = m_children_required_properties[child_index];
	return CRequiredPhysicalProp::Prpp(prp);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pop
//
//	@doc:
//		Get operator from handle
//
//---------------------------------------------------------------------------
Operator *CExpressionHandle::Pop() const {
	if (nullptr != m_pop) {
		return m_pop;
	}
	if (nullptr != m_pgexpr) {
		return m_pgexpr->m_operator.get();
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pop
//
//	@doc:
//		Get child operator from handle
//
//---------------------------------------------------------------------------
Operator *CExpressionHandle::Pop(ULONG child_index) const {
	if (nullptr != m_pop) {
		return m_pop->children[child_index].get();
	}
	if (nullptr != m_pcc) {
		COptimizationContext *pocChild = m_pcc->m_optimization_contexts[child_index];
		CCostContext *pccChild = pocChild->m_best_cost_context;
		return pccChild->m_group_expression->m_operator.get();
	}
	return nullptr;
}

Operator *CExpressionHandle::PopGrandchild(ULONG child_index, ULONG grandchild_index,
                                           CCostContext **grandchildContext) const {
	if (grandchildContext) {
		*grandchildContext = nullptr;
	}
	if (nullptr != m_pop) {
		if (nullptr != m_pop->children[child_index]) {
			return m_pop->children[child_index]->children[grandchild_index].get();
		}
		return nullptr;
	}
	if (nullptr != m_pcc) {
		COptimizationContext *pocChild = (m_pcc->m_optimization_contexts)[child_index];
		CCostContext *pccChild = pocChild->m_best_cost_context;
		COptimizationContext *pocGrandchild = (pccChild->m_optimization_contexts)[grandchild_index];
		if (nullptr != pocGrandchild) {
			CCostContext *pccgrandchild = pocGrandchild->m_best_cost_context;
			if (grandchildContext) {
				*grandchildContext = pccgrandchild;
			}
			return pccgrandchild->m_group_expression->m_operator.get();
		}
	}
	return nullptr;
}

//---------------------------------------------------------------------------
// CExpressionHandle::PexprScalarRepChild
//
// Get a representative (inexact) scalar child at given index. Subqueries
// in the child are replaced by a TRUE or nullptr constant. Use this method
// where exactness is not required, e. g. for statistics derivation,
// costing, or for heuristics.
//
//---------------------------------------------------------------------------
Expression *CExpressionHandle::PexprScalarRepChild(ULONG child_index) const {
	if (nullptr != m_pgexpr) {
		// access scalar expression cached on the child scalar group
		Expression *pexprScalar = (*m_pgexpr)[child_index]->m_scalar_expr;
		return pexprScalar;
	}
	if (nullptr != m_pop && nullptr != m_pop->children[child_index]->m_group_expression) {
		// access scalar expression cached on the child scalar group
		Expression *pexprScalar = m_pop->expressions[child_index].get();
		return pexprScalar;
	}
	if (nullptr != m_pop) {
		// if the expression does not come from a group, but its child does then
		// get the scalar child from that group
		CGroupExpression *pgexpr = m_pop->children[child_index]->m_group_expression;
		Expression *pexprScalar = pgexpr->m_group->m_scalar_expr;
		return pexprScalar;
	}
	// access scalar expression from the child expression node
	return m_pop->expressions[child_index].get();
}

//---------------------------------------------------------------------------
// CExpressionHandle::PexprScalarRep
//
// Get a representative scalar expression attached to handle,
// return nullptr if handle is not attached to a scalar expression.
// Note that this may be inexact if handle is attached to a
// CGroupExpression - subqueries will be replaced by a TRUE or nullptr
// constant. Use this method where exactness is not required, e. g.
// for statistics derivation, costing, or for heuristics.
//
//---------------------------------------------------------------------------
Expression *CExpressionHandle::PexprScalarRep() const {
	if (nullptr != m_expr) {
		return m_expr;
	}
	if (nullptr != m_pgexpr) {
		return m_pgexpr->m_group->m_scalar_expr;
	}
	return nullptr;
}

// return an exact scalar child at given index or return null if not possible
// (use this where exactness is required, e.g. for constraint derivation)
Expression *CExpressionHandle::PexprScalarExactChild(ULONG child_index, BOOL error_on_null_return) const {
	Expression *result_expr = nullptr;
	if (nullptr != m_pgexpr && !(*m_pgexpr)[child_index]->m_is_scalar_expr_exact) {
		result_expr = nullptr;
	} else if (nullptr != m_pop && nullptr != m_pop->children[child_index]->m_group_expression &&
	           !(m_pop->children[child_index]->m_group_expression->m_group->m_is_scalar_expr_exact)) {
		// the expression does not come from a group, but its child does and
		// the child group does not have an exact expression
		result_expr = nullptr;
	} else {
		result_expr = PexprScalarRepChild(child_index);
	}
	if (nullptr == result_expr && error_on_null_return) {
		assert(false);
	}
	return result_expr;
}

// return an exact scalar expression attached to handle or null if not possible
// (use this where exactness is required, e.g. for constraint derivation)
Expression *CExpressionHandle::PexprScalarExact() const {
	if (nullptr != m_pgexpr && !m_pgexpr->m_group->m_is_scalar_expr_exact) {
		return nullptr;
	}
	return PexprScalarRep();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FChildrenHaveVolatileFuncScan
//
//	@doc:
//		Check whether an expression's children have a volatile function
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FChildrenHaveVolatileFuncScan() {
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlFirstOptimizedChildIndex
//
//	@doc:
//		Return the index of first child to be optimized
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlFirstOptimizedChildIndex() const {
	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlLastOptimizedChildIndex
//
//	@doc:
//		Return the index of last child to be optimized
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlLastOptimizedChildIndex() const {
	const ULONG arity = Arity();
	return arity - 1;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlNextOptimizedChildIndex
//
//	@doc:
//		Return the index of child to be optimized next to the given child,
//		return gpos::ulong_max if there is no next child index
//
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlNextOptimizedChildIndex(ULONG child_index) const {
	ULONG ulNextChildIndex = gpos::ulong_max;
	if (Arity() - 1 > child_index) {
		ulNextChildIndex = child_index + 1;
	}
	return ulNextChildIndex;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlPreviousOptimizedChildIndex
//
//	@doc:
//		Return the index of child optimized before the given child,
//		return gpos::ulong_max if there is no previous child index
//
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlPreviousOptimizedChildIndex(ULONG child_index) const {
	ULONG ulPrevChildIndex = gpos::ulong_max;
	if (0 < child_index) {
		ulPrevChildIndex = child_index - 1;
	}
	return ulPrevChildIndex;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FNextChildIndex
//
//	@doc:
//		Get next child index based on child optimization order, return
//		true if such index could be found
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FNextChildIndex(ULONG *pulChildIndex) const {
	GPOS_ASSERT(nullptr != pulChildIndex);

	const ULONG arity = Arity();
	if (0 == arity) {
		// operator does not have children
		return false;
	}

	ULONG ulNextChildIndex = UlNextOptimizedChildIndex(*pulChildIndex);
	if (gpos::ulong_max == ulNextChildIndex) {
		return false;
	}
	*pulChildIndex = ulNextChildIndex;

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::PcrsUsedColumns
//
//	@doc:
//		Return the columns used by a logical operator and all its scalar children
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CExpressionHandle::PcrsUsedColumns() {
	duckdb::vector<ColumnBinding> cols = ((LogicalOperator *)Pop())->GetColumnBindings();
	return cols;
}

CDerivedProperty *CExpressionHandle::DerivedProperty() const {
	if (nullptr != m_pcc) {
		return m_derived_prop_pplan;
	}
	if (nullptr != m_pop) {
		return m_pop->Pdp(m_pop->Ept());
	}
	return m_pgexpr->m_group->m_derived_properties;
}

// The below functions use on-demand property derivation
// only if there is an expression associated with the expression handle.
// If there is only a group expression or a cost context assoicated with the handle,
// all properties must have already been derived as we can't derive anything.
duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOuterReferences(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	}
	return GetRelationalProperties(child_index)->GetOuterReferences();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOuterReferences() {
	if (nullptr != m_pop) {
		return m_pop->GetColumnBindings();
	}
	return GetRelationalProperties()->GetOuterReferences();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOutputColumns(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	} else if (nullptr != m_pgexpr) {
		return m_pgexpr->m_operator->children[child_index]->GetColumnBindings();
	}
	return GetRelationalProperties(child_index)->GetOutputColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOutputColumns() {
	return Pop()->GetColumnBindings();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveNotNullColumns(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	}

	return GetRelationalProperties(child_index)->GetNotNullColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveNotNullColumns() {
	if (nullptr != m_pop) {
		return m_pop->GetColumnBindings();
	}
	return GetRelationalProperties()->GetNotNullColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveCorrelatedApplyColumns(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	}
	return GetRelationalProperties(child_index)->GetCorrelatedApplyColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveCorrelatedApplyColumns() {
	if (nullptr != m_pop) {
		return m_pop->GetColumnBindings();
	}
	return GetRelationalProperties()->GetCorrelatedApplyColumns();
}

CKeyCollection *CExpressionHandle::DeriveKeyCollection(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->DeriveKeyCollection(*this);
	}
	return GetRelationalProperties(child_index)->GetKeyCollection();
}

CKeyCollection *CExpressionHandle::DeriveKeyCollection() {
	if (nullptr != m_pop) {
		return m_pop->DeriveKeyCollection(*this);
	}
	return GetRelationalProperties()->GetKeyCollection();
}

CPropConstraint *CExpressionHandle::DerivePropertyConstraint(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->DerivePropertyConstraint(*this);
	}
	return GetRelationalProperties(child_index)->GetPropertyConstraint();
}

CPropConstraint *CExpressionHandle::DerivePropertyConstraint() {
	if (nullptr != m_pop) {
		return m_pop->DerivePropertyConstraint(*this);
	}
	return GetRelationalProperties()->GetPropertyConstraint();
}

ULONG CExpressionHandle::DeriveJoinDepth(ULONG child_index) {
	if (nullptr != Pexpr()) {
		return m_pop->children[child_index]->DeriveJoinDepth(*this);
	}
	return GetRelationalProperties(child_index)->GetJoinDepth();
}

ULONG CExpressionHandle::DeriveJoinDepth() {
	if (nullptr != m_pop) {
		return m_pop->DeriveJoinDepth(*this);
	}
	return GetRelationalProperties()->GetJoinDepth();
}

duckdb::vector<CFunctionalDependency *> CExpressionHandle::Pdrgpfd(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->DeriveFunctionalDependencies(*this);
	}
	return GetRelationalProperties(child_index)->GetFunctionalDependencies();
}

duckdb::vector<CFunctionalDependency *> CExpressionHandle::Pdrgpfd() {
	if (nullptr != m_pop) {
		return m_pop->DeriveFunctionalDependencies(*this);
	}
	return GetRelationalProperties()->GetFunctionalDependencies();
}
} // namespace gpopt