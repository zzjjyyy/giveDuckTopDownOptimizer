//---------------------------------------------------------------------------
//	@filename:
//		Operator.cpp
//
//	@doc:
//		Base class for all operators: logical, physical, scalar, patterns
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/Operator.h"

#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/planner/operator/logical_get.hpp"

#include <cstdlib>

namespace gpopt {

//---------------------------------------------------------------------------
//	@function:
//		COperator::HashValue
//
//	@doc:
//		default hash function based on operator ID
//
//---------------------------------------------------------------------------
ULONG Operator::HashValue() const {
	ULONG ul_logical_type = (ULONG)logical_type;
	ULONG ul_physical_type = (ULONG)physical_type;
	return CombineHashes(gpos::HashValue<ULONG>(&ul_logical_type), gpos::HashValue<ULONG>(&ul_physical_type));
}

ULONG Operator::HashValue(const Operator *op) {
	ULONG ul_hash = op->HashValue();
	const ULONG arity = op->Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		ul_hash = CombineHashes(ul_hash, HashValue(op->children[ul].get()));
	}
	return ul_hash;
}

idx_t Operator::EstimateCardinality(ClientContext &context) {
	// simple estimator, just take the max of the children
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	idx_t max_cardinality = 0;
	for (auto &child : children) {
		Operator *logical_child = (Operator *)(child.get());
		max_cardinality = MaxValue(logical_child->EstimateCardinality(context), max_cardinality);
	}
	has_estimated_cardinality = true;
	estimated_cardinality = max_cardinality;
	return estimated_cardinality;
}

duckdb::vector<CFunctionalDependency *> Operator::DeriveFunctionalDependencies(CExpressionHandle &expression_handle) {
	return m_derived_logical_property->DeriveFunctionalDependencies(expression_handle);
}

//---------------------------------------------------------------------------
//	@function:
//		Operator::FMatchPattern
//
//	@doc:
//		Check a pattern expression against a given group;
//		shallow, do not	match its children, check only arity of the root
//
//---------------------------------------------------------------------------
bool Operator::FMatchPattern(CGroupExpression *group_expression) {
	if (this->FPattern()) {
		return true;
	} else {
		// match operator id and arity
		if ((this->logical_type == group_expression->m_operator->logical_type ||
		     this->physical_type == group_expression->m_operator->physical_type) &&
		    this->Arity() == group_expression->Arity()) {
			return true;
		}
	}
	return false;
}

CRequiredPhysicalProp *Operator::PrppCompute(CRequiredPhysicalProp *required_properties_input) {
	// derive plan properties
	CDrvdPropCtxtPlan *pdpctxtplan = new CDrvdPropCtxtPlan();
	(void)PdpDerive(pdpctxtplan);
	// decorate nodes with required properties
	return m_required_physical_property;
}

CDerivedProperty *Operator::PdpDerive(CDrvdPropCtxtPlan *pdpctxt) {
	const CDerivedProperty::EPropType ept = Ept();
	CExpressionHandle expression_handle;
	expression_handle.Attach(this);
	// see if suitable prop is already cached. This only applies to plan properties.
	// relational properties are never null and are handled in the next case
	if (nullptr == Pdp(ept)) {
		const ULONG arity = Arity();
		for (ULONG ul = 0; ul < arity; ul++) {
			CDerivedProperty *pdp = children[ul]->PdpDerive(pdpctxt);
			// add child props to derivation context
			CDerivedPropertyContext::AddDerivedProps(pdp, pdpctxt);
		}
		switch (ept) {
		case CDerivedProperty::EptPlan:
			m_derived_physical_property = new CDerivedPhysicalProp();
			break;
		default:
			break;
		}
		Pdp(ept)->Derive(expression_handle, pdpctxt);
	}
	// If we havn't derived all properties, do that now. If we've derived some
	// of the properties, this will only derive properties that have not yet been derived.
	else if (!Pdp(ept)->IsComplete()) {
		Pdp(ept)->Derive(expression_handle, pdpctxt);
	}
	// Otherwise, we've already derived all properties and can simply return them
	return Pdp(ept);
}

CRequiredPhysicalProp *Operator::PrppDecorate(CRequiredPhysicalProp *required_properties_input) {
	return m_required_physical_property;
}

duckdb::unique_ptr<Operator> Operator::Copy() {
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	return result;
}

duckdb::unique_ptr<Operator> Operator::CopyWithNewGroupExpression(CGroupExpression *group_expression) {
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	result->m_group_expression = group_expression;
	return result;
}

duckdb::unique_ptr<Operator> Operator::CopyWithNewChildren(CGroupExpression *group_expression,
                                                           duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                                           double cost) {
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	result->m_group_expression = group_expression;
	for (auto &child : pdrgpexpr) {
		result->AddChild(child->Copy());
	}
	result->m_cost = cost;
	return result;
}

void Operator::CE() {
	if (this->has_estimated_cardinality) {
		return;
	}
	this->has_estimated_cardinality = true;
	this->estimated_cardinality = static_cast<double>(rand() % 1000);
	return;
}

CDerivedProperty *Operator::Pdp(const CDerivedProperty::EPropType ept) const {
	switch (ept) {
	case CDerivedProperty::EptRelational:
		return (CDerivedProperty *)m_derived_logical_property;
	case CDerivedProperty::EptPlan:
		return (CDerivedProperty *)m_derived_physical_property;
	default:
		break;
	}
	return nullptr;
}

CDerivedProperty::EPropType Operator::Ept() const {
	if (FLogical()) {
		return CDerivedProperty::EptRelational;
	}
	if (FPhysical()) {
		return CDerivedProperty::EptPlan;
	}
	return CDerivedProperty::EptInvalid;
}

Operator *Operator::PexprRehydrate(CCostContext *cost_context, duckdb::vector<Operator *> pdrgpexpr,
                                   CDrvdPropCtxtPlan *pdpctxtplan) {
	CGroupExpression *group_expression = cost_context->m_group_expression;
	return group_expression->m_operator->SelfRehydrate(cost_context, pdrgpexpr, pdpctxtplan);
}

void Operator::ResolveOperatorTypes() {
	types.clear();
	// first resolve child types
	for (duckdb::unique_ptr<Operator> &child : children) {
		child->ResolveOperatorTypes();
	}
	// now resolve the types for this operator
	ResolveTypes();
	D_ASSERT(types.size() == GetColumnBindings().size());
}
} // namespace gpopt