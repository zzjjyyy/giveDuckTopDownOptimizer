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
size_t Operator::HashValue() const {
	size_t ul_logical_type = (size_t)logical_type;
	size_t ul_physical_type = (size_t)physical_type;
	return duckdb::CombineHash(duckdb::Hash<size_t>(ul_logical_type), duckdb::Hash<size_t>(ul_physical_type));
}

size_t Operator::HashValue(const duckdb::unique_ptr<Operator> op) {
	size_t ul_hash = op->HashValue();
	const size_t arity = op->Arity();
	for (size_t ul = 0; ul < arity; ul++) {
		ul_hash = CombineHashes(ul_hash, HashValue(op->children[ul]));
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

duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>>
Operator::DeriveFunctionalDependencies(CExpressionHandle &expression_handle) {
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
bool Operator::FMatchPattern(duckdb::unique_ptr<CGroupExpression> group_expression) {
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

duckdb::unique_ptr<CRequiredPhysicalProp>
Operator::PrppCompute(duckdb::unique_ptr<Operator> this_operator,
					  duckdb::unique_ptr<CRequiredPhysicalProp> required_properties_input) {
	// derive plan properties
	auto pdpctxtplan = make_uniq<CDrvdPropCtxtPlan>();
	(void)PdpDerive(this_operator, pdpctxtplan);
	// decorate nodes with required properties
	return m_required_physical_property;
}

duckdb::unique_ptr<CDerivedProperty>
Operator::PdpDerive(duckdb::unique_ptr<Operator> this_operator, duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxt) {
	const CDerivedProperty::EPropType ept = Ept();
	CExpressionHandle expression_handle;
	expression_handle.Attach(this_operator);
	// see if suitable prop is already cached. This only applies to plan properties.
	// relational properties are never null and are handled in the next case
	if (nullptr == Pdp(ept)) {
		const ULONG arity = Arity();
		for (ULONG ul = 0; ul < arity; ul++) {
			auto pdp = children[ul]->PdpDerive(children[ul], pdpctxt);
			// add child props to derivation context
			CDerivedPropertyContext::AddDerivedProps(pdp, pdpctxt);
		}
		switch (ept) {
		case CDerivedProperty::EptPlan:
			m_derived_physical_property = make_uniq<CDerivedPhysicalProp>();
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

duckdb::unique_ptr<CRequiredPhysicalProp>
Operator::PrppDecorate(duckdb::unique_ptr<CRequiredPhysicalProp> required_properties_input) {
	return m_required_physical_property;
}

duckdb::unique_ptr<Operator> Operator::Copy() {
	throw InternalException("Shouldn't go here.");
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	return result;
}

duckdb::unique_ptr<Operator>
Operator::CopyWithNewGroupExpression(duckdb::unique_ptr<CGroupExpression> group_expression) {
	throw InternalException("Shouldn't go here.");
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	result->m_group_expression = group_expression;
	return result;
}

duckdb::unique_ptr<Operator>
Operator::CopyWithNewChildren(duckdb::unique_ptr<CGroupExpression> group_expression,
                              duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                              double cost) {
	throw InternalException("Shouldn't go here.");
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	result->m_group_expression = group_expression;
	// Need to delete
	// for (auto &child : pdrgpexpr) {
	for (auto child : pdrgpexpr) {
		// Need to delete
		// result->AddChild(child->Copy());
		result->AddChild(child);
	}
	result->m_cost = cost;
	return result;
}

idx_t Operator::GetChildrenRelIds() {
	return this->children[0]->GetChildrenRelIds();
}

void Operator::CE() {
	throw InternalException("Operator::CE(): shouldn't enter this function!");
	if (this->has_estimated_cardinality) {
		return;
	}
	this->has_estimated_cardinality = true;
	this->estimated_cardinality = static_cast<double>(rand() % 1000);
	return;
}

duckdb::unique_ptr<CDerivedProperty> Operator::Pdp(const CDerivedProperty::EPropType ept) const {
	switch (ept) {
	case CDerivedProperty::EptRelational:
		return unique_ptr_cast<CDerivedLogicalProp, CDerivedProperty>(m_derived_logical_property);
	case CDerivedProperty::EptPlan:
		return unique_ptr_cast<CDerivedPhysicalProp, CDerivedProperty>(m_derived_physical_property);
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

duckdb::unique_ptr<Operator>
Operator::PexprRehydrate(duckdb::unique_ptr<CCostContext> cost_context,
						 duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                         duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan) {
	auto group_expression = cost_context->m_group_expression;
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