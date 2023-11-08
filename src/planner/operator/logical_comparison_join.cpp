#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type)
{
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = JoinTypeToString(join_type);
	for (auto &condition : conditions) {
		result += "\n";
		auto expr = make_uniq<BoundComparisonExpression>(condition.comparison, condition.left->Copy(), condition.right->Copy());
		result += expr->ToString();
	}

	return result;
}

void LogicalComparisonJoin::Serialize(FieldWriter &writer) const {
	LogicalJoin::Serialize(writer);
	writer.WriteRegularSerializableList(conditions);
	writer.WriteRegularSerializableList(delim_types);
}

void LogicalComparisonJoin::Deserialize(LogicalComparisonJoin &comparison_join, LogicalDeserializationState &state,
                                        FieldReader &reader) {
	LogicalJoin::Deserialize(comparison_join, state, reader);
	comparison_join.conditions = reader.ReadRequiredSerializableList<JoinCondition, JoinCondition>(state.gstate);
	comparison_join.delim_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
}

unique_ptr<LogicalOperator>
LogicalComparisonJoin::Deserialize(LogicalDeserializationState &state, FieldReader &reader)
{
	auto result = make_uniq<LogicalComparisonJoin>(JoinType::INVALID, state.type);
	LogicalComparisonJoin::Deserialize(*result, state, reader);
	return std::move(result);
}

duckdb::unique_ptr<CKeyCollection>
LogicalComparisonJoin::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CPropConstraint>
LogicalComparisonJoin::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return PpcDeriveConstraintPassThru(exprhdl, 0);
}

// Rehydrate expression from a given cost context and child expressions
duckdb::unique_ptr<Operator>
LogicalComparisonJoin::SelfRehydrate(duckdb::unique_ptr<CCostContext> pcc,
									 duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
									 duckdb::unique_ptr<CDrvdPropCtxtPlan> pdpctxtplan)
{
	auto pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	auto pexpr = make_uniq<LogicalComparisonJoin>(join_type);
	// Need to delete
	// for(auto &child : pdrgpexpr)
	for(auto child : pdrgpexpr)
	{
		pexpr->AddChild(child);
	}
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

unique_ptr<Operator> LogicalComparisonJoin::Copy() {
	unique_ptr<LogicalComparisonJoin> copy = make_uniq<LogicalComparisonJoin>(this->join_type, this->logical_type);
	/* LogicalComparisonJoin fields */
	for(auto &child : this->conditions) {
		JoinCondition jc;
		jc.left = child.left->Copy();
		jc.right = child.right->Copy();
		jc.comparison = child.comparison;
		copy->conditions.emplace_back(std::move(jc));
	}
	copy->delim_types = this->delim_types;
	
	/* LogicalJoin fields */
	copy->mark_index = this->mark_index;
	copy->left_projection_map = this->left_projection_map;
	copy->right_projection_map = this->right_projection_map;
	
	/* Operator fields */
	copy->m_derived_logical_property = this->m_derived_logical_property;
	copy->m_derived_physical_property = this->m_derived_physical_property;
	copy->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	for (auto &child : this->expressions) {
		copy->expressions.push_back(child->Copy());
	}
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	copy->logical_type = this->logical_type;
	copy->physical_type = this->physical_type;
	for (auto &child : this->children) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = this->m_group_expression;
	copy->m_cost = this->m_cost;
	return unique_ptr_cast<LogicalComparisonJoin, Operator>(std::move(copy));
}

unique_ptr<Operator>
LogicalComparisonJoin::CopyWithNewGroupExpression(unique_ptr<CGroupExpression> pgexpr) {
	unique_ptr<LogicalComparisonJoin> copy = make_uniq<LogicalComparisonJoin>(this->join_type, this->logical_type);
	/* LogicalComparisonJoin fields */
	for(auto &child : this->conditions) {
		JoinCondition jc;
		jc.left = child.left->Copy();
		jc.right = child.right->Copy();
		jc.comparison = child.comparison;
		copy->conditions.emplace_back(std::move(jc));
	}
	copy->delim_types = this->delim_types;

	/* LogicalJoin fields */
	copy->mark_index = this->mark_index;
	copy->left_projection_map = this->left_projection_map;
	copy->right_projection_map = this->right_projection_map;

	/* Operator fields */
	copy->m_derived_logical_property = this->m_derived_logical_property;
	copy->m_derived_physical_property = this->m_derived_physical_property;
	copy->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	for (auto &child : this->expressions) {
		copy->expressions.push_back(child->Copy());
	}
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	copy->logical_type = this->logical_type;
	copy->physical_type = this->physical_type;
	for (auto &child : this->children) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = this->m_cost;
	return unique_ptr_cast<LogicalComparisonJoin, Operator>(std::move(copy));
}

unique_ptr<Operator>
LogicalComparisonJoin::CopyWithNewChildren(unique_ptr<CGroupExpression> pgexpr,
                                           duckdb::vector<unique_ptr<Operator>> pdrgpexpr,
                                           double cost) {
	unique_ptr<LogicalComparisonJoin> copy = make_uniq<LogicalComparisonJoin>(this->join_type, this->logical_type);
	/* LogicalComparisonJoin fields */
	for(auto &child : this->conditions) {
		JoinCondition jc;
		jc.left = child.left->Copy();
		jc.right = child.right->Copy();
		jc.comparison = child.comparison;
		copy->conditions.emplace_back(std::move(jc));
	}
	copy->delim_types = this->delim_types;

	/* LogicalJoin fields */
	copy->mark_index = this->mark_index;
	copy->left_projection_map = this->left_projection_map;
	copy->right_projection_map = this->right_projection_map;

	/* Operator fields */
	copy->m_derived_logical_property = this->m_derived_logical_property;
	copy->m_derived_physical_property = this->m_derived_physical_property;
	copy->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	for (auto &child : this->expressions) {
		copy->expressions.push_back(child->Copy());
	}
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	copy->logical_type = this->logical_type;
	copy->physical_type = this->physical_type;
	for(auto &child : pdrgpexpr)
	{
		copy->AddChild(std::move(child));
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = cost;
	return unique_ptr_cast<LogicalComparisonJoin, Operator>(std::move(copy));										
}

idx_t LogicalComparisonJoin::GetChildrenRelIds() {
	idx_t first = this->children[0]->GetChildrenRelIds();
	idx_t second = this->children[1]->GetChildrenRelIds();
	idx_t res = first | second;
	return res;
}

void LogicalComparisonJoin::CE() {
	if(!this->children[0]->has_estimated_cardinality) {
		this->children[0]->CE();
	}
	if(!this->children[1]->has_estimated_cardinality) {
		this->children[1]->CE();
	}
	if(this->has_estimated_cardinality) {
		return;
	}
	this->has_estimated_cardinality = true;
	idx_t relids = this->GetChildrenRelIds();
	auto true_val = true_set.find(relids);
	if(true_val != true_set.end()) {
		this->estimated_cardinality = true_val->second;
	} else {
		this->estimated_cardinality = 9999999999.0;
	}
	// this->estimated_cardinality = 99999999.0;
	return;
}
} // namespace duckdb