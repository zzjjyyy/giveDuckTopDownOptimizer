#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

LogicalAggregate::LogicalAggregate(idx_t group_index, idx_t aggregate_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY, std::move(select_list)),
      group_index(group_index), aggregate_index(aggregate_index), groupings_index(DConstants::INVALID_INDEX) {
}

void LogicalAggregate::ResolveTypes() {
	D_ASSERT(groupings_index != DConstants::INVALID_INDEX || grouping_functions.empty());
	for (auto &expr : groups) {
		types.push_back(expr->return_type);
	}
	// get the chunk types from the projection list
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
	for (idx_t i = 0; i < grouping_functions.size(); i++) {
		types.emplace_back(LogicalType::BIGINT);
	}
}

vector<ColumnBinding> LogicalAggregate::GetColumnBindings() {
	D_ASSERT(groupings_index != DConstants::INVALID_INDEX || grouping_functions.empty());
	vector<ColumnBinding> result;
	result.reserve(groups.size() + expressions.size() + grouping_functions.size());
	/*
	for (idx_t i = 0; i < groups.size(); i++) {
		result.emplace_back(group_index, i);
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		result.emplace_back(aggregate_index, i);
	}
	for (idx_t i = 0; i < grouping_functions.size(); i++) {
		result.emplace_back(groupings_index, i);
	}
	*/
	for (idx_t i = 0; i < groups.size(); i++) {
		auto v = groups[i]->GetColumnBinding();
		result.insert(result.end(), v.begin(), v.end());
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		result.emplace_back(aggregate_index, i);
	}
	return result;
}

string LogicalAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += expressions[i]->GetName();
	}
	return result;
}

void LogicalAggregate::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(expressions);

	writer.WriteField(group_index);
	writer.WriteField(aggregate_index);
	writer.WriteField(groupings_index);
	writer.WriteSerializableList(groups);
	writer.WriteField<idx_t>(grouping_sets.size());
	for (auto &entry : grouping_sets) {
		writer.WriteList<idx_t>(entry);
	}
	writer.WriteField<idx_t>(grouping_functions.size());
	for (auto &entry : grouping_functions) {
		writer.WriteList<idx_t>(entry);
	}

	// TODO statistics
}

unique_ptr<LogicalOperator> LogicalAggregate::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	auto group_index = reader.ReadRequired<idx_t>();
	auto aggregate_index = reader.ReadRequired<idx_t>();
	auto groupings_index = reader.ReadRequired<idx_t>();
	auto groups = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	auto grouping_sets_size = reader.ReadRequired<idx_t>();
	vector<GroupingSet> grouping_sets;
	for (idx_t i = 0; i < grouping_sets_size; i++) {
		grouping_sets.push_back(reader.ReadRequiredSet<idx_t>());
	}
	vector<vector<idx_t>> grouping_functions;
	auto grouping_functions_size = reader.ReadRequired<idx_t>();
	for (idx_t i = 0; i < grouping_functions_size; i++) {
		grouping_functions.push_back(reader.ReadRequiredList<idx_t>());
	}
	auto result = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(expressions));
	result->groupings_index = groupings_index;
	result->groups = std::move(groups);
	result->grouping_functions = std::move(grouping_functions);
	result->grouping_sets = std::move(grouping_sets);

	return std::move(result);
}

idx_t LogicalAggregate::EstimateCardinality(ClientContext &context) {
	if (groups.empty()) {
		// ungrouped aggregate
		return 1;
	}
	return LogicalOperator::EstimateCardinality(context);
}

vector<idx_t> LogicalAggregate::GetTableIndex() const {
	vector<idx_t> result {group_index, aggregate_index};
	if (groupings_index != DConstants::INVALID_INDEX) {
		result.push_back(groupings_index);
	}
	return result;
}

unique_ptr<Operator> LogicalAggregate::Copy() {
	/* LogicalAggregate fields */
	vector<unique_ptr<Expression>> v;
	for(auto &child : this->expressions) {
		v.push_back(child->Copy());
	}
	unique_ptr<LogicalAggregate> copy = make_uniq<LogicalAggregate>(this->group_index, this->aggregate_index, std::move(v));
	for(auto &child : this->groups) {
		copy->groups.push_back(child->Copy());
	}
	
	/* Operator fields */
	copy->m_derived_logical_property = this->m_derived_logical_property;
	copy->m_derived_physical_property = this->m_derived_physical_property;
	copy->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	copy->logical_type = this->logical_type;
	copy->physical_type = this->physical_type;
	for (auto &child : this->children) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = this->m_group_expression;
	copy->m_cost = this->m_cost;
	return unique_ptr_cast<LogicalAggregate, Operator>(std::move(copy));
}
	
unique_ptr<Operator>
LogicalAggregate::CopyWithNewGroupExpression(unique_ptr<CGroupExpression> pgexpr) {
	unique_ptr<Operator> copy = this->Copy();
	copy->m_group_expression = pgexpr;
	return copy;
}

unique_ptr<Operator>
LogicalAggregate::CopyWithNewChildren(unique_ptr<CGroupExpression> pgexpr,
                                      duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                      double cost) {
	/* LogicalComparisonJoin fields */
	vector<unique_ptr<Expression>> v;
	for(auto &child : this->expressions) {
		v.push_back(child->Copy());
	}
	unique_ptr<LogicalAggregate> copy = make_uniq<LogicalAggregate>(this->group_index, this->aggregate_index, std::move(v));
	for(auto &child : this->groups) {
		copy->groups.push_back(child->Copy());
	}

	/* Operator fields */
	copy->m_derived_logical_property = this->m_derived_logical_property;
	copy->m_derived_physical_property = this->m_derived_physical_property;
	copy->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	copy->logical_type = this->logical_type;
	copy->physical_type = this->physical_type;
	for (auto &child : pdrgpexpr) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = cost;
	return unique_ptr_cast<LogicalAggregate, Operator>(std::move(copy));
}
	
void LogicalAggregate::CE() {
	if(!this->children[0]->has_estimated_cardinality) {
		this->children[0]->CE();
	}
	if (this->has_estimated_cardinality) {
		return;
	}
	this->has_estimated_cardinality = true;
	if(this->groups.empty()) {
		this->estimated_cardinality = 1;
	} else {
		this->estimated_cardinality = this->groups.size();
	}
	return;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalProjection::XformCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CXform_set> LogicalAggregate::XformCandidates() const {
	auto xform_set = make_uniq<CXform_set>();
	(void)xform_set->set(CXform::ExfLogicalAggregateImplementation);
	return xform_set;
}

} // namespace duckdb
