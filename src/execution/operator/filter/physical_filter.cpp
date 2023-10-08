#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
namespace duckdb {

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality)
{
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1)
	{
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list)
		{
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	}
	else
	{
		expression = std::move(select_list[0]);
	}
}

class FilterState : public CachingOperatorState {
public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "filter", 0);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (FilterState &)state_p;
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, state.sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalFilter::ParamsToString() const {
	auto result = expression->GetName();
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_props->GetCardinality<idx_t>());
	return result;
}

unique_ptr<Operator> PhysicalFilter::Copy() {
	/* PhysicalFilter fields */
	vector<unique_ptr<Expression>> v_expr;
	v_expr.push_back(this->expression->Copy());
	unique_ptr<PhysicalFilter> copy = make_uniq<PhysicalFilter>(this->types, std::move(v_expr),
															    this->estimated_cardinality);
	/* PhysicalOperator fields */
	copy->m_total_opt_requests = this->m_total_opt_requests;

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
	for (auto &child : this->children) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = this->m_group_expression;
	copy->m_cost = this->m_cost;
	return unique_ptr_cast<PhysicalFilter, Operator>(std::move(copy));
}

unique_ptr<Operator> PhysicalFilter::CopyWithNewGroupExpression(CGroupExpression *pgexpr) {
	auto copy = this->Copy();
	copy->m_group_expression = pgexpr;
	return copy;
}

unique_ptr<Operator> PhysicalFilter::CopyWithNewChildren(CGroupExpression *pgexpr, vector<unique_ptr<Operator>> pdrgpexpr,
	                                     				 double cost) {
    /* PhysicalFilter fields */
	vector<unique_ptr<Expression>> v_expr;
	v_expr.push_back(this->expression->Copy());
	unique_ptr<PhysicalFilter> copy = make_uniq<PhysicalFilter>(this->types, std::move(v_expr),
															    this->estimated_cardinality);
	/* PhysicalOperator fields */
	copy->m_total_opt_requests = this->m_total_opt_requests;

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
	for (auto &child : pdrgpexpr) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = cost;
	return unique_ptr_cast<PhysicalFilter, Operator>(std::move(copy));
}

void PhysicalFilter::CE() {
	if(!this->children[0]->has_estimated_cardinality) {
		this->children[0]->CE();
	}
	if (this->has_estimated_cardinality) {
		return;
	}
	idx_t relids = this->GetChildrenRelIds();
	char* pos;
	char* p;
	char cmp[1000];
	int relid_in_file;
	FILE* fp = fopen("/root/giveDuckTopDownOptimizer/optimal/query.txt", "r+");
	while(fgets(cmp, 1000, fp) != NULL) {
		if((pos = strchr(cmp, '\n')) != NULL) {
			*pos = '\0';
		}
		p = strtok(cmp, ":");
		relid_in_file = atoi(p);
		if(relid_in_file == relids) {
			p = strtok(NULL, ":");
			double true_val = atof(p);
			if(true_val < 9999999999999.0) {
				fclose(fp);
				this->has_estimated_cardinality = true;
				this->estimated_cardinality = true_val;
				return;
			}
		}
	}
	fclose(fp);
	this->has_estimated_cardinality = true;
	this->estimated_cardinality = 0.5 * this->children[0]->estimated_cardinality;
	return;
}
	
vector<ColumnBinding> PhysicalFilter::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}
} // namespace duckdb
