#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAnyJoin &op)
{
	// first visit the child nodes
	D_ASSERT(op.children.size() == 2);
	D_ASSERT(op.condition);
	LogicalOperator* left_pop = (LogicalOperator*)op.children[0].get();
	auto left = CreatePlan(*left_pop);
	LogicalOperator* right_pop = (LogicalOperator*)op.children[0].get();
	auto right = CreatePlan(*right_pop);
	// create the blockwise NL join
	return make_uniq<PhysicalBlockwiseNLJoin>(op, std::move(left), std::move(right), std::move(op.condition), op.join_type, op.estimated_cardinality);
}
} // namespace duckdb