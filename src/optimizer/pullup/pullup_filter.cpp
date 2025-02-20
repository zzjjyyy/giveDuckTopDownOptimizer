#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"

namespace duckdb
{
unique_ptr<LogicalOperator> FilterPullup::PullupFilter(unique_ptr<LogicalOperator> op)
{
	D_ASSERT(op->logical_type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();
	if (can_pullup && filter.projection_map.empty())
	{
		unique_ptr<LogicalOperator> child = unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[0]));
		child = Rewrite(std::move(child));
		// moving filter's expressions
		for (idx_t i = 0; i < op->expressions.size(); ++i)
		{
			filters_expr_pullup.push_back(std::move(op->expressions[i]));
		}
		return child;
	}
	op->children[0] = Rewrite(unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[0])));
	return op;
}
} // namespace duckdb