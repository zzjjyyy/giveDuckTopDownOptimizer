#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb
{

unique_ptr<LogicalOperator> TopN::Optimize(unique_ptr<LogicalOperator> op)
{
	if (op->logical_type == LogicalOperatorType::LOGICAL_LIMIT && op->children[0]->logical_type == LogicalOperatorType::LOGICAL_ORDER_BY)
	{
		auto &limit = op->Cast<LogicalLimit>();
		auto &order_by = ((LogicalOperator*)op->children[0].get())->Cast<LogicalOrder>();
		// This optimization doesn't apply when OFFSET is present without LIMIT
		// Or if offset is not constant
		if (limit.limit_val != NumericLimits<int64_t>::Maximum() || limit.offset)
		{
			auto topn = make_uniq<LogicalTopN>(std::move(order_by.orders), limit.limit_val, limit.offset_val);
			topn->AddChild(unique_ptr_cast<Operator, LogicalOperator>(std::move(order_by.children[0])));
			op = std::move(topn);
		}
	}
	else
	{
		for (auto &child : op->children)
		{
			child = Optimize(unique_ptr_cast<Operator, LogicalOperator>(std::move(child)));
		}
	}
	return op;
}
} // namespace duckdb
