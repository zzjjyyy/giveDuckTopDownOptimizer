//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/optimizer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"

#include <functional>

namespace duckdb {
class Binder;

class Optimizer {
public:
	Optimizer(Binder &binder, ClientContext &context, bool forcascade = false);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<Operator> plan);
	unique_ptr<PhysicalOperator> OptimizebyCascade(unique_ptr<Operator> plan);
	ClientContext &context;
	Binder &binder;
	ExpressionRewriter rewriter;
	bool forCascade;
private:
	void RunOptimizer(OptimizerType type, const std::function<void()> &callback);
	void Verify(LogicalOperator &op);

private:
	unique_ptr<LogicalOperator> plan;
};

} // namespace duckdb
