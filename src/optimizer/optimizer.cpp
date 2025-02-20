#include "duckdb/optimizer/optimizer.hpp"

#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/cascade/Cascade.h"
#include "duckdb/optimizer/column_lifetime_optimizer.hpp"
#include "duckdb/optimizer/common_aggregate_optimizer.hpp"
#include "duckdb/optimizer/cse_optimizer.hpp"
#include "duckdb/optimizer/deliminator.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/regex_range_filter.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/optimizer/rule/equal_or_null_simplification.hpp"
#include "duckdb/optimizer/rule/in_clause_simplification.hpp"
#include "duckdb/optimizer/rule/list.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/optimizer/unnest_rewriter.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

Optimizer::Optimizer(Binder &binder, ClientContext &context, bool forcascade)
    : context(context), binder(binder), rewriter(context), forCascade(forcascade) {
	rewriter.rules.push_back(make_uniq<ConstantFoldingRule>(rewriter));
	rewriter.rules.push_back(make_uniq<DistributivityRule>(rewriter));
	rewriter.rules.push_back(make_uniq<ArithmeticSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<CaseSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<ConjunctionSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<DatePartSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<ComparisonSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<InClauseSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<EqualOrNullSimplification>(rewriter));
	rewriter.rules.push_back(make_uniq<MoveConstantsRule>(rewriter));
	rewriter.rules.push_back(make_uniq<LikeOptimizationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<OrderedAggregateOptimizer>(rewriter));
	rewriter.rules.push_back(make_uniq<RegexOptimizationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<EmptyNeedleRemovalRule>(rewriter));
	rewriter.rules.push_back(make_uniq<EnumComparisonRule>(rewriter));
}

void Optimizer::RunOptimizer(OptimizerType type, const std::function<void()> &callback) {
	auto &config = DBConfig::GetConfig(context);
	if (config.options.disabled_optimizers.find(type) != config.options.disabled_optimizers.end()) {
		// optimizer is marked as disabled: skip
		return;
	}
	auto &profiler = QueryProfiler::Get(context);
	profiler.StartPhase(OptimizerTypeToString(type));
	callback();
	profiler.EndPhase();
	if (plan) {
		Verify(*plan);
	}
}

void Optimizer::Verify(LogicalOperator &op) {
	ColumnBindingResolver::Verify(op);
}

unique_ptr<LogicalOperator> Optimizer::Optimize(unique_ptr<Operator> plan_p) {
	unique_ptr<LogicalOperator> logical_plan = unique_ptr_cast<Operator, LogicalOperator>(std::move(plan_p));

	// print the logical plan
	//	Printer::Print("Logical Plan: \n");
	//	logical_plan->Print();

	Verify(*logical_plan);
	this->plan = std::move(logical_plan);
	// first we perform expression rewrites using the ExpressionRewriter
	// this does not change the logical plan structure, but only simplifies the expression trees
	RunOptimizer(OptimizerType::EXPRESSION_REWRITER, [&]() { rewriter.VisitOperator(*plan); });
	// perform filter pullup
	RunOptimizer(OptimizerType::FILTER_PULLUP, [&]() {
		FilterPullup filter_pullup;
		plan = filter_pullup.Rewrite(std::move(plan));
	});
	// perform filter pushdown
	RunOptimizer(OptimizerType::FILTER_PUSHDOWN, [&]() {
		FilterPushdown filter_pushdown(*this);
		plan = filter_pushdown.Rewrite(std::move(plan));
	});
	RunOptimizer(OptimizerType::REGEX_RANGE, [&]() {
		RegexRangeFilter regex_opt;
		plan = regex_opt.Rewrite(std::move(plan));
	});
	/*
	RunOptimizer(OptimizerType::IN_CLAUSE, [&]() {
		InClauseRewriter rewriter(context, *this);
		plan = rewriter.Rewrite(std::move(plan));
	});
	*/
	if (!forCascade) {
		// then we perform the join ordering optimization
		// this also rewrites cross products + filters into joins and performs filter pushdowns
		RunOptimizer(OptimizerType::JOIN_ORDER, [&]() {
			JoinOrderOptimizer optimizer(context);
			plan = optimizer.Optimize(std::move(plan));
		});
		// removes any redundant DelimGets/DelimJoins
		RunOptimizer(OptimizerType::DELIMINATOR, [&]() {
			Deliminator deliminator(context);
			plan = deliminator.Optimize(std::move(plan));
		});
		// rewrites UNNESTs in DelimJoins by moving them to the projection
		RunOptimizer(OptimizerType::UNNEST_REWRITER, [&]() {
			UnnestRewriter unnest_rewriter;
			plan = unnest_rewriter.Optimize(std::move(plan));
		});
		// removes unused columns
		RunOptimizer(OptimizerType::UNUSED_COLUMNS, [&]() {
			RemoveUnusedColumns unused(binder, context, true);
			unused.VisitOperator(*plan);
		});
		// perform statistics propagation
		RunOptimizer(OptimizerType::STATISTICS_PROPAGATION, [&]() {
			StatisticsPropagator propagator(context);
			propagator.PropagateStatistics(plan);
		});
		// then we extract common subexpressions inside the different operators
		RunOptimizer(OptimizerType::COMMON_SUBEXPRESSIONS, [&]() {
			CommonSubExpressionOptimizer cse_optimizer(binder);
			cse_optimizer.VisitOperator(*plan);
		});
		RunOptimizer(OptimizerType::COMMON_AGGREGATE, [&]() {
			CommonAggregateOptimizer common_aggregate;
			common_aggregate.VisitOperator(*plan);
		});
		RunOptimizer(OptimizerType::COLUMN_LIFETIME, [&]() {
			ColumnLifetimeAnalyzer column_lifetime(true);
			column_lifetime.VisitOperator(*plan);
		});
		// transform ORDER BY + LIMIT to TopN
		RunOptimizer(OptimizerType::TOP_N, [&]() {
			TopN topn;
			plan = topn.Optimize(std::move(plan));
		});
		// apply simple expression heuristics to get an initial reordering
		RunOptimizer(OptimizerType::REORDER_FILTER, [&]() {
			ExpressionHeuristics expression_heuristics(*this);
			plan = expression_heuristics.Rewrite(std::move(plan));
		});
		for (auto &optimizer_extension : DBConfig::GetConfig(context).optimizer_extensions) {
			RunOptimizer(OptimizerType::EXTENSION, [&]() {
				optimizer_extension.optimize_function(context, optimizer_extension.optimizer_info.get(), plan);
			});
		}
	}
	unique_ptr<Operator> tmp_plan = unique_ptr_cast<LogicalOperator, Operator>(std::move(plan));
	Planner::VerifyPlan(context, tmp_plan);
	unique_ptr<LogicalOperator> final_plan = unique_ptr_cast<Operator, LogicalOperator>(std::move(tmp_plan));

	// print the logical plan
	// Printer::Print("Input Logical Plan: \n");
	// final_plan->Print();

	return final_plan;
}
} // namespace duckdb