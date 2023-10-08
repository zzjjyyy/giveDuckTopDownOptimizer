//---------------------------------------------------------------------------
//	@filename:
//		Cascade.cpp
//
//	@doc:
//		Implementation of cascade optimizer
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/Cascade.h"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/cascade/NewColumnBindingResolver.h"
#include "duckdb/optimizer/cascade/base/CAutoOptCtxt.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/task/CWorkerPoolManager.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"
#include "duckdb/planner/operator/logical_get.hpp"

#include <cstdlib>

namespace gpos {
	unsigned int enumeration_pairs = 0;
}

namespace duckdb {
using namespace gpos;
using namespace gpopt;

duckdb::unique_ptr<PhysicalOperator> Cascade::Optimize(duckdb::unique_ptr<LogicalOperator> plan) {
	/* Used for CCostContext::CostCompute */
	gpos::enumeration_pairs = 0;

	// calculate the initial cardinality
	plan->CE();
	
	// ColumnBindingResolver resolver;
	// resolver.VisitOperator(*plan);

	// now resolve types of all the operators
	plan->ResolveOperatorTypes();

	// extract dependencies from the logical plan
	// DependencyExtractor extractor(dependencies);
	// extractor.VisitOperator(*plan);

	// init Xform factory
	if (GPOS_OK != CXformFactory::Init()) {
		return nullptr;
	}

	// init worker pool manager
	if (GPOS_OK != CWorkerPoolManager::Init()) {
		return nullptr;
	}
	if (nullptr == CWorkerPoolManager::m_worker_pool_manager) {
		return nullptr;
	}
	CWorkerPoolManager *worker_pool_manager = CWorkerPoolManager::m_worker_pool_manager.get();
	duckdb::unique_ptr<CWorker> worker = make_uniq<CWorker>(GPOS_WORKER_STACK_SIZE, (ULONG_PTR)&worker_pool_manager);
	CWorkerPoolManager::m_worker_pool_manager->RegisterWorker(std::move(worker));

	// init task proxy and TLS
	CAutoTaskProxy task_proxy(worker_pool_manager, true);
	CTask *task = task_proxy.Create(nullptr, nullptr);
	task->GetTls().Reset();
	task_proxy.Execute(task);
	IConstExprEvaluator *expr_evaluator = nullptr;
	COptimizerConfig *optimizer_config = nullptr;
	CAutoOptCtxt optimizer_context(expr_evaluator, optimizer_config);

	// init query context, it describes the requirements of the query output.
	duckdb::vector<ULONG *> output_column_ids(1, nullptr);
	duckdb::vector<std::string> output_column_names(1, "A");
	CQueryContext *query_context =
	    CQueryContext::QueryContextGenerate(std::move(plan), output_column_ids, output_column_names, true);

	// init orca engine
	CEngine engine;
	vector<CSearchStage *> search_strategy;
	engine.Init(query_context, search_strategy);

	// optimize
	engine.Optimize();
	duckdb::unique_ptr<PhysicalOperator> physical_plan =
	    duckdb::unique_ptr<PhysicalOperator>((PhysicalOperator *)engine.PreviousSearchStage()->m_best_expr.release());

	/* I comment here */
	// CExpression* physical_plan = engine.ExprExtractPlan();
	// CheckCTEConsistency(physical_plan);
	// PrintQueryOrPlan(physical_plan);
	// (void) physical_plan->PrppCompute(query_context->m_required_physical_property);
	task_proxy.DestroyAll();
	worker.release();

	FILE* f_pair = fopen("/root/giveDuckTopDownOptimizer/expr/result.txt", "a+");
	fprintf(f_pair, "Considered join pairs: %d, ", enumeration_pairs);
	fclose(f_pair);

	// print physical plan
	// Printer::Print("Physical Plan: \n");
	// physical_plan->Print();

	// resolve column references
	NewColumnBindingResolver new_resolver;
	new_resolver.VisitOperator(*physical_plan);
	return physical_plan;
}
} // namespace duckdb