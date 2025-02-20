#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/cascade/base/CDerivedPropPlan.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalOperator::PhysicalOperator(PhysicalOperatorType type, vector<LogicalType> types, idx_t estimated_cardinality) {
	/* PhysicalOperator fields */
	m_total_opt_requests = 1;
	/* Operator fields */
	physical_type = type;
	m_group_expression = nullptr;
	m_derived_physical_property = nullptr;
	m_required_physical_property = nullptr;
	m_cost = GPOPT_INVALID_COST;
	estimated_props = make_uniq<EstimatedProperties>(estimated_cardinality, 0);
	this->types = types;
	this->estimated_cardinality = estimated_cardinality;
	has_estimated_cardinality = false;
}

PhysicalOperator::~PhysicalOperator() {
}

string PhysicalOperator::GetName() const {
	return PhysicalOperatorToString(physical_type);
}

string PhysicalOperator::ParamsToString() const {
	return "";
}

string PhysicalOperator::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

// LCOV_EXCL_START
void PhysicalOperator::Print() const {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

vector<const_reference<PhysicalOperator>> PhysicalOperator::GetChildren() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		result.push_back(*(PhysicalOperator *)child.get());
	}
	return result;
}

bool PhysicalOperator::Equals(const PhysicalOperator &other) const {
	return false;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
unique_ptr<OperatorState> PhysicalOperator::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<OperatorState>();
}

unique_ptr<GlobalOperatorState> PhysicalOperator::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<GlobalOperatorState>();
}

OperatorResultType PhysicalOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling Execute on a node that is not an operator!");
}

OperatorFinalizeResultType PhysicalOperator::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                          GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling FinalExecute on a node that is not an operator!");
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalOperator::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<LocalSourceState>();
}

unique_ptr<GlobalSourceState> PhysicalOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GlobalSourceState>();
}

// LCOV_EXCL_START
void PhysicalOperator::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                               LocalSourceState &lstate) const {
	throw InternalException("Calling GetData on a node that is not a source!");
}

idx_t PhysicalOperator::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	throw InternalException("Calling GetBatchIndex on a node that does not support it");
}

double PhysicalOperator::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	return -1;
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
SinkResultType PhysicalOperator::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                      DataChunk &input) const {
	throw InternalException("Calling Sink on a node that is not a sink!");
}
// LCOV_EXCL_STOP

void PhysicalOperator::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
}

SinkFinalizeType PhysicalOperator::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalOperator::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<LocalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalOperator::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<GlobalSinkState>();
}

idx_t PhysicalOperator::GetMaxThreadMemory(ClientContext &context) {
	// Memory usage per thread should scale with max mem / num threads
	// We take 1/4th of this, to be conservative
	idx_t max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	return (max_memory / num_threads) / 4;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalOperator::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	auto &state = meta_pipeline.GetState();
	if (IsSink()) {
		// operator is a sink, build a pipeline
		sink_state.reset();
		D_ASSERT(children.size() == 1);

		// single operator: the operator becomes the data source of the current pipeline
		state.SetPipelineSource(current, *this);

		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		PhysicalOperator *physical_children = (PhysicalOperator *)(children[0].get());
		child_meta_pipeline.Build(*physical_children);
	} else {
		// operator is not a sink! recurse in children
		if (children.empty()) {
			// source
			state.SetPipelineSource(current, *this);
		} else {
			if (children.size() != 1) {
				throw InternalException("Operator not supported in BuildPipelines");
			}
			state.AddPipelineOperator(current, *this);
			PhysicalOperator *physical_children = (PhysicalOperator *)(children[0].get());
			physical_children->BuildPipelines(current, meta_pipeline);
		}
	}
}

vector<const_reference<PhysicalOperator>> PhysicalOperator::GetSources() const {
	vector<const_reference<PhysicalOperator>> result;
	if (IsSink()) {
		D_ASSERT(children.size() == 1);
		result.push_back(*this);
		return result;
	} else {
		if (children.empty()) {
			// source
			result.push_back(*this);
			return result;
		} else {
			if (children.size() != 1) {
				throw InternalException("Operator not supported in GetSource");
			}
			PhysicalOperator *physical_children = (PhysicalOperator *)(children[0].get());
			return physical_children->GetSources();
		}
	}
}

bool PhysicalOperator::AllSourcesSupportBatchIndex() const {
	auto sources = GetSources();
	for (auto &source : sources) {
		if (!source.get().SupportsBatchIndex()) {
			return false;
		}
	}
	return true;
}

void PhysicalOperator::Verify() {
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::CreateDerivedProperty
//
//	@doc:
//		Create base container of derived properties
//
//---------------------------------------------------------------------------
CDerivedProperty *PhysicalOperator::CreateDerivedProperty() {
	if (m_derived_physical_property == nullptr)
		return new CDerivedPhysicalProp();

	return m_derived_physical_property;
}

CRequiredProperty *PhysicalOperator::CreateRequiredProperty() const {
	if (m_required_physical_property == nullptr)
		return new CRequiredPhysicalProp();

	return m_required_physical_property;
}

COrderProperty::EOrderMatching PhysicalOperator::OrderMatching(CRequiredPhysicalProp *, ULONG,
                                                               vector<CDerivedProperty *>, ULONG) {
	return COrderProperty::EomSatisfy;
}

// The ColumnBinding will change after pass through the projection operator
unique_ptr<Expression> PhysicalOperator::ExpressionPassThrough(const PhysicalOperator *op, Expression *expr) {
	if (op->physical_type == PhysicalOperatorType::PROJECTION) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_COLUMN_REF);
		auto *proj = (PhysicalProjection *)op;
		auto *result = (BoundColumnRefExpression *)(expr);
		// For yiming, I think the commented code should be used to solve the problem
		idx_t tbl_index = result->binding.table_index;
		if (tbl_index == proj->v_column_binding[0].table_index) {
			// This means that the ColumnBinding belongs to Projection's
			idx_t col_idx = result->binding.column_index;
			return proj->select_list[col_idx]->Copy();
		} else {
			// This means that the ColumnBinding does not belong to Projection's
			return expr->Copy();
		}
		// Add by Junyi
	} else {
		return expr->Copy();
	}
}

COrderProperty::EPropEnforcingType PhysicalOperator::EnforcingTypeOrder(CExpressionHandle &handle,
                                                                        vector<BoundOrderByNode> &peo) const {
	if (handle.group_expr() != nullptr) {
		vector<BoundOrderByNode> v;
		/* In case inconsistence */
		for (auto &child : peo) {
			unique_ptr<Expression> new_expr = ExpressionPassThrough(this, child.expression.get());
			v.emplace_back(child.type, child.null_order, std::move(new_expr));
		}

		// derive all the possible order of this CGroupExpression
		CGroupExpression *group_expr = handle.group_expr();
		if (group_expr->m_child_groups.size() > 0) {
			// Only the order of the first child influence the order of its parent
			CGroup *gp = group_expr->m_child_groups[0];
			for (auto &it : gp->m_sht) {
				auto &opt_context = it.second;
				auto &order_nodes = opt_context->m_required_plan_properties->m_sort_order->m_order_spec->order_nodes;
				if (CUtils::ContainsAll(order_nodes, v)) {
					return COrderProperty::EPropEnforcingType::EpetOptional;
				}
			}
		}
	}
	return COrderProperty::EPropEnforcingType::EpetRequired;
}

COrderSpec *PhysicalOperator::RequiredSortSpec(CExpressionHandle &handle, COrderSpec *order_spec, ULONG child_index,
                                               vector<CDerivedProperty *> children_derived_property,
                                               ULONG num_opt_request) const {
	if (child_index == 0) {
		auto first_child_cols = children[0]->GetColumnBindings();
		COrderSpec *res = new COrderSpec();
		for (auto &child : order_spec->order_nodes) {
			unique_ptr<Expression> expr = ExpressionPassThrough(this, child.expression.get());
			if (CUtils::ContainsAll(first_child_cols, expr->GetColumnBinding())) {
				BoundOrderByNode order(child.type, child.null_order, std::move(expr));
				res->order_nodes.push_back(std::move(order));
			}
		}
		return res;
	} else {
		return new COrderSpec();
	}
}

bool CachingPhysicalOperator::CanCacheType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return false;
	case LogicalTypeId::STRUCT: {
		auto &entries = StructType::GetChildTypes(type);
		for (auto &entry : entries) {
			if (!CanCacheType(entry.second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

CachingPhysicalOperator::CachingPhysicalOperator(PhysicalOperatorType type, vector<LogicalType> types_p,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(type, std::move(types_p), estimated_cardinality) {
	caching_supported = true;
	for (auto &col_type : types) {
		if (!CanCacheType(col_type)) {
			caching_supported = false;
			break;
		}
	}
}

OperatorResultType CachingPhysicalOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<CachingOperatorState>();
	// Execute child operator
	auto child_result = ExecuteInternal(context, input, chunk, gstate, state);

#if STANDARD_VECTOR_SIZE >= 128
	if (!state.initialized) {
		state.initialized = true;
		state.can_cache_chunk = true;

		if (!context.pipeline || !caching_supported) {
			state.can_cache_chunk = false;
		} else if (!context.pipeline->GetSink()) {
			// Disabling for pipelines without Sink, i.e. when pulling
			state.can_cache_chunk = false;
		} else if (context.pipeline->GetSink()->RequiresBatchIndex()) {
			state.can_cache_chunk = false;
		} else if (context.pipeline->IsOrderDependent()) {
			state.can_cache_chunk = false;
		}
	}
	if (!state.can_cache_chunk) {
		return child_result;
	}
	if (chunk.size() < CACHE_THRESHOLD) {
		// we have filtered out a significant amount of tuples
		// add this chunk to the cache and continue

		if (!state.cached_chunk) {
			state.cached_chunk = make_uniq<DataChunk>();
			state.cached_chunk->Initialize(Allocator::Get(context.client), chunk.GetTypes());
		}

		state.cached_chunk->Append(chunk);

		if (state.cached_chunk->size() >= (STANDARD_VECTOR_SIZE - CACHE_THRESHOLD) ||
		    child_result == OperatorResultType::FINISHED) {
			// chunk cache full: return it
			chunk.Move(*state.cached_chunk);
			state.cached_chunk->Initialize(Allocator::Get(context.client), chunk.GetTypes());
			return child_result;
		} else {
			// chunk cache not full return empty result
			chunk.Reset();
		}
	}
#endif

	return child_result;
}

OperatorFinalizeResultType CachingPhysicalOperator::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                 GlobalOperatorState &gstate,
                                                                 OperatorState &state_p) const {
	auto &state = state_p.Cast<CachingOperatorState>();
	if (state.cached_chunk) {
		chunk.Move(*state.cached_chunk);
		state.cached_chunk.reset();
	} else {
		chunk.SetCardinality(0);
	}
	return OperatorFinalizeResultType::FINISHED;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::FUnaryProvidesReqdCols
//
//	@doc:
//		Helper for checking if output columns of a unary operator that defines
//		no new columns include the required columns
//
//---------------------------------------------------------------------------
BOOL PhysicalOperator::FUnaryProvidesReqdCols(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrsRequired) const {
	vector<ColumnBinding> pcrsOutput = exprhdl.DeriveOutputColumns(0);
	return CUtils::ContainsAll(pcrsOutput, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysical::PcrsChildReqd
//
//	@doc:
//		Helper for computing required output columns of the n-th child;
//		the caller must be an operator whose ulScalarIndex-th child is a
//		scalar
//
//---------------------------------------------------------------------------
vector<ColumnBinding> PhysicalOperator::PcrsChildReqd(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrsRequired,
                                                      ULONG child_index) {
	// intersect computed column set with child's output columns
	vector<ColumnBinding> output_cols = exprhdl.DeriveOutputColumns(child_index);
	vector<ColumnBinding> res;
	for (auto &child : output_cols) {
		for (auto &sub_child : pcrsRequired) {
			if (child == sub_child) {
				res.push_back(child);
				break;
			}
		}
	}
	return res;
}
} // namespace duckdb