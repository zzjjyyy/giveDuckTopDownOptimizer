//---------------------------------------------------------------------------
//	@filename:
//		CXformLogicalAggregateImplementation.cpp
//
//	@doc:
//		Implementation of Logical Aggregate
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformLogicalAggregateImplementation.h"

#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::CXformGet2TableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformLogicalAggregateImplementation::CXformLogicalAggregateImplementation()
    : CXformImplementation(make_uniq<LogicalAggregate>(0, 0, duckdb::vector<duckdb::unique_ptr<Expression>>())) {
	this->m_operator->AddChild(make_uniq<CPatternLeaf>());
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::XformPromise
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformLogicalAggregateImplementation::XformPromise(CExpressionHandle &expression_handle) const {
	return CXform::ExfpMedium;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformLogicalAggregateImplementation::Transform(CXformContext *pxfctxt, CXformResult *pxfres, Operator *pexpr) const {
	LogicalAggregate *op_agg = static_cast<LogicalAggregate *>(pexpr);
    if (op_agg->groups.empty()) {
		// no groups, check if we can use a simple aggregation
		// special case: aggregate entire columns together
		bool use_simple_aggregation = true;
		for (auto &expression : op_agg->expressions) {
			auto &aggregate = expression->Cast<BoundAggregateExpression>();
			if (!aggregate.function.simple_update) {
				// unsupported aggregate for simple aggregation: use hash aggregation
				use_simple_aggregation = false;
				break;
			}
		}
        duckdb::vector<duckdb::unique_ptr<Expression>> v;
        for(auto &child : op_agg->expressions) {
            v.push_back(child->Copy());
        }
		if (use_simple_aggregation) {
			auto Agg = make_uniq<PhysicalUngroupedAggregate>(op_agg->types, std::move(v),
			                                                 op_agg->estimated_cardinality);
			Agg->v_column_binding = op_agg->GetColumnBindings();
			for(auto &child : pexpr->children) {
				Agg->AddChild(child->Copy());
			}
			// Cardinality Estimation
			Agg->CE();
			// add implementation to transformation result
			pxfres->Add(std::move(Agg));
		} else {
			auto groupby = make_uniq<PhysicalHashAggregate>(op_agg->types, std::move(v),
                                                            op_agg->estimated_cardinality);
			groupby->v_column_binding = op_agg->GetColumnBindings();
			for(auto &child : pexpr->children) {
				groupby->AddChild(child->Copy());
			}
			// Cardinality Estimation
			groupby->CE();
			// add implementation to transformation result
			pxfres->Add(std::move(groupby));
		}
	} else {
        // groups! create a GROUP BY aggregator
		duckdb::vector<idx_t> required_bits;
		duckdb::vector<duckdb::unique_ptr<Expression>> expressions;
		for(auto &child : op_agg->expressions) {
            expressions.push_back(child->Copy());
        }
		duckdb::vector<duckdb::unique_ptr<Expression>> groups;
		for(auto &child : op_agg->groups) {
            groups.push_back(child->Copy());
        }
		duckdb::vector<duckdb::GroupingSet> grouping_sets;
		for(auto &child : op_agg->grouping_sets) {
            grouping_sets.push_back(child);
        }
		duckdb::vector<duckdb::vector<idx_t>> grouping_functions;
		for(auto &child : op_agg->grouping_functions) {
            grouping_functions.push_back(child);
        }
		auto groupby = make_uniq<PhysicalHashAggregate>(
			    op_agg->types, std::move(expressions), std::move(groups), std::move(grouping_sets),
			    std::move(grouping_functions), op_agg->estimated_cardinality);
		groupby->v_column_binding = op_agg->GetColumnBindings();
		for(auto &child : pexpr->children) {
			groupby->AddChild(child->Copy());
		}
		groupby->CE();
		pxfres->Add(std::move(groupby));
	}
}
} // namespace gpopt