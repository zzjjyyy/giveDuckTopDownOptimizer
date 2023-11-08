//---------------------------------------------------------------------------
//	@filename:
//		CXformJoinCommutativity.cpp
//
//	@doc:
//		Implementation of join commute transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformJoinAssociativity.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/optimizer/cascade/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformJoinAssociativity::CXformJoinAssociativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformJoinAssociativity::CXformJoinAssociativity()
	: CXformExploration(make_uniq<LogicalComparisonJoin>(JoinType::INNER))
{
    duckdb::unique_ptr<LogicalComparisonJoin> left_child = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    left_child->AddChild(make_uniq<CPatternLeaf>());
    left_child->AddChild(make_uniq<CPatternLeaf>());
    // Need to delete
	// this->m_operator->AddChild(std::move(left_child));
    this->m_operator->AddChild(left_child);
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
CXform::EXformPromise CXformJoinAssociativity::XformPromise(CExpressionHandle &exprhdl) const {
	return CXform::ExfpMedium;
}

void CXformJoinAssociativity::CreatePredicates(duckdb::unique_ptr<Operator> join,
                                               duckdb::vector<JoinCondition> &upper_join_condition,
                                               duckdb::vector<JoinCondition> &lower_join_condition) const {
    auto UpperJoin = unique_ptr_cast<Operator, LogicalComparisonJoin>(join);
    auto LowerJoin =
        unique_ptr_cast<Operator, LogicalComparisonJoin>(UpperJoin->children[0]);
    duckdb::vector<ColumnBinding> NewLowerOutputCols;
    auto LowerLeftOutputCols = LowerJoin->children[0]->GetColumnBindings();
    auto UpperRightOutputCols = UpperJoin->children[1]->GetColumnBindings();
    NewLowerOutputCols.insert(NewLowerOutputCols.end(), LowerLeftOutputCols.begin(), LowerLeftOutputCols.end());
    NewLowerOutputCols.insert(NewLowerOutputCols.end(), UpperRightOutputCols.begin(), UpperRightOutputCols.end());
    for (auto &child : UpperJoin->conditions) {
        if (CUtils::ContainsAll(NewLowerOutputCols, child.left->GetColumnBinding())
            && CUtils::ContainsAll(NewLowerOutputCols, child.right->GetColumnBinding())) {
            JoinCondition jc;
            // Need to delete
            // jc.left = child.left->Copy();
            // jc.right = child.right->Copy();
            jc.left = child.left;
            jc.right = child.right;
            jc.comparison = child.comparison;
            lower_join_condition.emplace_back(std::move(jc));
        }
    }
    for (auto &child : UpperJoin->conditions) {
        if (CUtils::ContainsAll(NewLowerOutputCols, child.left->GetColumnBinding())
            && CUtils::ContainsAll(NewLowerOutputCols, child.right->GetColumnBinding())) {
            continue;
        }
        JoinCondition jc;
        // Need to delete
        // jc.left = child.right->Copy();
        // jc.right = child.left->Copy();
        jc.left = child.right;
        jc.right = child.left;
        jc.comparison = child.comparison;
        upper_join_condition.emplace_back(std::move(jc));
    }
    for (auto &child : LowerJoin->conditions) {
        JoinCondition jc;
        // Need to delete
        // jc.left = child.left->Copy();
        // jc.right = child.right->Copy();
        jc.left = child.left;
        jc.right = child.right;
        jc.comparison = child.comparison;
        upper_join_condition.emplace_back(std::move(jc));
    }
}

//---------------------------------------------------------------------------
//	@function:
//		CXformJoinAssociativity::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformJoinAssociativity::Transform(duckdb::unique_ptr<CXformContext> pxfctxt,
                                        duckdb::unique_ptr<CXformResult> pxfres,
                                        duckdb::unique_ptr<Operator> pexpr) const {
    // enumeration_pairs++;
	auto UpperJoin = unique_ptr_cast<Operator, LogicalComparisonJoin>(pexpr);
    auto LowerJoin = unique_ptr_cast<Operator, LogicalComparisonJoin>(pexpr->children[0]);
    duckdb::vector<JoinCondition> NewUpperJoinCondition;
    duckdb::vector<JoinCondition> NewLowerJoinCondition;
    CreatePredicates(UpperJoin, NewUpperJoinCondition, NewLowerJoinCondition);
    if(NewUpperJoinCondition.size() == 0 || NewLowerJoinCondition.size() == 0)
        return;
    duckdb::unique_ptr<LogicalComparisonJoin> NewLowerJoin = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    // Need to delete
    // NewLowerJoin->AddChild(LowerJoin->children[0]->Copy());
    // NewLowerJoin->AddChild(UpperJoin->children[1]->Copy());
    NewLowerJoin->AddChild(LowerJoin->children[0]);
    NewLowerJoin->AddChild(UpperJoin->children[1]);
    NewLowerJoin->conditions = std::move(NewLowerJoinCondition);
    NewLowerJoin->ResolveTypes();
    duckdb::unique_ptr<LogicalComparisonJoin> NewUpperJoin = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    NewUpperJoin->AddChild(std::move(NewLowerJoin));
    // Need to delete
    // NewUpperJoin->AddChild(LowerJoin->children[1]->Copy());
    NewUpperJoin->AddChild(LowerJoin->children[1]);
    NewUpperJoin->conditions = std::move(NewUpperJoinCondition);
    NewUpperJoin->ResolveTypes();
    // Cardinality Estimation
	// NewUpperJoin->CE();
	// add alternative to transformation result
	pxfres->Add(std::move(NewUpperJoin));
}