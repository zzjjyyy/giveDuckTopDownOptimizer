#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb
{
LogicalJoin::LogicalJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalOperator(logical_type), join_type(join_type)
{
}

vector<ColumnBinding> LogicalJoin::GetColumnBindings()
{
	auto left_bindings = MapBindings(children[0]->GetColumnBindings(), left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI)
	{
		// for SEMI and ANTI join we only project the left hand side
		return left_bindings;
	}
	if (join_type == JoinType::MARK)
	{
		// for MARK join we project the left hand side plus the MARK column
		left_bindings.emplace_back(mark_index, 0);
		return left_bindings;
	}
	// for other join types we project both the LHS and the RHS
	auto right_bindings = MapBindings(children[1]->GetColumnBindings(), right_projection_map);
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalJoin::ResolveTypes()
{
	types = MapTypes(children[0]->types, left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI)
	{
		// for SEMI and ANTI join we only project the left hand side
		return;
	}
	if (join_type == JoinType::MARK)
	{
		// for MARK join we project the left hand side, plus a BOOLEAN column indicating the MARK
		types.emplace_back(LogicalType::BOOLEAN);
		return;
	}
	// for any other join we project both sides
	auto right_types = MapTypes(children[1]->types, right_projection_map);
	types.insert(types.end(), right_types.begin(), right_types.end());
}

void LogicalJoin::GetTableReferences(LogicalOperator &op, unordered_set<idx_t> &bindings)
{
	auto column_bindings = op.GetColumnBindings();
	for (auto binding : column_bindings) {
		bindings.insert(binding.table_index);
	}
}

void LogicalJoin::GetExpressionBindings(Expression &expr, unordered_set<idx_t> &bindings)
{
	if (expr.type == ExpressionType::BOUND_COLUMN_REF)
	{
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		bindings.insert(colref.binding.table_index);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { GetExpressionBindings(child, bindings); });
}

void LogicalJoin::Serialize(FieldWriter &writer) const
{
	writer.WriteField<JoinType>(join_type);
	writer.WriteField<idx_t>(mark_index);
	writer.WriteList<idx_t>(left_projection_map);
	writer.WriteList<idx_t>(right_projection_map);
	//	writer.WriteSerializableList(join_stats);
}

void LogicalJoin::Deserialize(LogicalJoin &join, LogicalDeserializationState &state, FieldReader &reader)
{
	join.join_type = reader.ReadRequired<JoinType>();
	join.mark_index = reader.ReadRequired<idx_t>();
	join.left_projection_map = reader.ReadRequiredList<idx_t>();
	join.right_projection_map = reader.ReadRequiredList<idx_t>();
	//	join.join_stats = reader.ReadRequiredSerializableList<BaseStatistics>(reader.GetSource());
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalJoin::XformCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXform_set * LogicalJoin::XformCandidates() const
{
	CXform_set * xform_set = new CXform_set();
	if(join_type == JoinType::INNER)
	{
		(void) xform_set->set(CXform::ExfInnerJoin2NLJoin);
		(void) xform_set->set(CXform::ExfInnerJoin2HashJoin);
		(void) xform_set->set(CXform::ExfSubqJoin2Apply);
		(void) xform_set->set(CXform::ExfInnerJoin2IndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoin2DynamicIndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoin2PartialDynamicIndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoin2BitmapIndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoinWithInnerSelect2IndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoinWithInnerSelect2DynamicIndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoin2DynamicBitmapIndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoinWithInnerSelect2BitmapIndexGetApply);
		(void) xform_set->set(CXform::ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply);
		(void) xform_set->set(CXform::ExfJoinCommutativity);
		(void) xform_set->set(CXform::ExfJoinAssociativity);
		(void) xform_set->set(CXform::ExfInnerJoinSemiJoinSwap);
		(void) xform_set->set(CXform::ExfInnerJoinAntiSemiJoinSwap);
		(void) xform_set->set(CXform::ExfInnerJoinAntiSemiJoinNotInSwap);
	}
	return xform_set;
}
} // namespace duckdb
