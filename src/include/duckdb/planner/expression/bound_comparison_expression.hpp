//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_comparison_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb
{

class BoundComparisonExpression : public Expression
{
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_COMPARISON;

public:
	BoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right);

	unique_ptr<Expression> left;
	unique_ptr<Expression> right;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<Expression> Deserialize(ExpressionDeserializationState &state, FieldReader &reader);

public:
	static LogicalType BindComparison(LogicalType left_type, LogicalType right_type);

public:
	vector<ColumnBinding> GetColumnBinding() override
	{
		vector<ColumnBinding> v;
		v = left->GetColumnBinding();
		vector<ColumnBinding> v1 = right->GetColumnBinding();
		v.insert(v1.begin(), v1.end(), v.end());
		return v;
	}
};
} // namespace duckdb
