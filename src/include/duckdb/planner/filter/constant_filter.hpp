//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/constant_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

class ConstantFilter : public TableFilter {
public:
	ConstantFilter(ExpressionType comparison_type, Value constant);

	//! The comparison type (e.g. COMPARE_EQUAL, COMPARE_GREATERTHAN, COMPARE_LESSTHAN, ...)
	ExpressionType comparison_type;
	//! The constant value to filter on
	Value constant;

public:
	unique_ptr<TableFilter> Copy() override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<TableFilter> Deserialize(FieldReader &source);
};

} // namespace duckdb
