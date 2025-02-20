#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_serialization.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {
LogicalGet::LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
                       vector<LogicalType> returned_types, vector<string> returned_names)
    : LogicalOperator(LogicalOperatorType::LOGICAL_GET), table_index(table_index), function(std::move(function)),
      bind_data(std::move(bind_data)), returned_types(std::move(returned_types)), names(std::move(returned_names)) {
}

string LogicalGet::GetName() const {
	return StringUtil::Upper(function.name);
}

TableCatalogEntry *LogicalGet::GetTable() const {
	return TableScanFunction::GetTableEntry(function, bind_data.get());
}

string LogicalGet::ParamsToString() const {
	string result;
	for (auto &kv : table_filters.filters) {
		auto &column_index = kv.first;
		auto &filter = kv.second;
		if (column_index < names.size()) {
			result += filter->ToString(names[column_index]);
		}
		result += "\n";
	}
	if (!function.to_string) {
		return string();
	}
	return function.to_string(bind_data.get());
}

vector<ColumnBinding> LogicalGet::GetColumnBindings() {
	if (column_ids.empty()) {
		return {ColumnBinding(table_index, 0)};
	}
	vector<ColumnBinding> result;
	if (projection_ids.empty()) {
		for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
			/* I change here and may cause new bugs */
			result.emplace_back(table_index, col_idx);
		}
	} else {
		for (auto proj_id : projection_ids) {
			result.emplace_back(table_index, proj_id);
		}
	}
	if (!projected_input.empty()) {
		if (children.size() != 1) {
			throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
		}
		auto child_bindings = children[0]->GetColumnBindings();
		for (auto entry : projected_input) {
			D_ASSERT(entry < child_bindings.size());
			result.emplace_back(child_bindings[entry]);
		}
	}
	return result;
}

void LogicalGet::ResolveTypes() {
	if (column_ids.empty()) {
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	if (projection_ids.empty()) {
		for (auto &index : column_ids) {
			if (index == COLUMN_IDENTIFIER_ROW_ID) {
				types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				types.push_back(returned_types[index]);
			}
		}
	} else {
		for (auto &proj_index : projection_ids) {
			auto &index = column_ids[proj_index];
			if (index == COLUMN_IDENTIFIER_ROW_ID) {
				types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				types.push_back(returned_types[index]);
			}
		}
	}
	if (!projected_input.empty()) {
		if (children.size() != 1) {
			throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
		}
		for (auto entry : projected_input) {
			D_ASSERT(entry < children[0]->types.size());
			types.push_back(children[0]->types[entry]);
		}
	}
}

idx_t LogicalGet::EstimateCardinality(ClientContext &context) {
	if (function.cardinality) {
		auto node_stats = function.cardinality(context, bind_data.get());
		if (node_stats && node_stats->has_estimated_cardinality) {
			return node_stats->estimated_cardinality;
		}
	}
	return 1;
}

void LogicalGet::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteRegularSerializableList(returned_types);
	writer.WriteList<string>(names);
	writer.WriteList<column_t>(column_ids);
	writer.WriteList<column_t>(projection_ids);
	writer.WriteSerializable(table_filters);
	FunctionSerializer::SerializeBase<TableFunction>(writer, function, bind_data.get());
	if (!function.serialize) {
		D_ASSERT(!function.deserialize);
		// no serialize method: serialize input values and named_parameters for rebinding purposes
		writer.WriteRegularSerializableList(parameters);
		writer.WriteField<idx_t>(named_parameters.size());
		for (auto &pair : named_parameters) {
			writer.WriteString(pair.first);
			writer.WriteSerializable(pair.second);
		}
		writer.WriteRegularSerializableList(input_table_types);
		writer.WriteList<string>(input_table_names);
	}
	writer.WriteList<column_t>(projected_input);
}

unique_ptr<LogicalOperator> LogicalGet::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto returned_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto returned_names = reader.ReadRequiredList<string>();
	auto column_ids = reader.ReadRequiredList<column_t>();
	auto projection_ids = reader.ReadRequiredList<column_t>();
	auto table_filters = reader.ReadRequiredSerializable<TableFilterSet>();
	unique_ptr<FunctionData> bind_data;
	bool has_deserialize;
	auto function = FunctionSerializer::DeserializeBaseInternal<TableFunction, TableFunctionCatalogEntry>(
	    reader, state.gstate, CatalogType::TABLE_FUNCTION_ENTRY, bind_data, has_deserialize);
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	if (!has_deserialize) {
		D_ASSERT(!bind_data);
		parameters = reader.ReadRequiredSerializableList<Value, Value>();
		auto named_parameters_size = reader.ReadRequired<idx_t>();
		for (idx_t i = 0; i < named_parameters_size; i++) {
			auto first = reader.ReadRequired<string>();
			auto second = reader.ReadRequiredSerializable<Value, Value>();
			auto pair = make_pair(first, second);
			named_parameters.insert(pair);
		}
		input_table_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
		input_table_names = reader.ReadRequiredList<string>();
		TableFunctionBindInput input(parameters, named_parameters, input_table_types, input_table_names,
		                             function.function_info.get());
		vector<LogicalType> bind_return_types;
		vector<string> bind_names;
		bind_data = function.bind(state.gstate.context, input, bind_return_types, bind_names);
		if (returned_types != bind_return_types) {
			throw SerializationException(
			    "Table function deserialization failure - bind returned different return types than were serialized");
		}
		// names can actually be different because of aliases - only the sizes cannot be different
		if (returned_names.size() != bind_names.size()) {
			throw SerializationException(
			    "Table function deserialization failure - bind returned different returned names than were serialized");
		}
	}
	vector<column_t> projected_input;
	reader.ReadList<column_t>(projected_input);
	auto result = make_uniq<LogicalGet>(table_index, function, std::move(bind_data), returned_types, returned_names);
	result->column_ids = std::move(column_ids);
	result->projection_ids = std::move(projection_ids);
	result->table_filters = std::move(*table_filters);
	result->parameters = std::move(parameters);
	result->named_parameters = std::move(named_parameters);
	result->input_table_types = input_table_types;
	result->input_table_names = input_table_names;
	result->projected_input = std::move(projected_input);
	return std::move(result);
}

vector<idx_t> LogicalGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

size_t LogicalGet::HashValue() const {
	size_t ulLogicalType = (size_t)logical_type;
	size_t ulPhysicalType = (size_t)physical_type;
	size_t ulHash = duckdb::CombineHash(duckdb::Hash<size_t>(ulLogicalType), duckdb::Hash<size_t>(ulPhysicalType));
	ulHash = duckdb::CombineHash(ulHash, duckdb::Hash<idx_t>(table_index));
	std::string str = ParamsToString();
	size_t ulHash2 = std::hash<std::string> {}(str);
	ulHash = duckdb::CombineHash(ulHash, ulHash2);
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalGet::XformCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXform_set *LogicalGet::XformCandidates() const {
	CXform_set *xform_set = new CXform_set();
	(void)xform_set->set(CXform::ExfGet2TableScan);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalColumnDataGet::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *LogicalGet::DerivePropertyConstraint(CExpressionHandle &exprhdl) {
	return nullptr;
	// return PpcDeriveConstraintPassThru(exprhdl, 0);
}

// Rehydrate expression from a given cost context and child expressions
Operator *LogicalGet::SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
                                    CDrvdPropCtxtPlan *pdpctxtplan) {
	CGroupExpression *pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	LogicalGet *pexpr = new LogicalGet(table_index, function, std::move(tmp_bind_data), returned_types, names);
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

unique_ptr<Operator> LogicalGet::Copy() {
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	unique_ptr<LogicalGet> result =
	    make_uniq<LogicalGet>(table_index, this->function, std::move(tmp_bind_data), returned_types, names);
	result->m_derived_logical_property = m_derived_logical_property;
	result->m_derived_physical_property = m_derived_physical_property;
	result->m_required_physical_property = m_required_physical_property;
	if (nullptr != estimated_props) {
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	for (auto &child : expressions) {
		result->expressions.push_back(child->Copy());
	}
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for (auto &child : children) {
		result->AddChild(child->Copy());
	}
	result->m_group_expression = m_group_expression;
	result->m_cost = m_cost;
	result->column_ids.assign(column_ids.begin(), column_ids.end());
	result->projection_ids.assign(projection_ids.begin(), projection_ids.end());
	for (auto &child : table_filters.filters) {
		result->table_filters.filters.insert(make_pair(child.first, child.second->Copy()));
	}
	result->parameters.assign(parameters.begin(), parameters.end());
	result->named_parameters = named_parameters;
	result->input_table_types.assign(input_table_types.begin(), input_table_types.end());
	result->input_table_names.assign(input_table_names.begin(), input_table_names.end());
	result->projected_input.assign(projected_input.begin(), projected_input.end());
	return unique_ptr_cast<LogicalGet, Operator>(std::move(result));
}

unique_ptr<Operator> LogicalGet::CopyWithNewGroupExpression(CGroupExpression *pgexpr) {
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	unique_ptr<LogicalGet> result =
	    make_uniq<LogicalGet>(table_index, this->function, std::move(tmp_bind_data), returned_types, names);
	result->m_derived_logical_property = m_derived_logical_property;
	result->m_derived_physical_property = m_derived_physical_property;
	result->m_required_physical_property = m_required_physical_property;
	if (nullptr != estimated_props) {
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	for (auto &child : expressions) {
		result->expressions.push_back(child->Copy());
	}
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for (auto &child : children) {
		result->AddChild(child->Copy());
	}
	result->m_group_expression = pgexpr;
	result->m_cost = m_cost;
	result->column_ids.assign(column_ids.begin(), column_ids.end());
	result->projection_ids.assign(projection_ids.begin(), projection_ids.end());
	for (auto &child : table_filters.filters) {
		result->table_filters.filters.insert(make_pair(child.first, child.second->Copy()));
	}
	result->parameters.assign(parameters.begin(), parameters.end());
	result->named_parameters = named_parameters;
	result->input_table_types.assign(input_table_types.begin(), input_table_types.end());
	result->input_table_names.assign(input_table_names.begin(), input_table_names.end());
	result->projected_input.assign(projected_input.begin(), projected_input.end());
	return unique_ptr_cast<LogicalGet, Operator>(std::move(result));
}

unique_ptr<Operator> LogicalGet::CopyWithNewChildren(CGroupExpression *pgexpr,
                                                     duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                                     double cost) {
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	unique_ptr<LogicalGet> result =
	    make_uniq<LogicalGet>(table_index, this->function, std::move(tmp_bind_data), returned_types, names);
	result->m_derived_logical_property = m_derived_logical_property;
	result->m_derived_physical_property = m_derived_physical_property;
	result->m_required_physical_property = m_required_physical_property;
	if (nullptr != estimated_props) {
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	for (auto &child : expressions) {
		result->expressions.push_back(child->Copy());
	}
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for(auto &child : pdrgpexpr) {
		result->AddChild(std::move(child));
	}
	result->m_group_expression = pgexpr;
	result->m_cost = cost;
	result->column_ids.assign(column_ids.begin(), column_ids.end());
	result->projection_ids.assign(projection_ids.begin(), projection_ids.end());
	for (auto &child : table_filters.filters) {
		result->table_filters.filters.insert(make_pair(child.first, child.second->Copy()));
	}
	result->parameters.assign(parameters.begin(), parameters.end());
	result->named_parameters = named_parameters;
	result->input_table_types.assign(input_table_types.begin(), input_table_types.end());
	result->input_table_names.assign(input_table_names.begin(), input_table_names.end());
	result->projected_input.assign(projected_input.begin(), projected_input.end());
	return unique_ptr_cast<LogicalGet, Operator>(std::move(result));
}

idx_t LogicalGet::GetChildrenRelIds() {
	idx_t res = 1 << (table_index + 1);
	return res;
}

void LogicalGet::CE() {
	if(this->has_estimated_cardinality) {
		return;
	}
	idx_t relids = this->GetChildrenRelIds();
	char* pos;
	char* p;
	char cmp[1000];
	int relid_in_file;
	FILE* fp = fopen("/root/giveDuckTopDownOptimizer/optimal/query.txt", "r+");
	while(fgets(cmp, 1000, fp) != NULL) {
		if((pos = strchr(cmp, '\n')) != NULL) {
			*pos = '\0';
		}
		p = strtok(cmp, ":");
		relid_in_file = atoi(p);
		if(relid_in_file == relids) {
			p = strtok(NULL, ":");
			double true_val = atof(p);
			if(true_val < 9999999999999.0) {
				fclose(fp);
				this->has_estimated_cardinality = true;
				this->estimated_cardinality = true_val;
				return;
			}
		}
	}
	fclose(fp);
	this->has_estimated_cardinality = true;
	this->estimated_cardinality = static_cast<double>(rand() % 1000);
	return;
}
} // namespace duckdb