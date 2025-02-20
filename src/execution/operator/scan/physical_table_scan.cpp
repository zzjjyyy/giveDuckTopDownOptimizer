#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <cstdlib>
#include <utility>

namespace duckdb {
using namespace gpopt;

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)),
      names(std::move(names_p)), table_filters(std::move(table_filters_p)) {
}

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                     vector<column_t> column_ids_p, vector<idx_t> projection_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
      column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)),
      table_filters(std::move(table_filters_p)) {
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.function.init_global) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
	}
	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;
	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
	}
	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

void PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                LocalSourceState &lstate) const {
	D_ASSERT(!column_ids.empty());
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	function.function(context.client, data, chunk);
}

double PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	if (function.table_scan_progress) {
		return function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
	}
	// if table_scan_progress is not implemented we don't support this function yet in the progress bar
	return -1;
}

idx_t PhysicalTableScan::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                       LocalSourceState &lstate) const {
	D_ASSERT(SupportsBatchIndex());
	D_ASSERT(function.get_batch_index);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(),
	                                gstate.global_state.get());
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name + " " + function.extra_info);
}

string PhysicalTableScan::ParamsToString() const {
	string result;
	if (function.to_string) {
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown) {
		if (function.filter_prune) {
			for (idx_t i = 0; i < projection_ids.size(); i++) {
				const auto &column_id = column_ids[projection_ids[i]];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		} else {
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column_id = column_ids[i];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		}
	}
	if (function.filter_pushdown && table_filters) {
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_props->GetCardinality<idx_t>());
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (physical_type != other_p.physical_type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function.function != other.function.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

size_t PhysicalTableScan::HashValue() const
{
	size_t ulLogicalType = (size_t)logical_type;
	size_t ulPhysicalType = (size_t)physical_type;
	size_t ulHash = duckdb::CombineHash(duckdb::Hash<size_t>(ulLogicalType), duckdb::Hash<size_t>(ulPhysicalType));
	std::string str = ParamsToString();
	size_t ulHash2 = std::hash<std::string>{}(str);
	ulHash = duckdb::CombineHash(ulHash, ulHash2);
	size_t ulHash3 = duckdb::Hash<size_t>(v_column_binding[0].table_index);
	ulHash = duckdb::CombineHash(ulHash, ulHash3);
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		PhysicalTableScan::EnforcingTypeOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
COrderProperty::EPropEnforcingType PhysicalTableScan::EnforcingTypeOrder(CExpressionHandle &exprhdl,
                                                            vector<BoundOrderByNode> &peo) const {
	return COrderProperty::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		PhysicalTableScan::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
bool PhysicalTableScan::FProvidesReqdCols(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrsRequired,
                                          ULONG ulOptReq) const {
	bool result = CUtils::ContainsAll(v_column_binding, pcrsRequired);
	return result;
}

CKeyCollection *PhysicalTableScan::DeriveKeyCollection(CExpressionHandle &exprhdl) {
	return nullptr;
}

CPropConstraint *PhysicalTableScan::DerivePropertyConstraint(CExpressionHandle &exprhdl) {
	return nullptr;
}

ULONG PhysicalTableScan::DeriveJoinDepth(CExpressionHandle &exprhdl) {
	return 1;
}

// Rehydrate expression from a given cost context and child expressions
Operator *PhysicalTableScan::SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
                                           CDrvdPropCtxtPlan *pdpctxtplan) {
	CGroupExpression *pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	// if (!table_filters->filters.empty())
	// {
	// 	copy_table_filters = CreateTableFilterSet(*table_filters, column_ids);
	// }
	TableFunction tmp_function(function.name, function.arguments, function.function, function.bind,
	                           function.init_global, function.init_local);
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	PhysicalTableScan *pexpr = new PhysicalTableScan(returned_types, tmp_function, std::move(tmp_bind_data), column_ids,
	                                                 names, std::move(table_filters), 1);
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

duckdb::unique_ptr<Operator> PhysicalTableScan::Copy() {
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(this->bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = this->bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	if(this->table_filters != nullptr) {
		table_filters = make_uniq<TableFilterSet>();
		for(auto &child : this->table_filters->filters) {
			table_filters->filters.insert(make_pair(child.first, child.second->Copy()));
		}
	}
	
	/* PhysicalTableScan fields */
	unique_ptr<PhysicalTableScan> result = make_uniq<PhysicalTableScan>(
	    this->returned_types, this->function, std::move(tmp_bind_data), this->column_ids, this->names, std::move(table_filters), 1);
	
	/* PhysicalOperator fields */
	result->v_column_binding = this->v_column_binding;

	/* Operator fields */
	result->m_derived_logical_property = this->m_derived_logical_property;
	result->m_derived_physical_property = this->m_derived_physical_property;
	result->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		result->estimated_props = this->estimated_props->Copy();
	}
	result->types = this->types;
	result->estimated_cardinality = this->estimated_cardinality;
	result->has_estimated_cardinality = this->has_estimated_cardinality;
	for (auto &child : this->children) {
		result->AddChild(child->Copy());
	}
	result->m_group_expression = this->m_group_expression;
	result->m_cost = this->m_cost;
	return unique_ptr_cast<PhysicalTableScan, Operator>(std::move(result));
}

duckdb::unique_ptr<Operator> PhysicalTableScan::CopyWithNewGroupExpression(CGroupExpression *pgexpr) {
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(this->bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = this->bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	if(this->table_filters != nullptr) {
		table_filters = make_uniq<TableFilterSet>();
		for(auto &child : this->table_filters->filters) {
			table_filters->filters.insert(make_pair(child.first, child.second->Copy()));
		}
	}

	/* PhysicalTableScan fields */
	unique_ptr<PhysicalTableScan> result = make_uniq<PhysicalTableScan>(
	    this->returned_types, this->function, std::move(tmp_bind_data),
		this->column_ids, this->names, std::move(table_filters), 1);

	/* PhysicalOperator fields */
	result->v_column_binding = this->v_column_binding;
	
	/* Operator fields */
	result->m_derived_logical_property = this->m_derived_logical_property;
	result->m_derived_physical_property = this->m_derived_physical_property;
	result->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		result->estimated_props = this->estimated_props->Copy();
	}
	result->types = this->types;
	result->estimated_cardinality = this->estimated_cardinality;
	result->has_estimated_cardinality = this->has_estimated_cardinality;
	for (auto &child :  this->children) {
		result->AddChild(child->Copy());
	}
	result->m_group_expression = pgexpr;
	result->m_cost = this->m_cost;
	return unique_ptr_cast<PhysicalTableScan, Operator>(std::move(result));
}

duckdb::unique_ptr<Operator>
PhysicalTableScan::CopyWithNewChildren(CGroupExpression *pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                       double cost) {
	unique_ptr<TableScanBindData> tmp_bind_data =
	    make_uniq<TableScanBindData>(this->bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = this->bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	if(this->table_filters != nullptr) {
		table_filters = make_uniq<TableFilterSet>();
		for(auto &child : this->table_filters->filters) {
			table_filters->filters.insert(make_pair(child.first, child.second->Copy()));
		}
	}

	/* PhysicalTableScan fields */
	unique_ptr<PhysicalTableScan> result = make_uniq<PhysicalTableScan>(
	    this->returned_types, this->function, std::move(tmp_bind_data), this->column_ids,
		this->names, std::move(table_filters), 1);

	/* PhysicalOperator fields */
	result->v_column_binding = this->v_column_binding;

	/* Operator fields */
	result->m_derived_logical_property = this->m_derived_logical_property;
	result->m_derived_physical_property = this->m_derived_physical_property;
	result->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		result->estimated_props = this->estimated_props->Copy();
	}
	result->types = this->types;
	result->estimated_cardinality = this->estimated_cardinality;
	result->has_estimated_cardinality = this->has_estimated_cardinality;
	for (auto &child : pdrgpexpr) {
		result->AddChild(std::move(child));
	}
	result->m_group_expression = pgexpr;
	result->m_cost = cost;
	return unique_ptr_cast<PhysicalTableScan, Operator>(std::move(result));
}

idx_t PhysicalTableScan::GetChildrenRelIds() {
	idx_t res = 1 << (v_column_binding[0].table_index + 1);
	return res;
}

void PhysicalTableScan::CE() {
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