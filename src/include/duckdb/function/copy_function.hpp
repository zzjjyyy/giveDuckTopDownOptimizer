//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/copy_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"

namespace duckdb {

class Binder;
struct BoundStatement;
class ExecutionContext;

struct LocalFunctionData {
	virtual ~LocalFunctionData() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return (TARGET &)*this;
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return (const TARGET &)*this;
	}
};

struct GlobalFunctionData {
	virtual ~GlobalFunctionData() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return (TARGET &)*this;
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return (const TARGET &)*this;
	}
};

typedef BoundStatement (*copy_to_plan_t)(Binder &binder, CopyStatement &stmt);
typedef unique_ptr<FunctionData> (*copy_to_bind_t)(ClientContext &context, CopyInfo &info, vector<string> &names,
                                                   vector<LogicalType> &sql_types);
typedef unique_ptr<LocalFunctionData> (*copy_to_initialize_local_t)(ExecutionContext &context, FunctionData &bind_data);
typedef unique_ptr<GlobalFunctionData> (*copy_to_initialize_global_t)(ClientContext &context, FunctionData &bind_data,
                                                                      const string &file_path);
typedef void (*copy_to_sink_t)(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                               LocalFunctionData &lstate, DataChunk &input);
typedef void (*copy_to_combine_t)(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                  LocalFunctionData &lstate);
typedef void (*copy_to_finalize_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate);

typedef void (*copy_to_serialize_t)(FieldWriter &writer, const FunctionData &bind_data, const CopyFunction &function);

typedef unique_ptr<FunctionData> (*copy_to_deserialize_t)(ClientContext &context, FieldReader &reader,
                                                          CopyFunction &function);

typedef unique_ptr<FunctionData> (*copy_from_bind_t)(ClientContext &context, CopyInfo &info,
                                                     vector<string> &expected_names,
                                                     vector<LogicalType> &expected_types);
typedef bool (*copy_to_is_parallel_t)(ClientContext &context, FunctionData &bind_data);

class CopyFunction : public Function {
public:
	explicit CopyFunction(string name)
	    : Function(name), plan(nullptr), copy_to_bind(nullptr), copy_to_initialize_local(nullptr),
	      copy_to_initialize_global(nullptr), copy_to_sink(nullptr), copy_to_combine(nullptr),
	      copy_to_finalize(nullptr), parallel(nullptr), serialize(nullptr), deserialize(nullptr),
	      copy_from_bind(nullptr) {
	}

	//! Plan rewrite copy function
	copy_to_plan_t plan;

	copy_to_bind_t copy_to_bind;
	copy_to_initialize_local_t copy_to_initialize_local;
	copy_to_initialize_global_t copy_to_initialize_global;
	copy_to_sink_t copy_to_sink;
	copy_to_combine_t copy_to_combine;
	copy_to_finalize_t copy_to_finalize;
	copy_to_is_parallel_t parallel;

	copy_to_serialize_t serialize;
	copy_to_deserialize_t deserialize;

	copy_from_bind_t copy_from_bind;
	TableFunction copy_from_function;

	string extension;
};

} // namespace duckdb
