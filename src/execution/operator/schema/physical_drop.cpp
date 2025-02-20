#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class DropSourceState : public GlobalSourceState {
public:
	DropSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalDrop::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<DropSourceState>();
}

void PhysicalDrop::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                           LocalSourceState &lstate) const {
	auto &state = gstate.Cast<DropSourceState>();
	if (state.finished) {
		return;
	}
	switch (info->type) {
	case CatalogType::PREPARED_STATEMENT: {
		// DEALLOCATE silently ignores errors
		auto &statements = ClientData::Get(context.client).prepared_statements;
		if (statements.find(info->name) != statements.end()) {
			statements.erase(info->name);
		}
		break;
	}
	case CatalogType::SCHEMA_ENTRY: {
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		catalog.DropEntry(context.client, info.get());
		auto qualified_name = QualifiedName::Parse(info->name);

		// Check if the dropped schema was set as the current schema
		auto &client_data = ClientData::Get(context.client);
		auto &default_entry = client_data.catalog_search_path->GetDefault();
		auto &current_catalog = default_entry.catalog;
		auto &current_schema = default_entry.schema;
		D_ASSERT(info->name != DEFAULT_SCHEMA);

		if (info->catalog == current_catalog && current_schema == info->name) {
			// Reset the schema to default
			SchemaSetting::SetLocal(context.client, DEFAULT_SCHEMA);
		}
		break;
	}
	default: {
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		catalog.DropEntry(context.client, info.get());
		break;
	}
	}
	state.finished = true;
}

} // namespace duckdb
