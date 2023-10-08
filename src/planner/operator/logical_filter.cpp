#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"

namespace duckdb {
LogicalFilter::LogicalFilter()
	: LogicalOperator(LogicalOperatorType::LOGICAL_FILTER)
{
}

LogicalFilter::LogicalFilter(unique_ptr<Expression> expression)
	: LogicalOperator(LogicalOperatorType::LOGICAL_FILTER)
{
	expressions.push_back(std::move(expression));
	SplitPredicates(expressions);
}

void LogicalFilter::ResolveTypes()
{
	types = MapTypes(children[0]->types, projection_map);
}

vector<ColumnBinding> LogicalFilter::GetColumnBindings()
{
	return MapBindings(children[0]->GetColumnBindings(), projection_map);
}

// Split the predicates separated by AND statements
// These are the predicates that are safe to push down because all of them MUST
// be true
bool LogicalFilter::SplitPredicates(vector<unique_ptr<Expression>> &expressions)
{
	bool found_conjunction = false;
	for (idx_t i = 0; i < expressions.size(); i++)
	{
		if (expressions[i]->type == ExpressionType::CONJUNCTION_AND)
		{
			auto &conjunction = expressions[i]->Cast<BoundConjunctionExpression>();
			found_conjunction = true;
			// AND expression, append the other children
			for (idx_t k = 1; k < conjunction.children.size(); k++)
			{
				expressions.push_back(std::move(conjunction.children[k]));
			}
			// replace this expression with the first child of the conjunction
			expressions[i] = std::move(conjunction.children[0]);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
		}
	}
	return found_conjunction;
}

void LogicalFilter::Serialize(FieldWriter &writer) const
{
	writer.WriteSerializableList<Expression>(expressions);
	writer.WriteList<idx_t>(projection_map);
}

unique_ptr<LogicalOperator> LogicalFilter::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	auto projection_map = reader.ReadRequiredList<idx_t>();
	auto result = make_uniq<LogicalFilter>();
	result->expressions = std::move(expressions);
	result->projection_map = std::move(projection_map);
	return std::move(result);
}

CKeyCollection* LogicalFilter::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	return PkcDeriveKeysPassThru(exprhdl, 0);
}

// derive constraint property
CPropConstraint* LogicalFilter::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return PpcDeriveConstraintFromPredicates(exprhdl);
}

// Rehydrate expression from a given cost context and child expressions
Operator* LogicalFilter::SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan)
{
	CGroupExpression* pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	LogicalFilter* pexpr = new LogicalFilter();
	pexpr->expressions = std::move(pgexpr->m_operator->expressions);
	for(auto &child : pdrgpexpr)
	{
		pexpr->AddChild(child->Copy());
	}
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

// Rehydrate expression from a given cost context and child expressions
unique_ptr<Operator> LogicalFilter::Copy()
{
	/* LogicalFilter fields */
	unique_ptr<LogicalFilter> copy = make_uniq<LogicalFilter>();
	
	/* Operator fields */
	copy->m_derived_logical_property = this->m_derived_logical_property;
	copy->m_derived_physical_property = this->m_derived_physical_property;
	copy->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	for (auto &child : this->expressions) {
		copy->expressions.push_back(child->Copy());
	}
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	copy->logical_type = this->logical_type;
	copy->physical_type = this->physical_type;
	for (auto &child : this->children) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = this->m_group_expression;
	copy->m_cost = this->m_cost;
	return unique_ptr_cast<LogicalFilter, Operator>(std::move(copy));
}

unique_ptr<Operator> LogicalFilter::CopyWithNewGroupExpression(CGroupExpression *pgexpr) {
	auto copy = this->Copy();
	copy->m_group_expression = pgexpr;
	return copy;
}

unique_ptr<Operator> LogicalFilter::CopyWithNewChildren(CGroupExpression *pgexpr,
                                        duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                        double cost) {
	/* LogicalFilter fields */
	unique_ptr<LogicalFilter> copy = make_uniq<LogicalFilter>();
	
	/* Operator fields */
	copy->m_derived_logical_property = this->m_derived_logical_property;
	copy->m_derived_physical_property = this->m_derived_physical_property;
	copy->m_required_physical_property = this->m_required_physical_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	for (auto &child : this->expressions) {
		copy->expressions.push_back(child->Copy());
	}
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	copy->logical_type = this->logical_type;
	copy->physical_type = this->physical_type;
	for (auto &child : pdrgpexpr) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = cost;
	return unique_ptr_cast<LogicalFilter, Operator>(std::move(copy));							
}
	
void LogicalFilter::CE() {
	if(!this->children[0]->has_estimated_cardinality) {
		this->children[0]->CE();
	}
	if (this->has_estimated_cardinality) {
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
	this->estimated_cardinality = 0.5 * children[0]->estimated_cardinality;
	return;
}
//---------------------------------------------------------------------------
//	@function:
//		LogicalFilter::XformCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXform_set * LogicalFilter::XformCandidates() const
{
	CXform_set * xform_set = new CXform_set();
	(void) xform_set->set(CXform::ExfFilterImplementation);
	return xform_set;
}
} // namespace duckdb