#include "duckdb/optimizer/cascade/search/CGroupExpressionCmp.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpopt {
size_t CGroupExpressionCmp::operator()(const duckdb::unique_ptr<CGroupExpression> gexpr1,
                                       const duckdb::unique_ptr<CGroupExpression> gexpr2) const {
		// make sure we are not comparing to invalid group expression
		if (nullptr == gexpr1->m_operator || nullptr == gexpr2->m_operator) {
			return nullptr == gexpr1->m_operator && nullptr == gexpr2->m_operator;
		}
		// have same arity
		if (gexpr1->Arity() != gexpr2->Arity()) {
			return false;
		}
		// match operators
		if (!(gexpr1->m_operator->logical_type == gexpr2->m_operator->logical_type)
			|| !(gexpr1->m_operator->physical_type == gexpr2->m_operator->physical_type)) {
			return false;
		}
		// compare inputs
		if (0 == gexpr1->Arity()) {
			return true;
		} else {
			if (1 == gexpr1->Arity() || gexpr1->m_operator->FInputOrderSensitive()) {
				return CGroup::FMatchGroups(gexpr1->m_child_groups, gexpr2->m_child_groups);
			} else {
				return CGroup::FMatchGroups(gexpr1->m_child_groups_sorted, gexpr2->m_child_groups_sorted);
			}
		}
		return false;
	}
}