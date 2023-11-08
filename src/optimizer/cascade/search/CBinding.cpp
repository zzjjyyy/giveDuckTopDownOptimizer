//---------------------------------------------------------------------------
//	@filename:
//		CBinding.cpp
//
//	@doc:
//		Implementation of Binding structure
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CBinding.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"

#include <algorithm>

using namespace std;

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CBinding::NextGroupExpr
//
//	@doc:
//		Move cursor within a group (initialize if nullptr)
//
//---------------------------------------------------------------------------
list<duckdb::unique_ptr<CGroupExpression>>::iterator
CBinding::PgexprNext(duckdb::unique_ptr<CGroup> pgroup,
					 duckdb::unique_ptr<CGroupExpression> pgexpr) const {
	CGroupProxy gp(pgroup);
	if (nullptr == pgexpr) {
		return gp.PgexprFirst();
	}
	auto itr = std::find(gp.m_pgroup->m_group_exprs.begin(), gp.m_pgroup->m_group_exprs.end(), pgexpr);
	if (pgroup->m_is_scalar) {
		return ++itr;
	}
	// for non-scalar group, we only consider logical expressions in bindings
	return gp.PgexprNextLogical(++itr);
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprExpandPattern
//
//	@doc:
//		Pattern operators which match more than one operator need to be
//		passed around;
//		Given the pattern determine if we need to re-use the pattern operators;
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<Operator>
CBinding::PexprExpandPattern(duckdb::unique_ptr<Operator> pexprPattern,
							 ULONG ulPos, ULONG arity) {
	// re-use tree pattern
	if (pexprPattern->FPattern() && !(unique_ptr_cast<Operator, CPattern>(pexprPattern))->FLeaf())
	{
		return pexprPattern;
	}
	// re-use first child if it is a multi-leaf/tree
	if (0 < pexprPattern->Arity()) {
		if (ulPos == arity - 1) {
			// special-case last child
			return pexprPattern->children[pexprPattern->Arity() - 1];
		}
		// otherwise re-use multi-leaf/tree child
		return pexprPattern->children[0];
	}
	return pexprPattern->children[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprFinalize
//
//	@doc:
//		Assemble expression; substitute operator with pattern as necessary
//
//---------------------------------------------------------------------------
// Need to delete
// Operator *CBinding::PexprFinalize(CGroupExpression *pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr) {
duckdb::unique_ptr<Operator>
CBinding::PexprFinalize(duckdb::unique_ptr<CGroupExpression> pgexpr,
						duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr) {
	pgexpr->m_operator->children.clear();
	// Need to delete
	// for (auto &child : pdrgpexpr) {
	for (auto child : pdrgpexpr) {
		// Need to delete
		// pgexpr->m_operator->AddChild(std::move(child));
		pgexpr->m_operator->AddChild(child);
	}
	pgexpr->m_operator->m_cost = GPOPT_INVALID_COST;
	pgexpr->m_operator->m_group_expression = pgexpr;
	// Need to delete
	// return pgexpr->m_operator.get();
	return pgexpr->m_operator;
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprExtract
//
//	@doc:
//		Extract a binding according to a given pattern;
//		Keep root node fixed;
//
//---------------------------------------------------------------------------
// Need to delete
// Operator *CBinding::PexprExtract(CGroupExpression *pgexpr, Operator *pexprPattern, Operator *pexprLast) {
duckdb::unique_ptr<Operator> CBinding::PexprExtract(duckdb::unique_ptr<CGroupExpression> pgexpr, duckdb::unique_ptr<Operator> pexprPattern, duckdb::unique_ptr<Operator> pexprLast) {
	if (!pexprPattern->FMatchPattern(pgexpr)) {
		// shallow matching fails
		return nullptr;
	}
	if (pexprPattern->FPattern() && (unique_ptr_cast<Operator, CPattern>(pexprPattern))->FLeaf()) {
		// return immediately; no deep extraction for leaf patterns
		// Need to delete
		// return pgexpr->m_operator.get();
		return pgexpr->m_operator;
	}
	// for a scalar operator, there is always only one group expression in it's
	// group. scalar operators are required to derive the scalar properties only
	// and no xforms are applied to them (i.e no XformCandidates in scalar op)
	// specifically which will generate equivalent scalar operators in the same group.
	// so, if a scalar op been extracted once, there is no need to explore
	// all the child bindings, as the scalar properites will remain the same.
	if (nullptr != pexprLast && pgexpr->m_group->m_is_scalar) {
		return nullptr;
	}
	duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr;
	ULONG arity = pgexpr->Arity();
	if (0 == arity && nullptr != pexprLast) {
		// no more bindings
		return nullptr;
	} else {
		// attempt binding to children
		if (!FExtractChildren(pgexpr, pexprPattern, pexprLast, pdrgpexpr)) {
			return nullptr;
		}
	}
	// Need to delete
	// Operator *pexpr = PexprFinalize(pgexpr, std::move(pdrgpexpr));
	auto pexpr = PexprFinalize(pgexpr, pdrgpexpr);
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::FInitChildCursors
//
//	@doc:
//		Initialize cursors of child expressions
//
//---------------------------------------------------------------------------
bool CBinding::FInitChildCursors(duckdb::unique_ptr<CGroupExpression> pgexpr,
								 duckdb::unique_ptr<Operator> pexprPattern,
                                 duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr) {
	const ULONG arity = pgexpr->Arity();
	// grab first expression from each cursor
	for (ULONG ul = 0; ul < arity; ul++) {
		auto pgroup = (*pgexpr)[ul];
		auto pexprPatternChild = PexprExpandPattern(pexprPattern, ul, arity);
		auto pexprNewChild = PexprExtract(pgroup, pexprPatternChild, nullptr);
		if (nullptr == pexprNewChild) {
			// failure means we have no more expressions
			return false;
		}
		// Need to delete
		// pdrgpexpr.emplace_back(pexprNewChild->Copy());
		pdrgpexpr.emplace_back(pexprNewChild);
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::FAdvanceChildCursors
//
//	@doc:
//		Advance cursors of child expressions and populate the given array
//		with the next child expressions
//
//---------------------------------------------------------------------------
bool CBinding::FAdvanceChildCursors(duckdb::unique_ptr<CGroupExpression> pgexpr,
									duckdb::unique_ptr<Operator> pexprPattern,
									duckdb::unique_ptr<Operator> pexprLast,
                                    duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr) {
	const ULONG arity = pgexpr->Arity();
	if (nullptr == pexprLast) {
		// first call, initialize cursors
		return FInitChildCursors(pgexpr, pexprPattern, pdrgpexpr);
	}
	// could we advance a child's cursor?
	bool fCursorAdvanced = false;
	// number of exhausted cursors
	ULONG ulExhaustedCursors = 0;
	for (ULONG ul = 0; ul < arity; ul++) {
		auto pgroup = (*pgexpr)[ul];
		auto pexprPatternChild = PexprExpandPattern(pexprPattern, ul, arity);
		// Need to delete
		// Operator *pexprNewChild = nullptr;
		duckdb::unique_ptr<Operator> pexprNewChild;
		if (fCursorAdvanced) {
			// re-use last extracted child expression
			// Need to delete
			// pexprNewChild = pexprLast->children[ul].get();
			pexprNewChild = pexprLast->children[ul];
		} else {
			auto pexprLastChild = pexprLast->children[ul];
			// advance current cursor
			pexprNewChild = PexprExtract(pgroup, pexprPatternChild, pexprLastChild);
			if (nullptr == pexprNewChild) {
				// cursor is exhausted, we need to reset it
				pexprNewChild = PexprExtract(pgroup, pexprPatternChild, nullptr);
				ulExhaustedCursors++;
			} else {
				// advancing current cursor has succeeded
				fCursorAdvanced = true;
			}
		}
		// Need to delete
		// pdrgpexpr.emplace_back(pexprNewChild->Copy());
		pdrgpexpr.emplace_back(pexprNewChild);
	}
	return ulExhaustedCursors < arity;
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::FExtractChildren
//
//	@doc:
//		For a given root, extract children into a dynamic array;
//		Allocates the array for the children as needed;
//
//---------------------------------------------------------------------------
bool CBinding::FExtractChildren(duckdb::unique_ptr<CGroupExpression> pgexpr,
								duckdb::unique_ptr<Operator> pexprPattern,
								duckdb::unique_ptr<Operator> pexprLast,
                                duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr) {
	ULONG arity = pgexpr->Arity();
	if (arity < pexprPattern->Arity()) {
		// does not have enough children
		return false;
	}
	if (0 == arity) {
		return true;
	}
	return FAdvanceChildCursors(pgexpr, pexprPattern, pexprLast, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprExtract
//
//	@doc:
//		Extract a binding according to a given pattern;
//		If no appropriate child pattern can be matched advance the root node
//		until group is exhausted;
//
//---------------------------------------------------------------------------
// Need to delete
// Operator *CBinding::PexprExtract(CGroup *pgroup, Operator *pexprPattern, Operator *pexprLast) {
duckdb::unique_ptr<Operator>
CBinding::PexprExtract(duckdb::unique_ptr<CGroup> pgroup,
					   duckdb::unique_ptr<Operator> pexprPattern,
					   duckdb::unique_ptr<Operator> pexprLast) {
	duckdb::unique_ptr<CGroupExpression> pgexpr = nullptr;
	list<duckdb::unique_ptr<CGroupExpression>>::iterator itr;
	if (nullptr != pexprLast) {
		itr = find(pgroup->m_group_exprs.begin(), pgroup->m_group_exprs.end(), pexprLast->m_group_expression);
		if(pgroup->m_group_exprs.end() != itr) {
			pgexpr = *itr;
		}
	} else {
		// init cursor
		itr = PgexprNext(pgroup, nullptr);
		pgexpr = *itr;
	}
	if (pexprPattern->FPattern() && (unique_ptr_cast<Operator, CPattern>(pexprPattern))->FLeaf()) {
		// for leaf patterns, we do not iterate on group expressions
		if (nullptr != pexprLast) {
			// if a leaf was extracted before, then group is exhausted
			return nullptr;
		}
		return PexprExtract(pgexpr, pexprPattern, pexprLast);
	}
	// start position for next binding
	auto pexprStart = pexprLast;
	do {
		if (pexprPattern->FMatchPattern(pgexpr)) {
			auto pexprResult = PexprExtract(pgexpr, pexprPattern, pexprStart);
			if (nullptr != pexprResult) {
				return pexprResult;
			}
		}
		// move cursor and reset start position
		itr = PgexprNext(pgroup, pgexpr);
		if(pgroup->m_group_exprs.end() != itr) {
			pgexpr = *itr;
			pexprStart = nullptr;
		}
	} while (pgroup->m_group_exprs.end() != itr);
	// group exhausted
	return nullptr;
}
} // namespace gpopt