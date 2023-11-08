//---------------------------------------------------------------------------
//	@filename:
//		CSearchStage.cpp
//
//	@doc:
//		Implementation of optimizer search stage
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CSearchStage.h"

#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::CSearchStage
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSearchStage::CSearchStage(duckdb::unique_ptr<CXform_set> xform_set, ULONG time_threshold, double cost_threshold)
    : m_xforms(xform_set), m_time_threshold(time_threshold), m_cost_threshold(cost_threshold), m_best_cost(-0.5) {
	// include all implementation rules in any search strategy
	*m_xforms |= *(CXformFactory::XformFactory()->XformImplementation());
	m_best_expr = nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::~CSearchStage
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSearchStage::~CSearchStage() {
	// delete m_xforms;
	// CRefCount::SafeRelease(m_best_expr);
}

//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::SetBestExpr
//
//	@doc:
//		Set best plan found at the end of search stage
//
//---------------------------------------------------------------------------
// Need to delete
// void CSearchStage::SetBestExpr(Operator *pexpr) {
void CSearchStage::SetBestExpr(duckdb::unique_ptr<Operator> pexpr) {
	if(pexpr == nullptr) {
		InternalException("In CSearchStage::SetBestExpr: The pointer should not be nullptr!");
	}
	// Need to delete
	// m_best_expr = pexpr->Copy();
	m_best_expr = pexpr;
	if (NULL != m_best_expr) {
		m_best_cost = m_best_expr->m_cost;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::DefaultStrategy
//
//	@doc:
//		Generate default search strategy;
//		one stage with all xforms and no time/cost thresholds
//
//---------------------------------------------------------------------------
duckdb::vector<duckdb::unique_ptr<CSearchStage>> CSearchStage::DefaultStrategy() {
	auto xform_set = make_uniq<CXform_set>();
	*xform_set |= *(CXformFactory::XformFactory()->XformExploration());
	duckdb::vector<duckdb::unique_ptr<CSearchStage>> search_stage_array;
	search_stage_array.push_back(make_uniq<CSearchStage>(xform_set));
	return search_stage_array;
}