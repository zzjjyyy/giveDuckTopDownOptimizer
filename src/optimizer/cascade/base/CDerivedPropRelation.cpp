//---------------------------------------------------------------------------
//	@filename:
//		CDerivedLogicalProp.cpp
//
//	@doc:
//		Relational derived properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDerivedPropRelation.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CRequiredPhysicalProp.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::CDerivedLogicalProp
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDerivedLogicalProp::CDerivedLogicalProp()
    : m_collection(NULL), m_join_depth(0), m_prop_constraint(NULL), m_is_complete(false) {
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::CDerivedPropRelationn
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDerivedLogicalProp::~CDerivedLogicalProp() {
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::Derive
//
//	@doc:
//		Derive relational props. This derives ALL properties
//
//---------------------------------------------------------------------------
void CDerivedLogicalProp::Derive(CExpressionHandle &exprhdl, duckdb::unique_ptr<CDerivedPropertyContext> pdpctxt) {
	// call output derivation function on the operator
	DeriveOutputColumns(exprhdl);
	// derive outer-references
	// DeriveOuterReferences(exprhdl);
	// derive not null columns
	DeriveNotNullColumns(exprhdl);
	// derive correlated apply columns
	// DeriveCorrelatedApplyColumns(exprhdl);
	// derive constraint
	// DerivePropertyConstraint(exprhdl);
	// derive keys
	// DeriveKeyCollection(exprhdl);
	// derive join depth
	DeriveJoinDepth(exprhdl);
	// derive functional dependencies
	// DeriveFunctionalDependencies(exprhdl);
	m_is_complete = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
bool CDerivedLogicalProp::FSatisfies(const duckdb::unique_ptr<CRequiredPhysicalProp> prop_plan) const {
	auto v1 = GetOutputColumns();
	return CUtils::ContainsAll(v1, prop_plan->m_cols);
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::GetRelationalProperties
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<CDerivedLogicalProp> CDerivedLogicalProp::GetRelationalProperties(duckdb::unique_ptr<CDerivedProperty> pdp) {
	return unique_ptr_cast<CDerivedProperty, CDerivedLogicalProp>(pdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::PdrgpfdChild
//
//	@doc:
//		Helper for getting applicable FDs from child
//
//---------------------------------------------------------------------------
duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>>
CDerivedLogicalProp::DeriveChildFunctionalDependencies(ULONG child_index, CExpressionHandle &exprhdl) {
	// get FD's of the child
	auto pdrgpfdChild = exprhdl.Pdrgpfd(child_index);
	// get output columns of the parent
	auto pcrsOutput = exprhdl.DeriveOutputColumns();
	// collect child FD's that are applicable to the parent
	duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> pdrgpfd;
	const ULONG size = pdrgpfdChild.size();
	for (ULONG ul = 0; ul < size; ul++) {
		auto pfd = pdrgpfdChild[ul];
		// check applicability of FD's LHS
		if (CUtils::ContainsAll(pcrsOutput, pfd->PcrsKey())) {
			// decompose FD's RHS to extract the applicable part
			duckdb::vector<ColumnBinding> pcrsDetermined;
			duckdb::vector<ColumnBinding> v = pfd->PcrsDetermined();
			pcrsDetermined.insert(pcrsDetermined.end(), v.begin(), v.end());
			duckdb::vector<ColumnBinding> target;
			std::set_intersection(pcrsDetermined.begin(),
								  pcrsDetermined.end(),
								  pcrsOutput.begin(),
								  pcrsOutput.end(),
			                      target.begin());
			if (0 < target.size()) {
				// create a new FD and add it to the output array
				auto pfdNew = make_uniq<CFunctionalDependency>(pfd->PcrsKey(), pcrsDetermined);
				pdrgpfd.push_back(pfdNew);
			}
		}
	}
	return pdrgpfd;
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::PdrgpfdLocal
//
//	@doc:
//		Helper for deriving local FDs
//
//---------------------------------------------------------------------------
duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>>
CDerivedLogicalProp::DeriveLocalFunctionalDependencies(CExpressionHandle &exprhdl) {
	duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> pdrgpfd;
	// get local key
	auto pkc = exprhdl.DeriveKeyCollection();
	if (nullptr == pkc) {
		return pdrgpfd;
	}
	ULONG ulKeys = pkc->Keys();
	for (ULONG ul = 0; ul < ulKeys; ul++) {
		duckdb::vector<ColumnBinding> pdrgpcrKey = pkc->PdrgpcrKey(ul);
		duckdb::vector<ColumnBinding> pcrsKey;
		pcrsKey.insert(pcrsKey.begin(), pdrgpcrKey.begin(), pdrgpcrKey.end());
		// get output columns
		duckdb::vector<ColumnBinding> pcrsOutput = exprhdl.DeriveOutputColumns();
		duckdb::vector<ColumnBinding> pcrsDetermined;
		pcrsDetermined.insert(pcrsDetermined.begin(), pcrsOutput.begin(), pcrsOutput.end());
		duckdb::vector<ColumnBinding> target;
		std::set_difference(pcrsDetermined.begin(), pcrsDetermined.end(), pcrsKey.begin(), pcrsKey.end(),
		                    target.begin());
		if (0 < target.size()) {
			// add FD between key and the rest of output columns
			auto pfdLocal = make_uniq<CFunctionalDependency>(pcrsKey, pcrsDetermined);
			pdrgpfd.push_back(pfdLocal);
		}
	}
	return pdrgpfd;
}

// output columns
duckdb::vector<ColumnBinding> CDerivedLogicalProp::GetOutputColumns() const {
	return m_output_cols;
}

// output columns
duckdb::vector<ColumnBinding> CDerivedLogicalProp::DeriveOutputColumns(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPcrsOutput]) {
		m_output_cols = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsOutput, true);
	return m_output_cols;
}

// outer references
duckdb::vector<ColumnBinding> CDerivedLogicalProp::GetOuterReferences() const {
	return m_outer_cols;
}

// outer references
duckdb::vector<ColumnBinding> CDerivedLogicalProp::DeriveOuterReferences(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPcrsOuter]) {
		m_outer_cols = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsOuter, true);
	return m_outer_cols;
}

// nullable columns
duckdb::vector<ColumnBinding> CDerivedLogicalProp::GetNotNullColumns() const {
	return m_not_null_cols;
}

duckdb::vector<ColumnBinding> CDerivedLogicalProp::DeriveNotNullColumns(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPcrsNotNull]) {
		m_not_null_cols = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsNotNull, true);
	return m_not_null_cols;
}

// columns from the inner child of a correlated-apply expression that can be used above the apply expression
duckdb::vector<ColumnBinding> CDerivedLogicalProp::GetCorrelatedApplyColumns() const {
	return m_correlated_apply_cols;
}

duckdb::vector<ColumnBinding> CDerivedLogicalProp::DeriveCorrelatedApplyColumns(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPcrsCorrelatedApply]) {
		m_correlated_apply_cols = exprhdl.Pop()->GetColumnBindings();
	}
	m_is_prop_derived.set(EdptPcrsCorrelatedApply, true);
	return m_correlated_apply_cols;
}

// key collection
duckdb::unique_ptr<CKeyCollection> CDerivedLogicalProp::GetKeyCollection() const {
	return m_collection;
}

duckdb::unique_ptr<CKeyCollection> CDerivedLogicalProp::DeriveKeyCollection(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPkc]) {
		m_collection = (unique_ptr_cast<Operator, LogicalOperator>(exprhdl.Pop()))->DeriveKeyCollection(exprhdl);
	}
	m_is_prop_derived.set(EdptPkc, true);
	return m_collection;
}

// functional dependencies
duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> CDerivedLogicalProp::GetFunctionalDependencies() const {
	return m_fun_deps;
}

duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>>
CDerivedLogicalProp::DeriveFunctionalDependencies(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPdrgpfd]) {
		duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> pdrgpfd;
		const ULONG arity = exprhdl.Arity();
		// collect applicable FD's from logical children
		for (ULONG ul = 0; ul < arity; ul++) {
			auto pdrgpfdChild = DeriveChildFunctionalDependencies(ul, exprhdl);
			pdrgpfd.insert(pdrgpfdChild.begin(), pdrgpfdChild.end(), pdrgpfd.end());
		}
		// add local FD's
		auto pdrgpfdLocal = DeriveLocalFunctionalDependencies(exprhdl);
		pdrgpfd.insert(pdrgpfdLocal.begin(), pdrgpfdLocal.end(), pdrgpfd.end());
		m_fun_deps = pdrgpfd;
	}
	m_is_prop_derived.set(EdptPdrgpfd, true);
	return m_fun_deps;
}

// join depth
ULONG CDerivedLogicalProp::GetJoinDepth() const {
	return m_join_depth;
}

ULONG CDerivedLogicalProp::DeriveJoinDepth(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptJoinDepth]) {
		m_join_depth = unique_ptr_cast<Operator, LogicalOperator>(exprhdl.Pop())->DeriveJoinDepth(exprhdl);
	}
	m_is_prop_derived.set(EdptJoinDepth, true);
	return m_join_depth;
}

// constraint property
duckdb::unique_ptr<CPropConstraint>
CDerivedLogicalProp::GetPropertyConstraint() const {
	return m_prop_constraint;
}

duckdb::unique_ptr<CPropConstraint>
CDerivedLogicalProp::DerivePropertyConstraint(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPpc]) {
		m_prop_constraint = unique_ptr_cast<Operator, LogicalOperator>(exprhdl.Pop())->DerivePropertyConstraint(exprhdl);
	}
	m_is_prop_derived.set(EdptPpc, true);
	return m_prop_constraint;
}