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
void CDerivedLogicalProp::Derive(CExpressionHandle &exprhdl, CDerivedPropertyContext *pdpctxt) {
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
bool CDerivedLogicalProp::FSatisfies(const CRequiredPhysicalProp *prop_plan) const {
	auto v1 = GetOutputColumns();
	duckdb::vector<ColumnBinding> v(v1.size() + prop_plan->m_cols.size());
	auto itr = set_difference(v1.begin(), v1.end(), prop_plan->m_cols.begin(), prop_plan->m_cols.end(), v.begin());
	v.resize(itr - v.begin());
	return (v1.size() == prop_plan->m_cols.size() + v.size());
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::GetRelationalProperties
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDerivedLogicalProp *CDerivedLogicalProp::GetRelationalProperties(CDerivedProperty *pdp) {
	return (CDerivedLogicalProp *)pdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDerivedLogicalProp::PdrgpfdChild
//
//	@doc:
//		Helper for getting applicable FDs from child
//
//---------------------------------------------------------------------------
duckdb::vector<CFunctionalDependency *>
CDerivedLogicalProp::DeriveChildFunctionalDependencies(ULONG child_index, CExpressionHandle &exprhdl) {
	// get FD's of the child
	duckdb::vector<CFunctionalDependency *> pdrgpfdChild = exprhdl.Pdrgpfd(child_index);
	// get output columns of the parent
	duckdb::vector<ColumnBinding> pcrsOutput = exprhdl.DeriveOutputColumns();
	// collect child FD's that are applicable to the parent
	duckdb::vector<CFunctionalDependency *> pdrgpfd;
	const ULONG size = pdrgpfdChild.size();
	for (ULONG ul = 0; ul < size; ul++) {
		CFunctionalDependency *pfd = pdrgpfdChild[ul];
		// check applicability of FD's LHS
		if (CUtils::ContainsAll(pcrsOutput, pfd->PcrsKey())) {
			// decompose FD's RHS to extract the applicable part
			duckdb::vector<ColumnBinding> pcrsDetermined;
			duckdb::vector<ColumnBinding> v = pfd->PcrsDetermined();
			pcrsDetermined.insert(pcrsDetermined.end(), v.begin(), v.end());
			duckdb::vector<ColumnBinding> target;
			std::set_intersection(pcrsDetermined.begin(), pcrsDetermined.end(), pcrsOutput.begin(), pcrsOutput.end(),
			                      target.begin());
			if (0 < target.size()) {
				// create a new FD and add it to the output array
				CFunctionalDependency *pfdNew = new CFunctionalDependency(pfd->PcrsKey(), pcrsDetermined);
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
duckdb::vector<CFunctionalDependency *>
CDerivedLogicalProp::DeriveLocalFunctionalDependencies(CExpressionHandle &exprhdl) {
	duckdb::vector<CFunctionalDependency *> pdrgpfd;
	// get local key
	CKeyCollection *pkc = exprhdl.DeriveKeyCollection();
	if (NULL == pkc) {
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
			CFunctionalDependency *pfdLocal = new CFunctionalDependency(pcrsKey, pcrsDetermined);
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
CKeyCollection *CDerivedLogicalProp::GetKeyCollection() const {
	return m_collection;
}

CKeyCollection *CDerivedLogicalProp::DeriveKeyCollection(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPkc]) {
		m_collection = (static_cast<LogicalOperator *>(exprhdl.Pop()))->DeriveKeyCollection(exprhdl);
	}
	m_is_prop_derived.set(EdptPkc, true);
	return m_collection;
}

// functional dependencies
duckdb::vector<CFunctionalDependency *> CDerivedLogicalProp::GetFunctionalDependencies() const {
	return m_fun_deps;
}

duckdb::vector<CFunctionalDependency *> CDerivedLogicalProp::DeriveFunctionalDependencies(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPdrgpfd]) {
		duckdb::vector<CFunctionalDependency *> pdrgpfd;
		const ULONG arity = exprhdl.Arity();
		// collect applicable FD's from logical children
		for (ULONG ul = 0; ul < arity; ul++) {
			duckdb::vector<CFunctionalDependency *> pdrgpfdChild = DeriveChildFunctionalDependencies(ul, exprhdl);
			pdrgpfd.insert(pdrgpfdChild.begin(), pdrgpfdChild.end(), pdrgpfd.end());
		}
		// add local FD's
		duckdb::vector<CFunctionalDependency *> pdrgpfdLocal = DeriveLocalFunctionalDependencies(exprhdl);
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
		m_join_depth = ((LogicalOperator *)exprhdl.Pop())->DeriveJoinDepth(exprhdl);
	}
	m_is_prop_derived.set(EdptJoinDepth, true);
	return m_join_depth;
}

// constraint property
CPropConstraint *CDerivedLogicalProp::GetPropertyConstraint() const {
	return m_prop_constraint;
}

CPropConstraint *CDerivedLogicalProp::DerivePropertyConstraint(CExpressionHandle &exprhdl) {
	if (!m_is_prop_derived[EdptPpc]) {
		m_prop_constraint = ((LogicalOperator *)exprhdl.Pop())->DerivePropertyConstraint(exprhdl);
	}
	m_is_prop_derived.set(EdptPpc, true);
	return m_prop_constraint;
}