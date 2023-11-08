//---------------------------------------------------------------------------
//	@filename:
//		CDerivedLogicalProp.h
//
//	@doc:
//		Derived logical properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropRelational_H
#define GPOPT_CDrvdPropRelational_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDerivedProperty.h"
#include "duckdb/optimizer/cascade/base/CFunctionalDependency.h"
#include "duckdb/optimizer/cascade/base/CPropConstraint.h"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"

#include <bitset>

using namespace gpos;

namespace gpopt {
// fwd declaration
class CExpressionHandle;
class CRequiredPhysicalProp;
class CKeyCollection;
class CPropConstraint;
class CPartInfo;

//---------------------------------------------------------------------------
//	@class:
//		CDerivedLogicalProp
//
//	@doc:
//		Derived logical properties container.
//
//		These are properties than can be inferred from logical expressions or
//		Memo groups. This includes output columns, outer references, primary
//		keys. These properties hold regardless of the physical implementation
//		of an expression.
//
//---------------------------------------------------------------------------
class CDerivedLogicalProp : public CDerivedProperty {
public:
	// See member variables (below) with the same name for description on what
	// the property types respresent
	enum EDrvdPropType {
		EdptPcrsOutput = 0,
		EdptPcrsOuter,
		EdptPcrsNotNull,
		EdptPcrsCorrelatedApply,
		EdptPkc,
		EdptPdrgpfd,
		EdptMaxCard,
		EdptPpc,
		EdptPfp,
		EdptJoinDepth,
		EdptTableDescriptor,
		EdptSentinel
	};

public:
	CDerivedLogicalProp();

	CDerivedLogicalProp(const CDerivedLogicalProp &) = delete;
	
	virtual ~CDerivedLogicalProp();

	// bitset representing whether property has been derived
	bitset<EdptSentinel> m_is_prop_derived;

	// output columns
	duckdb::vector<ColumnBinding> m_output_cols;

	// columns not defined in the underlying operator tree
	duckdb::vector<ColumnBinding> m_outer_cols;

	// output columns that do not allow null values
	duckdb::vector<ColumnBinding> m_not_null_cols;

	// columns from the inner child of a correlated-apply expression that can be used above the apply expression
	duckdb::vector<ColumnBinding> m_correlated_apply_cols;

	// key collection
	duckdb::unique_ptr<CKeyCollection> m_collection;

	// functional dependencies
	duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> m_fun_deps;

	// join depth (number of relations in underlying tree)
	ULONG m_join_depth;

	// constraint property
	duckdb::unique_ptr<CPropConstraint> m_prop_constraint;

	// true if all logical operators in the group are of type CLogicalDynamicGet,
	// and the dynamic get has partial indexes
	bool m_has_partial_indexes;

	// Have all the properties been derived?
	// NOTE1: This is set ONLY when Derive() is called. If all the properties
	// are independently derived, m_is_complete will remain false. In that
	// case, even though Derive() would attempt to derive all the properties
	// once again, it should be quick, since each individual member has been
	// cached.
	// NOTE2: Once these properties are detached from the
	// corresponding expression used to derive it, this MUST be set to true,
	// since after the detachment, there will be no way to derive the
	// properties once again.
	bool m_is_complete;

public:
	// helper for getting applicable FDs from child
	static duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>>
	DeriveChildFunctionalDependencies(ULONG child_index,
	                                  CExpressionHandle &exprhdl);
	// helper for creating local FDs
	static duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>>
	DeriveLocalFunctionalDependencies(CExpressionHandle &exprhdl);

public:
	// output columns
	duckdb::vector<ColumnBinding> DeriveOutputColumns(CExpressionHandle &);

	// outer references
	duckdb::vector<ColumnBinding> DeriveOuterReferences(CExpressionHandle &);

	// nullable columns
	duckdb::vector<ColumnBinding> DeriveNotNullColumns(CExpressionHandle &);

	// columns from the inner child of a correlated-apply expression that can be used above the apply expression
	duckdb::vector<ColumnBinding> DeriveCorrelatedApplyColumns(CExpressionHandle &);

	// key collection
	duckdb::unique_ptr<CKeyCollection> DeriveKeyCollection(CExpressionHandle &);

	// functional dependencies
	duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> DeriveFunctionalDependencies(CExpressionHandle &);

	// join depth
	ULONG DeriveJoinDepth(CExpressionHandle &);

	// constraint property
	duckdb::unique_ptr<CPropConstraint> DerivePropertyConstraint(CExpressionHandle &);

	// has partial indexes
	bool DeriveHasPartialIndexes(CExpressionHandle &);

public:
	// type of properties
	EPropType PropertyType() override {
		return EptRelational;
	}

	bool IsComplete() const override {
		return m_is_complete;
	}

	// derivation function
	void Derive(CExpressionHandle &exprhdl, duckdb::unique_ptr<CDerivedPropertyContext> pdpctxt) override;

	// output columns
	duckdb::vector<ColumnBinding> GetOutputColumns() const;

	// outer references
	duckdb::vector<ColumnBinding> GetOuterReferences() const;

	// nullable columns
	duckdb::vector<ColumnBinding> GetNotNullColumns() const;

	// columns from the inner child of a correlated-apply expression that can be used above the apply expression
	duckdb::vector<ColumnBinding> GetCorrelatedApplyColumns() const;

	// key collection
	duckdb::unique_ptr<CKeyCollection> GetKeyCollection() const;

	// functional dependencies
	duckdb::vector<duckdb::unique_ptr<CFunctionalDependency>> GetFunctionalDependencies() const;

	// join depth
	ULONG GetJoinDepth() const;

	// constraint property
	duckdb::unique_ptr<CPropConstraint> GetPropertyConstraint() const;

	// shorthand for conversion
	static duckdb::unique_ptr<CDerivedLogicalProp> GetRelationalProperties(duckdb::unique_ptr<CDerivedProperty> pdp);

	// check for satisfying required plan properties
	bool FSatisfies(const duckdb::unique_ptr<CRequiredPhysicalProp> prop_plan) const override;
}; // class CDerivedLogicalProp
} // namespace gpopt
#endif