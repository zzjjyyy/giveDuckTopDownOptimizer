//---------------------------------------------------------------------------
//	@filename:
//		CXformFactory.cpp
//
//	@doc:
//		Management of the global xform set
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"
#include "duckdb/optimizer/cascade/xforms/CXformFilterImplementation.h"
#include "duckdb/optimizer/cascade/xforms/CXformGet2TableScan.h"
#include "duckdb/optimizer/cascade/xforms/CXformLogicalProj2PhysicalProj.h"
#include "duckdb/optimizer/cascade/xforms/CXformOrderImplementation.h"
#include "duckdb/optimizer/cascade/xforms/CXformDummyScanImplementation.h"
#include "duckdb/optimizer/cascade/xforms/CXformInnerJoin2HashJoin.h"
#include "duckdb/optimizer/cascade/xforms/CXformJoinCommutativity.h"
#include "duckdb/optimizer/cascade/xforms/CXformJoinAssociativity.h"
#include "duckdb/optimizer/cascade/xforms/CXformLogicalAggregateImplementation.h"
#include "duckdb/optimizer/cascade/xforms/CXformPushGbBelowJoin.h"

namespace gpopt {

// global instance of xform factory
CXformFactory *CXformFactory::m_xform_factory = nullptr;

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::CXformFactory
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformFactory::CXformFactory() : m_xform_dict(0), m_exploration_xforms(nullptr), m_implementation_xforms(nullptr) {
	// null out array so dtor can be called prematurely
	for (ULONG i = 0; i < CXform::ExfSentinel; i++) {
		m_xform_range[i] = nullptr;
	}

	m_exploration_xforms = new CXform_set();
	m_implementation_xforms = new CXform_set();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::~CXformFactory
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CXformFactory::~CXformFactory() {
	// delete all xforms in the array
	for (ULONG i = 0; i < CXform::ExfSentinel; i++) {
		if (nullptr == m_xform_range[i]) {
			// dtor called after failing to populate array
			break;
		}
		delete m_xform_range[i];
		m_xform_range[i] = nullptr;
	}
	m_xform_dict.clear();
	delete m_exploration_xforms;
	delete m_implementation_xforms;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Add
//
//	@doc:
//		Add a given xform to the array; enforce the order in which they
//		are added for readability/debugging
//
//---------------------------------------------------------------------------
void CXformFactory::Add(CXform *xform) {
	CXform::EXformId xform_id = xform->ID();
	m_xform_range[xform_id] = xform;
	// create name -> xform mapping
	ULONG length = clib::Strlen(xform->Name());
	CHAR *sz_xform_name = new CHAR[length + 1];
	clib::Strncpy(sz_xform_name, xform->Name(), length + 1);
	m_xform_dict.insert(make_pair(sz_xform_name, xform));
	CXform_set *xform_set = m_exploration_xforms;
	if (xform->FImplementation()) {
		xform_set = m_implementation_xforms;
	}
	xform_set->set(xform_id);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Instantiate
//
//	@doc:
//		Construct all xforms
//
//---------------------------------------------------------------------------
void CXformFactory::Instantiate() {
	/* I comment here */
	/*
	Add(make_shared<CXformProject2ComputeScalar>();
	Add(make_shared<CXformExpandNAryJoin>();
	Add(make_shared<CXformExpandNAryJoinMinCard>();
	Add(make_shared<CXformExpandNAryJoinDP()>;
	Add(make_shared<CXformExpandNAryJoin>();
	*/
	Add(new CXformGet2TableScan());
	Add(new CXformLogicalProj2PhysicalProj());
	Add(new CXformOrderImplementation());
	Add(new CXformFilterImplementation());
	Add(new CXformDummyScanImplementation());
	Add(new CXformInnerJoin2HashJoin());
	Add(new CXformJoinCommutativity());
	Add(new CXformJoinAssociativity());
	Add(new CXformLogicalAggregateImplementation());
	Add(new CXformPushGbBelowJoin());
	/*
	Add(make_shared<CXformInnerJoin2HashJoin();
	Add(make_shared<CXformIndexGet2IndexScan();
	Add(make_shared<CXformDynamicGet2DynamicTableScan();
	Add(make_shared<CXformDynamicIndexGet2DynamicIndexScan();
	Add(make_shared<CXformImplementSequence();
	Add(make_shared<CXformImplementConstTableGet();
	Add(make_shared<CXformUnnestTVF();
	Add(make_shared<CXformImplementTVF();
	Add(make_shared<CXformImplementTVFNoArgs();
	Add(make_shared<CXformSelect2Filter();
	Add(make_shared<CXformSelect2IndexGet();
	Add(make_shared<CXformSelect2DynamicIndexGet();
	Add(make_shared<CXformSelect2PartialDynamicIndexGet();
	Add(make_shared<CXformSimplifySelectWithSubquery();
	Add(make_shared<CXformSimplifyProjectWithSubquery();
	Add(make_shared<CXformSelect2Apply();
	Add(make_shared<CXformProject2Apply();
	Add(make_shared<CXformGbAgg2Apply();
	Add(make_shared<CXformSubqJoin2Apply();
	Add(make_shared<CXformSubqNAryJoin2Apply();
	Add(make_shared<CXformInnerJoin2IndexGetApply();
	Add(make_shared<CXformInnerJoin2DynamicIndexGetApply();
	Add(make_shared<CXformInnerApplyWithOuterKey2InnerJoin();
	Add(make_shared<CXformInnerJoin2NLJoin();
	Add(make_shared<CXformImplementIndexApply();
	Add(make_shared<CXformInnerJoin2HashJoin();
	Add(make_shared<CXformInnerApply2InnerJoin();
	Add(make_shared<CXformInnerApply2InnerJoinNoCorrelations();
	Add(make_shared<CXformImplementInnerCorrelatedApply();
	Add(make_shared<CXformLeftOuterApply2LeftOuterJoin();
	Add(make_shared<CXformLeftOuterApply2LeftOuterJoinNoCorrelations();
	Add(make_shared<CXformImplementLeftOuterCorrelatedApply();
	Add(make_shared<CXformLeftSemiApply2LeftSemiJoin();
	Add(make_shared<CXformLeftSemiApplyWithExternalCorrs2InnerJoin();
	Add(make_shared<CXformLeftSemiApply2LeftSemiJoinNoCorrelations();
	Add(make_shared<CXformLeftAntiSemiApply2LeftAntiSemiJoin();
	Add(make_shared<CXformLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations();
	Add(make_shared<CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn();
	Add(make_shared<CXformLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotInNoCorrelations();
	Add(make_shared<CXformPushDownLeftOuterJoin();
	Add(make_shared<CXformSimplifyLeftOuterJoin();
	Add(make_shared<CXformLeftOuterJoin2NLJoin();
	Add(make_shared<CXformLeftOuterJoin2HashJoin();
	Add(make_shared<CXformLeftSemiJoin2NLJoin();
	Add(make_shared<CXformLeftSemiJoin2HashJoin();
	Add(make_shared<CXformLeftAntiSemiJoin2CrossProduct();
	Add(make_shared<CXformLeftAntiSemiJoinNotIn2CrossProduct();
	Add(make_shared<CXformLeftAntiSemiJoin2NLJoin();
	Add(make_shared<CXformLeftAntiSemiJoinNotIn2NLJoinNotIn();
	Add(make_shared<CXformLeftAntiSemiJoin2HashJoin();
	Add(make_shared<CXformLeftAntiSemiJoinNotIn2HashJoinNotIn();
	Add(make_shared<CXformGbAgg2HashAgg();
	Add(make_shared<CXformGbAgg2StreamAgg();
	Add(make_shared<CXformGbAgg2ScalarAgg();
	Add(make_shared<CXformGbAggDedup2HashAggDedup();
	Add(make_shared<CXformGbAggDedup2StreamAggDedup();
	Add(make_shared<CXformImplementLimit();
	Add(make_shared<CXformIntersectAll2LeftSemiJoin();
	Add(make_shared<CXformIntersect2Join();
	Add(make_shared<CXformDifference2LeftAntiSemiJoin();
	Add(make_shared<CXformDifferenceAll2LeftAntiSemiJoin();
	Add(make_shared<CXformUnion2UnionAll();
	Add(make_shared<CXformImplementUnionAll();
	Add(make_shared<CXformInsert2DML();
	Add(make_shared<CXformDelete2DML();
	Add(make_shared<CXformUpdate2DML();
	Add(make_shared<CXformImplementDML();
	Add(make_shared<CXformImplementRowTrigger();
	Add(make_shared<CXformImplementSplit();
	Add(make_shared<CXformJoinCommutativity();
	Add(make_shared<CXformJoinAssociativity();
	Add(make_shared<CXformSemiJoinSemiJoinSwap();
	Add(make_shared<CXformSemiJoinAntiSemiJoinSwap();
	Add(make_shared<CXformSemiJoinAntiSemiJoinNotInSwap();
	Add(make_shared<CXformSemiJoinInnerJoinSwap();
	Add(make_shared<CXformAntiSemiJoinAntiSemiJoinSwap();
	Add(make_shared<CXformAntiSemiJoinAntiSemiJoinNotInSwap();
	Add(make_shared<CXformAntiSemiJoinSemiJoinSwap();
	Add(make_shared<CXformAntiSemiJoinInnerJoinSwap();
	Add(make_shared<CXformAntiSemiJoinNotInAntiSemiJoinSwap();
	Add(make_shared<CXformAntiSemiJoinNotInAntiSemiJoinNotInSwap();
	Add(make_shared<CXformAntiSemiJoinNotInSemiJoinSwap();
	Add(make_shared<CXformAntiSemiJoinNotInInnerJoinSwap();
	Add(make_shared<CXformInnerJoinSemiJoinSwap();
	Add(make_shared<CXformInnerJoinAntiSemiJoinSwap();
	Add(make_shared<CXformInnerJoinAntiSemiJoinNotInSwap();
	Add(make_shared<CXformLeftSemiJoin2InnerJoin();
	Add(make_shared<CXformLeftSemiJoin2InnerJoinUnderGb();
	Add(make_shared<CXformLeftSemiJoin2CrossProduct();
	Add(make_shared<CXformSplitLimit();
	Add(make_shared<CXformSimplifyGbAgg();
	Add(make_shared<CXformCollapseGbAgg();
	Add(make_shared<CXformPushGbBelowJoin();
	Add(make_shared<CXformPushGbDedupBelowJoin();
	Add(make_shared<CXformPushGbWithHavingBelowJoin();
	Add(make_shared<CXformPushGbBelowUnion();
	Add(make_shared<CXformPushGbBelowUnionAll();
	Add(make_shared<CXformSplitGbAgg();
	Add(make_shared<CXformSplitGbAggDedup();
	Add(make_shared<CXformSplitDQA();
	Add(make_shared<CXformSequenceProject2Apply();
	Add(make_shared<CXformImplementSequenceProject();
	Add(make_shared<CXformImplementAssert();
	Add(make_shared<CXformCTEAnchor2Sequence();
	Add(make_shared<CXformCTEAnchor2TrivialSelect();
	Add(make_shared<CXformInlineCTEConsumer();
	Add(make_shared<CXformInlineCTEConsumerUnderSelect();
	Add(make_shared<CXformImplementCTEProducer();
	Add(make_shared<CXformImplementCTEConsumer();
	Add(make_shared<CXformExpandFullOuterJoin();
	Add(make_shared<CXformExternalGet2ExternalScan();
	Add(make_shared<CXformSelect2BitmapBoolOp();
	Add(make_shared<CXformSelect2DynamicBitmapBoolOp();
	Add(make_shared<CXformImplementBitmapTableGet();
	Add(make_shared<CXformImplementDynamicBitmapTableGet();
	Add(make_shared<CXformInnerJoin2PartialDynamicIndexGetApply();
	Add(make_shared<CXformLeftOuter2InnerUnionAllLeftAntiSemiJoin();
	Add(make_shared<CXformImplementLeftSemiCorrelatedApply();
	Add(make_shared<CXformImplementLeftSemiCorrelatedApplyIn();
	Add(make_shared<CXformImplementLeftAntiSemiCorrelatedApply();
	Add(make_shared<CXformImplementLeftAntiSemiCorrelatedApplyNotIn();
	Add(make_shared<CXformLeftSemiApplyIn2LeftSemiJoin();
	Add(make_shared<CXformLeftSemiApplyInWithExternalCorrs2InnerJoin();
	Add(make_shared<CXformLeftSemiApplyIn2LeftSemiJoinNoCorrelations();
	Add(make_shared<CXformInnerJoin2BitmapIndexGetApply();
	Add(make_shared<CXformImplementPartitionSelector();
	Add(make_shared<CXformMaxOneRow2Assert();
	Add(make_shared<CXformInnerJoinWithInnerSelect2IndexGetApply();
	Add(make_shared<CXformInnerJoinWithInnerSelect2DynamicIndexGetApply();
	Add(make_shared<CXformInnerJoinWithInnerSelect2PartialDynamicIndexGetApply();
	Add(make_shared<CXformInnerJoin2DynamicBitmapIndexGetApply();
	Add(make_shared<CXformInnerJoinWithInnerSelect2BitmapIndexGetApply();
	Add(make_shared<CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply();
	Add(make_shared<CXformGbAggWithMDQA2Join();
	Add(make_shared<CXformCollapseProject();
	Add(make_shared<CXformRemoveSubqDistinct();
	Add(make_shared<CXformLeftOuterJoin2BitmapIndexGetApply();
	Add(make_shared<CXformLeftOuterJoin2IndexGetApply();
	Add(make_shared<CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply();
	Add(make_shared<CXformLeftOuterJoinWithInnerSelect2IndexGetApply();
	Add(make_shared<CXformExpandNAryJoinGreedy();
	Add(make_shared<CXformEagerAgg();
	Add(make_shared<CXformExpandNAryJoinDPv2();
	Add(make_shared<CXformImplementFullOuterMergeJoin();
	Add(make_shared<CXformLeftOuterJoin2DynamicBitmapIndexGetApply();
	Add(make_shared<CXformLeftOuterJoin2DynamicIndexGetApply();
	Add(make_shared<CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply();
	Add(make_shared<CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply();
	*/
}

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Xform
//
//	@doc:
//		Accessor of xform array
//
//---------------------------------------------------------------------------
CXform *CXformFactory::Xform(CXform::EXformId xform_id) const {
	CXform *pxf = m_xform_range[xform_id];
	return pxf;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Xform
//
//	@doc:
//		Accessor by xform name
//
//---------------------------------------------------------------------------
CXform *CXformFactory::Xform(const CHAR *xform_name) const {
	auto itr = m_xform_dict.find(const_cast<CHAR *>(xform_name));
	return itr->second;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Init
//
//	@doc:
//		Initializes global instance
//
//---------------------------------------------------------------------------
GPOS_RESULT CXformFactory::Init() {
	GPOS_RESULT result = GPOS_OK;
	// create xform factory instance
	m_xform_factory = new CXformFactory();
	// instantiating the factory
	m_xform_factory->Instantiate();
	return result;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformFactory::Shutdown
//
//	@doc:
//		Cleans up allocated memory pool
//
//---------------------------------------------------------------------------
void CXformFactory::Shutdown() {
	CXformFactory *xform_factory = CXformFactory::XformFactory();
	// destroy xform factory
	CXformFactory::m_xform_factory = nullptr;
	delete xform_factory;
}
} // namespace gpopt