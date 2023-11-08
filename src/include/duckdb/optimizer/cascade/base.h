//---------------------------------------------------------------------------
//	@filename:
//		base.h
//
//	@doc:
//		Collection of commonly used OS abstraction primitives;
//		Most files should be fine including only this one from the GPOS folder;
//---------------------------------------------------------------------------
#ifndef GPOS_base_H
#define GPOS_base_H

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/types.h"
#include <memory>
#include <unordered_map>

// invalid cost value
#define GPOPT_INVALID_COST -0.5

// infinite plan cost
#define GPOPT_INFINITE_COST 1e+100

using namespace gpos;

extern double exploration_time;

extern double implementation_time;

extern double optimization_time;

namespace gpos
{

extern unsigned int enumeration_pairs;

extern std::unordered_map<int, double> true_set;

enum Etlsidx { EtlsidxTest, EtlsidxOptCtxt, EtlsidxInvalid, EtlsidxSentinel };

struct EtlsidxHash
{
	size_t operator()(const Etlsidx &idx) const
	{	
		// keys are unique
		return static_cast<ULONG>(idx);
	}
};
}

#endif