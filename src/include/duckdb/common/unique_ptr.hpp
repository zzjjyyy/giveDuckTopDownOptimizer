#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/likely.hpp"
#include <memory>
#include <iostream>
#include <type_traits>

namespace duckdb {

namespace {
struct __unique_ptr_utils {
	static inline void AssertNotNull(void *ptr) {
#ifdef DEBUG
		if (DUCKDB_UNLIKELY(!ptr)) {
			throw InternalException("Attempted to dereference unique_ptr that is NULL!");
		}
#endif
	}
};
} // namespace
/*
template <class _Tp, class _Dp = std::default_delete<_Tp>>
class unique_ptr : public std::unique_ptr<_Tp, _Dp> {
public:
	using original = std::unique_ptr<_Tp, _Dp>;
	using original::original;

	typename std::add_lvalue_reference<_Tp>::type operator*() const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return *(original::get());
	}

	typename original::pointer operator->() const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return original::get();
	}

#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	inline void
	reset(typename original::pointer ptr = typename original::pointer()) noexcept {
		original::reset(ptr);
	}
};
*/

template <class _Tp, class _Dp = std::default_delete<_Tp>>
class unique_ptr : public std::shared_ptr<_Tp> {
	template<typename... _Args>
	using _Constructible = typename std::enable_if<
	  std::is_constructible<std::__shared_ptr<_Tp>, _Args...>::value
	>::type;

	using pointer = typename std::__uniq_ptr_impl<_Tp, std::default_delete<_Tp>>::pointer;
	
	template<typename _Arg>
	using _Assignable = typename std::enable_if<
	  std::is_assignable<std::__shared_ptr<_Tp>&, _Arg>::value, unique_ptr&
	>::type;

private:
    // helper template for detecting a safe conversion from another unique_ptr
    template<typename _Up> using __safe_conversion_up = 
	std::__and_<
	  std::is_convertible<typename std::unique_ptr<_Up>::pointer, pointer>,
	  std::__not_<std::is_array<_Up>>
    >;

public:
	constexpr unique_ptr() : std::shared_ptr<_Tp>() { }

	constexpr unique_ptr(nullptr_t) : std::shared_ptr<_Tp>() { }

	template<typename _Deleter>
	unique_ptr(nullptr_t __p, _Deleter __d) : std::shared_ptr<_Tp>(__p, std::move(__d)) { }

	template<typename _Yp, typename = _Constructible<_Yp*>>
	explicit unique_ptr(_Yp* __p) : std::shared_ptr<_Tp>(__p) { }
	
	template<typename _Yp, typename = _Constructible<_Yp*>, typename _Deleter>
	explicit unique_ptr(_Yp* __p, _Deleter __d) : std::shared_ptr<_Tp>(__p, std::move(__d)) { }

	unique_ptr(const unique_ptr& __p) : std::shared_ptr<_Tp>(__p) { } // Copy constructor

	template<typename _Up, typename = std::_Require<__safe_conversion_up<_Up>>>
	unique_ptr(const unique_ptr<_Up>& __u) : std::shared_ptr<_Tp>(__u) { }

	template<typename _Yp>
	unique_ptr(const std::shared_ptr<_Yp>& __r, typename std::shared_ptr<_Tp>::element_type* __p) noexcept
	: shared_ptr<_Tp>(__r, __p) { }

    unique_ptr(unique_ptr&& __r) noexcept : std::shared_ptr<_Tp>(std::move(__r)) { }

    template<typename _Yp, typename = _Constructible<unique_ptr<_Yp>>>
	unique_ptr(unique_ptr<_Yp>&& __r) noexcept : std::shared_ptr<_Tp>(std::move(__r)) { }

	~unique_ptr()
	{
	}

	_Tp* release() {
		_Tp* ptr = this->get();
		return ptr;
	}

	unique_ptr& operator=(const unique_ptr&) noexcept = default;
	
    template<typename _Up>
    typename std::enable_if<__safe_conversion_up<_Up>::value, unique_ptr&>::type
	operator=(unique_ptr<_Up>&& __u)
	{
		this->shared_ptr<_Tp>::operator=(__u);
		return *this;
	}
};

/*
template <class _Tp, class _Dp>
class unique_ptr<_Tp[], _Dp> : public std::unique_ptr<_Tp[], _Dp> {
public:
	using original = std::unique_ptr<_Tp[], _Dp>;
	using original::original;

	typename std::add_lvalue_reference<_Tp>::type operator[](size_t __i) const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return (original::get())[__i];
	}
};
*/
} // namespace duckdb