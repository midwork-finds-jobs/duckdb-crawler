#pragma once

// RAII wrappers for yyjson allocations
// Include this header in files that use yyjson to get automatic memory management

#include "yyjson.hpp"

namespace duckdb {

// RAII wrapper for yyjson_doc (immutable document)
class YyjsonDocGuard {
public:
	explicit YyjsonDocGuard(yyjson_doc *doc) : doc_(doc) {
	}
	~YyjsonDocGuard() {
		if (doc_)
			yyjson_doc_free(doc_);
	}

	// Non-copyable
	YyjsonDocGuard(const YyjsonDocGuard &) = delete;
	YyjsonDocGuard &operator=(const YyjsonDocGuard &) = delete;

	// Movable
	YyjsonDocGuard(YyjsonDocGuard &&other) noexcept : doc_(other.doc_) {
		other.doc_ = nullptr;
	}
	YyjsonDocGuard &operator=(YyjsonDocGuard &&other) noexcept {
		if (this != &other) {
			if (doc_)
				yyjson_doc_free(doc_);
			doc_ = other.doc_;
			other.doc_ = nullptr;
		}
		return *this;
	}

	yyjson_doc *get() const {
		return doc_;
	}
	yyjson_doc *release() {
		auto *d = doc_;
		doc_ = nullptr;
		return d;
	}
	explicit operator bool() const {
		return doc_ != nullptr;
	}

private:
	yyjson_doc *doc_;
};

// RAII wrapper for yyjson_mut_doc (mutable document)
class YyjsonMutDocGuard {
public:
	explicit YyjsonMutDocGuard(yyjson_mut_doc *doc) : doc_(doc) {
	}
	~YyjsonMutDocGuard() {
		if (doc_)
			yyjson_mut_doc_free(doc_);
	}

	// Non-copyable
	YyjsonMutDocGuard(const YyjsonMutDocGuard &) = delete;
	YyjsonMutDocGuard &operator=(const YyjsonMutDocGuard &) = delete;

	// Movable
	YyjsonMutDocGuard(YyjsonMutDocGuard &&other) noexcept : doc_(other.doc_) {
		other.doc_ = nullptr;
	}
	YyjsonMutDocGuard &operator=(YyjsonMutDocGuard &&other) noexcept {
		if (this != &other) {
			if (doc_)
				yyjson_mut_doc_free(doc_);
			doc_ = other.doc_;
			other.doc_ = nullptr;
		}
		return *this;
	}

	yyjson_mut_doc *get() const {
		return doc_;
	}
	yyjson_mut_doc *release() {
		auto *d = doc_;
		doc_ = nullptr;
		return d;
	}
	explicit operator bool() const {
		return doc_ != nullptr;
	}

private:
	yyjson_mut_doc *doc_;
};

} // namespace duckdb
