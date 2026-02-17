#pragma once

#include "robots_parser.hpp"
#include "duckdb/common/helper.hpp"
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

//===--------------------------------------------------------------------===//
// URL Queue Entry - scheduling and retry tracking
//===--------------------------------------------------------------------===//

struct UrlQueueEntry {
	std::string url;
	int retry_count;
	bool is_update;
	std::chrono::steady_clock::time_point earliest_fetch;

	UrlQueueEntry() : retry_count(0), is_update(false), earliest_fetch(std::chrono::steady_clock::now()) {
	}

	UrlQueueEntry(const std::string &u, int rc, bool upd)
	    : url(u), retry_count(rc), is_update(upd), earliest_fetch(std::chrono::steady_clock::now()) {
	}

	UrlQueueEntry(const std::string &u, int rc, bool upd, std::chrono::steady_clock::time_point ef)
	    : url(u), retry_count(rc), is_update(upd), earliest_fetch(ef) {
	}

	// For priority queue: earlier times have higher priority
	bool operator>(const UrlQueueEntry &other) const {
		return earliest_fetch > other.earliest_fetch;
	}
};

//===--------------------------------------------------------------------===//
// Domain State - rate limiting and 429 handling per domain
//===--------------------------------------------------------------------===//

struct DomainState {
	mutable std::mutex mutex; // Per-domain lock for thread safety

	std::chrono::steady_clock::time_point last_crawl_time;
	double crawl_delay_seconds = 1.0;
	RobotsRules rules;
	bool robots_fetched = false;
	int urls_crawled = 0;
	int urls_failed = 0;
	int urls_skipped = 0;

	// 429 blocking
	std::chrono::steady_clock::time_point blocked_until;
	int consecutive_429s = 0;
	bool has_crawl_delay = false; // True if robots.txt specified crawl-delay

	// Parallel tracking
	int active_requests = 0;

	// Adaptive rate limiting
	double average_response_ms = 0;     // Exponential moving average
	double min_crawl_delay_seconds = 0; // Floor from robots.txt or default
	int response_count = 0;             // Number of responses for EMA warmup

	// Default constructor
	DomainState() = default;

	// Copy constructor - copies data but gets new mutex
	DomainState(const DomainState &other)
	    : last_crawl_time(other.last_crawl_time), crawl_delay_seconds(other.crawl_delay_seconds), rules(other.rules),
	      robots_fetched(other.robots_fetched), urls_crawled(other.urls_crawled), urls_failed(other.urls_failed),
	      urls_skipped(other.urls_skipped), blocked_until(other.blocked_until),
	      consecutive_429s(other.consecutive_429s), has_crawl_delay(other.has_crawl_delay),
	      active_requests(other.active_requests), average_response_ms(other.average_response_ms),
	      min_crawl_delay_seconds(other.min_crawl_delay_seconds), response_count(other.response_count) {
	}

	// Move constructor - moves data but gets new mutex
	DomainState(DomainState &&other) noexcept
	    : last_crawl_time(std::move(other.last_crawl_time)), crawl_delay_seconds(other.crawl_delay_seconds),
	      rules(std::move(other.rules)), robots_fetched(other.robots_fetched), urls_crawled(other.urls_crawled),
	      urls_failed(other.urls_failed), urls_skipped(other.urls_skipped),
	      blocked_until(std::move(other.blocked_until)), consecutive_429s(other.consecutive_429s),
	      has_crawl_delay(other.has_crawl_delay), active_requests(other.active_requests),
	      average_response_ms(other.average_response_ms), min_crawl_delay_seconds(other.min_crawl_delay_seconds),
	      response_count(other.response_count) {
	}

	// Copy assignment - copies data but mutex stays
	DomainState &operator=(const DomainState &other) {
		if (this != &other) {
			last_crawl_time = other.last_crawl_time;
			crawl_delay_seconds = other.crawl_delay_seconds;
			rules = other.rules;
			robots_fetched = other.robots_fetched;
			urls_crawled = other.urls_crawled;
			urls_failed = other.urls_failed;
			urls_skipped = other.urls_skipped;
			blocked_until = other.blocked_until;
			consecutive_429s = other.consecutive_429s;
			has_crawl_delay = other.has_crawl_delay;
			active_requests = other.active_requests;
			average_response_ms = other.average_response_ms;
			min_crawl_delay_seconds = other.min_crawl_delay_seconds;
			response_count = other.response_count;
		}
		return *this;
	}
};

//===--------------------------------------------------------------------===//
// Thread-Safe URL Priority Queue
//===--------------------------------------------------------------------===//

class ThreadSafeUrlQueue {
public:
	void Push(UrlQueueEntry entry);
	bool TryPop(UrlQueueEntry &entry);
	bool WaitAndPop(UrlQueueEntry &entry, std::chrono::milliseconds timeout);
	void Shutdown();
	bool Empty() const;
	size_t Size() const;

private:
	mutable std::mutex mutex_;
	std::condition_variable cv_;
	std::priority_queue<UrlQueueEntry, std::vector<UrlQueueEntry>, std::greater<UrlQueueEntry>> queue_;
	bool shutdown_ = false;
};

//===--------------------------------------------------------------------===//
// Thread-Safe Domain State Map
//===--------------------------------------------------------------------===//

class ThreadSafeDomainMap {
public:
	DomainState &GetOrCreate(const std::string &domain);
	DomainState *TryGet(const std::string &domain);
	void InitializeFromDiscovery(const std::string &domain, const DomainState &src);

private:
	std::mutex map_mutex_;
	std::unordered_map<std::string, unique_ptr<DomainState>> domain_states_;
};

} // namespace duckdb
