#include "thread_utils.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ThreadSafeUrlQueue Implementation
//===--------------------------------------------------------------------===//

void ThreadSafeUrlQueue::Push(UrlQueueEntry entry) {
	std::lock_guard<std::mutex> lock(mutex_);
	queue_.push(std::move(entry));
	cv_.notify_one();
}

bool ThreadSafeUrlQueue::TryPop(UrlQueueEntry &entry) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (queue_.empty())
		return false;
	entry = std::move(const_cast<UrlQueueEntry &>(queue_.top()));
	queue_.pop();
	return true;
}

bool ThreadSafeUrlQueue::WaitAndPop(UrlQueueEntry &entry, std::chrono::milliseconds timeout) {
	std::unique_lock<std::mutex> lock(mutex_);
	if (!cv_.wait_for(lock, timeout, [this] { return !queue_.empty() || shutdown_; })) {
		return false;
	}
	if (shutdown_ && queue_.empty())
		return false;
	entry = std::move(const_cast<UrlQueueEntry &>(queue_.top()));
	queue_.pop();
	return true;
}

void ThreadSafeUrlQueue::Shutdown() {
	std::lock_guard<std::mutex> lock(mutex_);
	shutdown_ = true;
	cv_.notify_all();
}

bool ThreadSafeUrlQueue::Empty() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return queue_.empty();
}

size_t ThreadSafeUrlQueue::Size() const {
	std::lock_guard<std::mutex> lock(mutex_);
	return queue_.size();
}

//===--------------------------------------------------------------------===//
// ThreadSafeDomainMap Implementation
//===--------------------------------------------------------------------===//

DomainState &ThreadSafeDomainMap::GetOrCreate(const std::string &domain) {
	std::lock_guard<std::mutex> lock(map_mutex_);
	auto it = domain_states_.find(domain);
	if (it == domain_states_.end()) {
		auto result = domain_states_.emplace(domain, make_uniq<DomainState>());
		return *result.first->second;
	}
	return *it->second;
}

DomainState *ThreadSafeDomainMap::TryGet(const std::string &domain) {
	std::lock_guard<std::mutex> lock(map_mutex_);
	auto it = domain_states_.find(domain);
	return it != domain_states_.end() ? it->second.get() : nullptr;
}

void ThreadSafeDomainMap::InitializeFromDiscovery(const std::string &domain, const DomainState &src) {
	std::lock_guard<std::mutex> lock(map_mutex_);
	auto &state = domain_states_[domain];
	if (!state) {
		state = make_uniq<DomainState>();
	}
	// Copy fields (but not mutex)
	state->last_crawl_time = src.last_crawl_time;
	state->crawl_delay_seconds = src.crawl_delay_seconds;
	state->rules = src.rules;
	state->robots_fetched = src.robots_fetched;
	state->has_crawl_delay = src.has_crawl_delay;
	state->min_crawl_delay_seconds = src.min_crawl_delay_seconds;
}

} // namespace duckdb
