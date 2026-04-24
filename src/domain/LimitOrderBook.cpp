#include "domain/LimitOrderBook.hpp"

#include "transport/FlatSyncedQueue.hpp"
#include "transport/HierarchicalSyncedQueue.hpp"

#include <unordered_set>
#include <utility>

namespace domain {

LobConsistentView::LobConsistentView(
    std::vector<LobsServingThread::Sptr> threads,
    std::unique_lock<std::mutex> freeze_lock)
    : paused_threads_(std::move(threads)),
      freeze_lock_(std::move(freeze_lock)) {}

LobConsistentView::~LobConsistentView() { resumeAll(); }

LobConsistentView::LobConsistentView(LobConsistentView &&other) noexcept
    : paused_threads_(std::move(other.paused_threads_)),
      freeze_lock_(std::move(other.freeze_lock_)) {
  other.paused_threads_.clear();
}

LobConsistentView &
LobConsistentView::operator=(LobConsistentView &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  resumeAll();
  paused_threads_ = std::move(other.paused_threads_);
  freeze_lock_ = std::move(other.freeze_lock_);
  other.paused_threads_.clear();
  return *this;
}

void LobConsistentView::resumeAll() noexcept {
  for (const auto &thread : paused_threads_) {
    thread->resume();
  }
  paused_threads_.clear();
}

template <typename QueueT>
LimitOrderBook<QueueT>::LimitOrderBook(const std::size_t max_work_threads_nums)
    : max_work_threads_nums_(max_work_threads_nums) {}

template <typename QueueT>
void LimitOrderBook<QueueT>::onEvent(const MarketDataEvent &event) {
  const auto resolved_instrument_id = resolveInstrumentId(event);
  if (!resolved_instrument_id.has_value()) {
    return;
  }

  const auto instrument_id = *resolved_instrument_id;
  if (event.action == 'R') {
    clearInstrumentOrderRoutes(instrument_id);
  } else {
    rememberOrderRoute(event, instrument_id);
  }

  const auto &route = registerInstrument(instrument_id);

  auto routed_event = event;
  routed_event.hd.instrument_id = instrument_id;
  route.thread->putEvent(routed_event, route.lob.get());
}

template <typename QueueT> void LimitOrderBook<QueueT>::onEndEvent() {
  std::unordered_set<const LobsServingThread *> stopped;
  for (const auto &[_, route] : instrument_routes_) {
    if (stopped.insert(route.thread.get()).second) {
      route.thread->stop();
    }
  }
}

template <typename QueueT> LobConsistentView LimitOrderBook<QueueT>::freeze() {

  std::unique_lock<std::mutex> freeze_lock(freeze_m_);

  std::vector<LobsServingThread::Sptr> unique_threads;
  std::unordered_set<const LobsServingThread *> seen;
  unique_threads.reserve(instrument_routes_.size());
  for (const auto &[_, route] : instrument_routes_) {
    if (seen.insert(route.thread.get()).second) {
      unique_threads.push_back(route.thread);
    }
  }

  for (const auto &thread : unique_threads) {
    thread->pause();
  }
  return LobConsistentView{std::move(unique_threads), std::move(freeze_lock)};
}

template <typename QueueT>
const typename LimitOrderBook<QueueT>::InstrumentRoute &
LimitOrderBook<QueueT>::registerInstrument(const InstrumentId instrument_id) {
  if (const auto it = instrument_routes_.find(instrument_id);
      it != instrument_routes_.end()) {
    return it->second;
  }

  auto lob = std::make_shared<Lob>();
  auto thread = acquireLeastLoadedThread();
  thread->attachLob(instrument_id, lob);
  work_threads_.push(thread);

  instrument_to_lob_.emplace(instrument_id, lob);
  const auto insert_result =
      instrument_routes_.emplace(instrument_id, InstrumentRoute{lob, thread});
  return insert_result.first->second;
}

template <typename QueueT>
typename LimitOrderBook<QueueT>::ThreadSptr
LimitOrderBook<QueueT>::acquireLeastLoadedThread() {
  if (work_threads_.size() < max_work_threads_nums_) {
    auto thread = std::make_shared<LobsServingThread>();
    thread->start();
    return thread;
  }
  auto thread = work_threads_.top();
  work_threads_.pop();
  return thread;
}

template <typename QueueT>
LobSptr
LimitOrderBook<QueueT>::findLob(const InstrumentId instrument_id) const {
  const auto it = instrument_to_lob_.find(instrument_id);
  return it == instrument_to_lob_.end() ? LobSptr{} : it->second;
}

template <typename QueueT>
BidsBookMap
LimitOrderBook<QueueT>::getBids(const InstrumentId instrument_id) const {
  const auto lob = findLob(instrument_id);
  return lob ? lob->getBids() : BidsBookMap{};
}

template <typename QueueT>
AsksBookMap
LimitOrderBook<QueueT>::getAsks(const InstrumentId instrument_id) const {
  const auto lob = findLob(instrument_id);
  return lob ? lob->getAsks() : AsksBookMap{};
}

template <typename QueueT>
BidsBookMap LimitOrderBook<QueueT>::getTopBids(const InstrumentId instrument_id,
                                               const std::size_t depth) const {
  const auto lob = findLob(instrument_id);
  return lob ? lob->getTopBids(depth) : BidsBookMap{};
}

template <typename QueueT>
AsksBookMap LimitOrderBook<QueueT>::getTopAsks(const InstrumentId instrument_id,
                                               const std::size_t depth) const {
  const auto lob = findLob(instrument_id);
  return lob ? lob->getTopAsks(depth) : AsksBookMap{};
}

template <typename QueueT>
BestQuote
LimitOrderBook<QueueT>::getBestBid(const InstrumentId instrument_id) const {
  const auto lob = findLob(instrument_id);
  return lob ? lob->getBestBid() : BestQuote{};
}

template <typename QueueT>
BestQuote
LimitOrderBook<QueueT>::getBestAsk(const InstrumentId instrument_id) const {
  const auto lob = findLob(instrument_id);
  return lob ? lob->getBestAsk() : BestQuote{};
}

template <typename QueueT>
Quantity
LimitOrderBook<QueueT>::getVolumeAtPrice(const InstrumentId instrument_id,
                                         const char side,
                                         const Price price) const {
  const auto lob = findLob(instrument_id);
  return lob ? lob->getVolumeAtPrice(side, price) : Quantity{0};
}

template <typename QueueT>
InstrumentIdToBidsBook LimitOrderBook<QueueT>::getAllBids() const {
  InstrumentIdToBidsBook result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getBids());
  }
  return result;
}

template <typename QueueT>
InstrumentIdToAsksBook LimitOrderBook<QueueT>::getAllAsks() const {
  InstrumentIdToAsksBook result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getAsks());
  }
  return result;
}

template <typename QueueT>
InstrumentIdToBidsBook
LimitOrderBook<QueueT>::getAllTopBids(const std::size_t depth) const {
  InstrumentIdToBidsBook result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getTopBids(depth));
  }
  return result;
}

template <typename QueueT>
InstrumentIdToAsksBook
LimitOrderBook<QueueT>::getAllTopAsks(const std::size_t depth) const {
  InstrumentIdToAsksBook result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getTopAsks(depth));
  }
  return result;
}

template <typename QueueT>
InstrumentIdToBestQuote LimitOrderBook<QueueT>::getAllBestBids() const {
  InstrumentIdToBestQuote result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getBestBid());
  }
  return result;
}

template <typename QueueT>
InstrumentIdToBestQuote LimitOrderBook<QueueT>::getAllBestAsks() const {
  InstrumentIdToBestQuote result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getBestAsk());
  }
  return result;
}

template <typename QueueT>
InstrumentIdToExecStats LimitOrderBook<QueueT>::getAllTradeStats() const {
  InstrumentIdToExecStats result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getTradeStats());
  }
  return result;
}

template <typename QueueT>
InstrumentIdToExecStats LimitOrderBook<QueueT>::getAllFillStats() const {
  InstrumentIdToExecStats result;
  result.reserve(instrument_to_lob_.size());
  for (const auto &[instrument_id, lob] : instrument_to_lob_) {
    result.emplace(instrument_id, lob->getFillStats());
  }
  return result;
}

template <typename QueueT>
std::size_t LimitOrderBook<QueueT>::getActiveWorkersCount() const {
  return getWorkerInstrumentLoads().size();
}

template <typename QueueT>
std::vector<std::size_t>
LimitOrderBook<QueueT>::getWorkerInstrumentLoads() const {
  std::unordered_map<const LobsServingThread *, std::size_t> loads_by_worker;
  loads_by_worker.reserve(instrument_routes_.size());
  for (const auto &[_, route] : instrument_routes_) {
    ++loads_by_worker[route.thread.get()];
  }

  std::vector<std::size_t> loads;
  loads.reserve(loads_by_worker.size());
  for (const auto &[_, load] : loads_by_worker) {
    loads.push_back(load);
  }
  return loads;
}

template <typename QueueT>
std::optional<InstrumentId> LimitOrderBook<QueueT>::resolveInstrumentId(
    const MarketDataEvent &event) const {
  if (event.hd.instrument_id != 0) {
    return event.hd.instrument_id;
  }
  if (event.order_id == 0) {
    return std::nullopt;
  }
  const auto it = order_to_instrument_.find(event.order_id);
  if (it == order_to_instrument_.end()) {
    return std::nullopt;
  }
  return it->second;
}

template <typename QueueT>
void LimitOrderBook<QueueT>::rememberOrderRoute(
    const MarketDataEvent &event, const InstrumentId instrument_id) {
  if (event.order_id == 0 || (event.action != 'A' && event.action != 'M')) {
    return;
  }

  order_to_instrument_[event.order_id] = instrument_id;
}

template <typename QueueT>
void LimitOrderBook<QueueT>::clearInstrumentOrderRoutes(
    const InstrumentId instrument_id) {
  auto it = order_to_instrument_.begin();
  while (it != order_to_instrument_.end()) {
    if (it->second == instrument_id) {
      it = order_to_instrument_.erase(it);
    } else {
      ++it;
    }
  }
}

template class LimitOrderBook<transport::FlatSyncedQueue>;
template class LimitOrderBook<transport::HierarchicalSyncedQueue>;
} // namespace domain
