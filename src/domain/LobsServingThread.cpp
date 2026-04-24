#include "domain/LobsServingThread.hpp"

#include <utility>

namespace domain {
LobsServingThread::LobsServingThread() = default;

LobsServingThread::~LobsServingThread() { stop(); }

void LobsServingThread::start() {
  thread_ = std::thread([this]() {
    for (;;) {
      auto queued_events = popAvailable();

      for (auto &queued_event : queued_events) {
        if (queued_event.kind == QueuedEvent::Kind::Stop) {
          return;
        }

        if (queued_event.kind == QueuedEvent::Kind::Pause) {
          std::unique_lock<std::mutex> lock(pause_m_);
          worker_paused_ = true;
          pause_cv_.notify_all();
          pause_cv_.wait(lock, [this] { return !pause_requested_; });
          worker_paused_ = false;
          pause_cv_.notify_all();
          continue;
        }

        if (queued_event.lob != nullptr) {
          queued_event.lob->onEvent(queued_event.event);
        }
      }
    }
  });
}

void LobsServingThread::stop() {
  if (!thread_.joinable()) {
    return;
  }
  {
    const std::lock_guard<std::mutex> lock(pause_m_);
    pause_requested_ = false;
    pause_cv_.notify_all();
  }
  put({.kind = QueuedEvent::Kind::Stop});
  thread_.join();
}

void LobsServingThread::attachLob(const InstrumentId instrument_id,
                                  const LobSptr &lob) {
  (void)instrument_id;
  (void)lob;
  ++capacity_;
}

void LobsServingThread::putEvent(const MarketDataEvent &event, Lob *lob) {
  put({.kind = QueuedEvent::Kind::MarketData, .event = event, .lob = lob});
}

std::size_t LobsServingThread::capacity() const { return capacity_; }

void LobsServingThread::pause() {
  std::unique_lock<std::mutex> lock(pause_m_);
  pause_requested_ = true;

  put({.kind = QueuedEvent::Kind::Pause});
  pause_cv_.wait(lock, [this] { return worker_paused_; });
}

void LobsServingThread::resume() {
  std::unique_lock<std::mutex> lock(pause_m_);
  pause_requested_ = false;
  pause_cv_.notify_all();
  pause_cv_.wait(lock, [this] { return !worker_paused_; });
}

void LobsServingThread::put(QueuedEvent event) {
  {
    const std::lock_guard<std::mutex> lock(queue_m_);
    queue_.push_back(std::move(event));
  }
  queue_cv_.notify_one();
}

std::deque<LobsServingThread::QueuedEvent> LobsServingThread::popAvailable() {
  std::unique_lock<std::mutex> lock(queue_m_);
  queue_cv_.wait(lock, [this] { return !queue_.empty(); });
  std::deque<QueuedEvent> events;
  events.swap(queue_);
  return events;
}

} // namespace domain
