#pragma once

#include "domain/LimitOrderBookSingleInstrument.hpp"
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

namespace domain {
using Lob = LimitOrderBookSingleInstrument;
using LobSptr = Lob::Sptr;

class LobsServingThread final {
public:
  using Sptr = std::shared_ptr<LobsServingThread>;

  struct CompareByCapacity {
    bool operator()(const Sptr &a, const Sptr &b) const {
      return a->capacity() > b->capacity();
    }
  };

  LobsServingThread();
  ~LobsServingThread();

  LobsServingThread(const LobsServingThread &) = delete;
  LobsServingThread &operator=(const LobsServingThread &) = delete;
  LobsServingThread(LobsServingThread &&) = delete;
  LobsServingThread &operator=(LobsServingThread &&) = delete;

  void start();
  void stop();

  void attachLob(InstrumentId instrument_id, const LobSptr &lob);

  void putEvent(const MarketDataEvent &event, Lob *lob);
  std::size_t capacity() const;

  void pause();
  void resume();

private:
  struct QueuedEvent {
    enum class Kind { MarketData, Pause, Stop };

    Kind kind{Kind::MarketData};
    MarketDataEvent event{};
    Lob *lob{nullptr};
  };

  void put(QueuedEvent event);
  std::deque<QueuedEvent> popAvailable();

  mutable std::mutex queue_m_;
  std::condition_variable queue_cv_;
  std::deque<QueuedEvent> queue_;

  std::thread thread_;
  std::size_t capacity_{0};

  mutable std::mutex pause_m_;
  std::condition_variable pause_cv_;
  bool pause_requested_{false};
  bool worker_paused_{false};
};
} // namespace domain
