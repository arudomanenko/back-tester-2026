#include "common/Pipeline.hpp"
#include "data_layer/JsonMarketDataFolderLoader.hpp"
#include "domain/LimitOrderBook.hpp"
#include "domain/Snapshotter.hpp"
#include "transport/Subscriber.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr std::uint64_t SNAPSHOT_INTERVAL_NS = 21'600'000'000'000ULL;
constexpr std::size_t SNAPSHOT_DEPTH = 5;
constexpr std::size_t SNAPSHOTS_TO_PRINT = 8;

bool hasBookLevels(const domain::LobSnapshot &snapshot) {
  for (const auto &instrument : snapshot.instruments) {
    if (!instrument.bids.empty() || !instrument.asks.empty()) {
      return true;
    }
  }
  return false;
}

void printSnapshots(const std::vector<domain::LobSnapshot> &snapshots) {
  std::size_t printed = 0;
  for (const auto &snapshot : snapshots) {
    if (!hasBookLevels(snapshot)) {
      continue;
    }
    std::cout << snapshot;
    if (++printed >= SNAPSHOTS_TO_PRINT) {
      break;
    }
  }
  if (printed == 0 && !snapshots.empty()) {
    std::cout << snapshots.back();
  }
}

template <typename QueueT>
void run(const std::string &bench_name, const std::vector<std::string> &files,
         std::size_t num_threads, bool print_snapshot_logs) {
  const auto queue = std::make_shared<QueueT>(files.size());

  domain::LimitOrderBook<QueueT> lob(num_threads);
  domain::Snapshotter<QueueT> snapshotter(lob, SNAPSHOT_INTERVAL_NS,
                                          SNAPSHOT_DEPTH);

  data_layer::MarketDataFolderLoaderT<QueueT> loader(files, queue);
  transport::QueueSubscriberT<QueueT> subscriber(queue);

  subscriber.addSubscriber({
      .name = "LOB",
      .onEvent = [&](const auto &e) { lob.onEvent(e); },
      .onEndEvents = [&] { lob.onEndEvent(); },
  });
  subscriber.addSubscriber({
      .name = "Snapshotter",
      .onEvent = [&](const auto &e) { snapshotter.onEvent(e); },
      .onEndEvents = [&] { snapshotter.onEndEvents(); },
  });

  const auto t0 = std::chrono::steady_clock::now();

  common::pipeline::runPipeline(loader, subscriber);
  common::pipeline::stopPipeline(subscriber, loader);

  const auto elapsed_s = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() - t0)
                             .count();
  const auto worker_instrument_loads = lob.getWorkerInstrumentLoads();

  std::cout << "bench_name=" << bench_name << " ";
  std::cout << "threads=" << num_threads
            << " events=" << snapshotter.eventsSeen()
            << " snapshots=" << snapshotter.snapshots().size()
            << " snapshot_depth=" << snapshotter.depth()
            << " elapsed_s=" << elapsed_s << '\n';
  std::cout << "worker_threads_active=" << lob.getActiveWorkersCount()
            << " worker_instruments=[";
  for (std::size_t i = 0; i < worker_instrument_loads.size(); ++i) {
    if (i != 0) {
      std::cout << ",";
    }
    std::cout << worker_instrument_loads[i];
  }
  std::cout << "]\n";
  if (print_snapshot_logs) {
    printSnapshots(snapshotter.snapshots());
  }
}

} // namespace

int main(int argc, char *argv[]) {
  namespace fs = std::filesystem;

  constexpr std::string_view suppress_logs_arg = "--suppress-logs";

  if (argc < 2 || argc > 3) {
    std::cerr << "Usage: " << argv[0]
              << " <folder_with_mbo_json> [--suppress-logs]\n"
              << "  --suppress-logs: skip snapshot printing\n";
    return 1;
  }

  std::string folder;
  bool suppress_logs = false;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg{argv[i]};
    if (arg == suppress_logs_arg) {
      suppress_logs = true;
    } else if (folder.empty()) {
      folder.assign(arg.data(), arg.size());
    } else {
      std::cerr << "Unexpected argument: " << arg << '\n';
      return 1;
    }
  }

  if (folder.empty()) {
    std::cerr << "Missing folder argument\n";
    return 1;
  }

  if (!fs::is_directory(folder)) {
    std::cerr << "Expected a directory with .mbo.json files: " << folder
              << '\n';
    return 1;
  }

  std::vector<std::string> json_files;
  try {
    json_files = data_layer::discoverJsonFiles(folder);
  } catch (const std::exception &ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }

  if (json_files.empty()) {
    std::cerr << "No .mbo.json files to process\n";
    return 1;
  }

  const bool print_snapshot_logs = !suppress_logs;

  run<transport::FlatSyncedQueue>("flat_1", json_files, 1, print_snapshot_logs);
  std::cout << "----------------------------------------\n";
  run<transport::HierarchicalSyncedQueue>("hierarchical_1", json_files, 1,
                                          print_snapshot_logs);
  std::cout << "----------------------------------------\n";
  run<transport::FlatSyncedQueue>("flat_4", json_files, 4, print_snapshot_logs);
  std::cout << "----------------------------------------\n";
  run<transport::HierarchicalSyncedQueue>("hierarchical_4", json_files, 4,
                                          print_snapshot_logs);
  std::cout << "----------------------------------------\n";
  return 0;
}
