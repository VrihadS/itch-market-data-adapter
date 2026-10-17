#pragma once
#include "itch/common/common.h"
#ifdef HAVE_FOLLY
#include "folly/concurrency/UnboundedQueue.h"
#endif
#include <boost/asio.hpp>
#include <atomic>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <pthread.h>
#include <unistd.h>
#ifdef __linux__
#include <sched.h>
#endif

namespace feed_handler
{

// How the WorkerThread waits for asio work when no handler is currently ready.
//
//   Default      — preserve legacy coupling with `core`: pinned (`core >= 0`) →
//                  Spin; unpinned (`core == -1`) → Block. Use this when callers
//                  haven't opted into a specific policy.
//   Block        — io_context::run(); kernel sleep in epoll_wait when idle. Zero
//                  CPU when idle, but every cross-thread post() pays one kernel
//                  wakeup.
//   Yield        — io_context::poll() loop with std::this_thread::yield() when
//                  no handler ran. No kernel wakeup on the hot path, but yield()
//                  does not sleep: an otherwise-idle thread spins at ~100% CPU.
//                  Reserve it for a thread pinned to a dedicated core; prefer
//                  SpinThenPark for an unpinned worker that must free the core when idle.
//   Spin         — io_context::poll() in a tight busy-loop. 100% CPU on the
//                  thread's core; lowest latency, highest cost.
//   SpinThenPark — Spin io_context::poll() for up to `spin_window` consecutive
//                  empty polls, then fall back to io_context::run_one() which
//                  blocks in epoll_wait until the next post(). Best for bursty
//                  workloads: stays busy-poll-fast through a burst, then frees
//                  the core during quiet periods. `spin_window` is per-thread.
enum class WorkerPollMode { Default, Block, Yield, Spin, SpinThenPark };

class WorkerThread
{
  public:
    // `spin_window` is only consulted when poll_mode == SpinThenPark. It is the
    // number of consecutive empty io_context::poll() iterations the thread will
    // run before falling back to a blocking io_context::run_one(). Ignored in
    // other modes.
    WorkerThread(const std::string& name, int16_t core = -1,
                 WorkerPollMode poll_mode = WorkerPollMode::Default,
                 int spin_window = 100);
    ~WorkerThread();

    void Start();
    void Stop();

    void ThreadEntry();

    void Post(std::function<void()> fn);

    boost::asio::io_context& GetIOContext() {return io_context_;};
    std::string GetThreadName() const { return name_; }
    bool IsStarted() const { return thread_.joinable(); }
    // True when called from the worker thread itself (e.g. to avoid waiting on work it would have to run).
    bool IsWorkerThread() const { return worker_id_.load(std::memory_order_relaxed) == std::this_thread::get_id(); }
  private:
    WorkerThread() = delete;

    // Resolve Default to the legacy core-implied policy: pinned → Spin, unpinned → Block.
    // Existing callers that only pass `core` keep the exact behavior they had before
    // WorkerPollMode existed. Both ThreadEntry() (which loop to run) and Post() (which queue
    // to route to) must agree on this, hence one shared resolver.
    WorkerPollMode ResolvedPollMode() const
    {
        if (poll_mode_ != WorkerPollMode::Default)
            return poll_mode_;
        return (core_ >= 0) ? WorkerPollMode::Spin : WorkerPollMode::Block;
    }

    // Parse a Linux cpulist (e.g. "5-23" or "1,3,5-7") into a set of CPU indices.
    // Malformed tokens are skipped rather than throwing.
    static void ParseCpuList(const std::string &spec, std::set<int> &out);

    // Compute the affinity mask an unpinned worker should run on: all online CPUs
    // minus the isolcpus-isolated set. This prevents an unpinned io_context thread
    // from silently inheriting an isolated-core mask from the thread that created
    // it (e.g. a library that pins its caller), which would otherwise place general
    // work on a core reserved via isolcpus.
    //
    // Returns true and fills `out` when a mask should be applied. Returns false
    // when no CPU isolation is in effect (leave the inherited mask untouched) or
    // when the housekeeping set cannot be determined.
#ifdef __linux__
    static bool ComputeUnpinnedWorkerAffinity(cpu_set_t &out);
#endif

    std::thread             thread_;
    boost::asio::io_context io_context_;
    std::atomic<bool>       finished_{false};
    std::atomic<std::thread::id> worker_id_{};
    int16_t                 core_;
    std::string             name_;
    WorkerPollMode          poll_mode_;
    int                     spin_window_;
    // Lock-free task queue used ONLY by the busy-spin path (ResolvedPollMode() == Spin). The
    // order interface posts amends here, skipping boost::asio io_context::post (an internal
    // mutex plus a reactor eventfd interrupt that is pure waste when the loop is already
    // spinning). Every other mode blocks in the asio reactor and would never drain it.
#ifdef HAVE_FOLLY
    folly::UMPSCQueue<std::function<void()>, false> task_queue_;
#endif
};

inline WorkerThread::WorkerThread(const std::string& name, int16_t core,
                                  WorkerPollMode poll_mode, int spin_window)
    : core_(core), name_(name), poll_mode_(poll_mode), spin_window_(spin_window)
{
}

inline WorkerThread::~WorkerThread()
{
    Stop();
}

inline void WorkerThread::Start()
{
    finished_ = false;

    try
    {
        if (thread_.joinable())
        {
            thread_.join();
        }
        // A prior Stop() left io_context_ in the stopped state (Stop() calls io_context_.stop()).
        // Clear it before the new thread starts so queued asio work (session start, reconcile,
        // socket reactor) runs immediately instead of being no-op'd by poll()/run() until the
        // loop's next periodic reset(). No-op on a fresh (never-stopped) io_context.
        io_context_.restart();
        thread_ = std::thread(&WorkerThread::ThreadEntry, this);
    }
    catch (const std::exception &e)
    {
        std::cout << "exception in worker thread" << e.what() << std::endl;
        // Handle or log the exception
    }
}

inline void WorkerThread::Stop()
{
    bool expected = false;
    if (!finished_.compare_exchange_strong(expected, true))   // strong: a spurious failure here would skip the join
    {
        return;
    }

    io_context_.stop();
    if (thread_.joinable())
    {
        thread_.join();
    }
}

inline void WorkerThread::ParseCpuList(const std::string &spec, std::set<int> &out)
{
    std::stringstream ss(spec);
    std::string       token;
    while (std::getline(ss, token, ','))
    {
        if (token.empty())
            continue;

        try
        {
            const auto dash = token.find('-');
            if (dash == std::string::npos)
            {
                out.insert(std::stoi(token));
            }
            else
            {
                const int lo = std::stoi(token.substr(0, dash));
                const int hi = std::stoi(token.substr(dash + 1));
                for (int c = lo; c <= hi; ++c)
                    out.insert(c);
            }
        }
        catch (const std::exception &)
        {
            // ignore malformed token
        }
    }
}

#ifdef __linux__
inline bool WorkerThread::ComputeUnpinnedWorkerAffinity(cpu_set_t &out)
{
    const long nproc = sysconf(_SC_NPROCESSORS_CONF);
    if (nproc <= 0)
        return false;

    std::ifstream isolated_file("/sys/devices/system/cpu/isolated");
    if (!isolated_file)
        return false;

    std::string spec;
    std::getline(isolated_file, spec);

    std::set<int> isolated;
    ParseCpuList(spec, isolated);

    if (isolated.empty())
        return false; // no isolcpus — keep the inherited (default) behavior

    CPU_ZERO(&out);
    for (long c = 0; c < nproc && c < CPU_SETSIZE; ++c)
        if (isolated.count(static_cast<int>(c)) == 0)
            CPU_SET(static_cast<int>(c), &out);

    return CPU_COUNT(&out) > 0;
}
#endif // __linux__

inline void WorkerThread::ThreadEntry()
{
    worker_id_.store(std::this_thread::get_id(), std::memory_order_relaxed);
    auto worker = boost::asio::make_work_guard(io_context_);

    if(!name_.empty())
    {
#ifdef __linux__
        pthread_setname_np(pthread_self(), name_.c_str());
#else
        pthread_setname_np(name_.c_str()); // macOS: names the calling thread only
#endif
    }

    // Pin if a core was specified. Pinning is independent of the poll mode —
    // a caller can pin AND block, or run unpinned AND yield.
#ifdef __linux__
    if (core_ >= 0)
    {
        spdlog::info("running fast thread on {}", core_);
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0)
            spdlog::warn("WorkerThread {}: failed to pin to core {} rc={}", name_, core_, rc);
    }
    else
    {
        // Unpinned: don't silently inherit an isolated-core affinity mask from
        // the creating thread. Confine to the non-isolated (housekeeping) CPUs so
        // an unpinned io_context worker never lands on an isolcpus core.
        cpu_set_t cpuset;
        if (ComputeUnpinnedWorkerAffinity(cpuset))
        {
            int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0)
                spdlog::warn("WorkerThread {}: failed to set housekeeping affinity rc={}", name_, rc);
        }
    }
#else
    if (core_ >= 0)
        spdlog::warn("WorkerThread {}: core pinning is not supported on this platform; ignoring core {}", name_, core_);
#endif

    switch (ResolvedPollMode())
    {
    case WorkerPollMode::Block:
        while (!finished_)
        {
            io_context_.run();
            io_context_.restart();
        }
        break;
    case WorkerPollMode::Spin:
    {
        // The busy-spin path drains the hot task_queue_ — the order interface posts
        // amends there. io_context only carries infrequent async work (session start, etc.),
        // so polling its asio reactor on EVERY spin (a reactor poll / possible epoll syscall)
        // gated each amend dequeue. Drain task_queue_ each iteration and poll io_context only
        // periodically, so async work is still serviced (never starved) without sitting on the
        // per-amend critical path.
        //
        // The drain is capped at kMaxTaskDrainBatch per iteration: an unbounded inner loop would,
        // under a sustained amend flood, never recheck finished_ (Stop() would block forever in
        // join()). The cap bounds shutdown latency to one batch.
        //
        // Poll cadence is bounded two ways so asio work can't be deferred arbitrarily under load:
        //   - every 256 outer iterations (services io_context when the queue is idle/light), and
        //   - every kPollTaskBudget drained tasks (caps the busy-case deferral; without this the
        //     bound would be 256 * kMaxTaskDrainBatch tasks).
        constexpr int      kMaxTaskDrainBatch = 64;
        constexpr uint32_t kPollTaskBudget    = 512; // max tasks between io_context polls under load
        uint32_t           spins              = 0;
        uint32_t           tasks_since_poll   = 0;
        while (!finished_)
        {
            int drained = 0;
#ifdef HAVE_FOLLY
            std::function<void()> task;
            while (drained < kMaxTaskDrainBatch && task_queue_.try_dequeue(task))
            {
                task();
                ++drained;
            }
#else
            (void)kMaxTaskDrainBatch;
#endif
            tasks_since_poll += drained;
            if ((++spins & 0xFF) == 0 || tasks_since_poll >= kPollTaskBudget)
            {
                io_context_.poll();
                io_context_.restart();
                tasks_since_poll = 0;
            }
        }
        break;
    }
    case WorkerPollMode::Yield:
        while (!finished_)
        {
            if (io_context_.poll() == 0)
                std::this_thread::yield();
            io_context_.restart();
        }
        break;
    case WorkerPollMode::SpinThenPark:
        while (!finished_)
        {
            int idle = 0;
            while (idle < spin_window_ && !finished_)
            {
                if (io_context_.poll() > 0)
                    idle = 0;
                else
                    ++idle;
                io_context_.restart();
            }
            if (finished_) break;

            // Quiet period — park in epoll_wait until the next post() arrives,
            // then return to the spin loop above for at least one more window.
            io_context_.run_one();
            io_context_.restart();
        }
        break;
    case WorkerPollMode::Default:
        // Unreachable: resolved above.
        break;
    }
}

inline void WorkerThread::Post(std::function<void()> fn)
{
    // Busy-spin threads (e.g. the pinned order interface) drain task_queue_ directly, so
    // route there to avoid the asio post mutex + reactor wakeup. Every other mode parks in
    // the asio reactor (run/run_one) or only polls io_context, and would never drain
    // task_queue_ — those stay on io_context, which their loop services.
#ifdef HAVE_FOLLY
    if (ResolvedPollMode() == WorkerPollMode::Spin)
    {
        task_queue_.enqueue(std::move(fn));
        return;
    }
#endif
    boost::asio::post(io_context_, std::move(fn));
}

} // namespace feed_handler


