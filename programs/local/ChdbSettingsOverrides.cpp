#include "ChdbSettingsOverrides.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{
namespace Setting
{
extern const SettingsBool materialize_statistics_on_insert;
extern const SettingsMaxThreads max_insert_threads;
extern const SettingsMaxThreads max_threads;
extern const SettingsBool output_format_arrow_use_native_writer;
}
}

namespace CHDB
{

using namespace DB;

namespace
{

/// Adjust a *default* without claiming the user asked for it. `changed` is what
/// ClickHouse serializes to remote servers (BaseSettings iteration is
/// allChanged(), see Connection::sendQuery) and what system.settings reports as
/// user-set, so leaving it raised would push chdb's local judgement onto a real
/// ClickHouse server in remote()/Distributed queries and make a chdb default
/// look like an explicit choice.
template <typename SettingType, typename ValueType>
void setAdjustedDefault(Settings & settings, const SettingType & setting, const ValueType & value)
{
    settings[setting] = value;
    settings[setting].changed = false;
}

#if defined(OS_LINUX)
/// Total logical CPUs across NUMA nodes, or 0 when the topology is unknown
/// or only one node has CPUs. Enumerates the node directories (ids can be
/// sparse) and skips memory-only nodes (CXL/HBM expanders have an empty
/// cpulist); parses cpulist files ("0-47,96-143").
size_t getLogicalCPUsIfMultiNUMA()
{
    size_t total_cpus = 0;
    size_t cpu_nodes = 0;
    std::error_code ec;
    for (const auto & entry : std::filesystem::directory_iterator("/sys/devices/system/node", ec))
    {
        const std::string name = entry.path().filename().string();
        if (name.size() <= 4 || name.compare(0, 4, "node") != 0
            || name.find_first_not_of("0123456789", 4) != std::string::npos)
            continue;
        std::ifstream f(entry.path() / "cpulist");
        std::string line;
        if (!f.is_open() || !std::getline(f, line) || line.empty())
            continue;
        size_t cpus = 0;
        std::vector<std::string> ranges;
        boost::split(ranges, line, boost::is_any_of(","));
        for (auto & range : ranges)
        {
            boost::trim(range);
            size_t lo = 0;
            size_t hi = 0;
            if (sscanf(range.c_str(), "%zu-%zu", &lo, &hi) == 2 && hi >= lo) // NOLINT(cert-err34-c)
                cpus += hi - lo + 1;
            else if (sscanf(range.c_str(), "%zu", &lo) == 1) // NOLINT(cert-err34-c)
                cpus += 1;
        }
        if (cpus == 0)
            continue;
        ++cpu_nodes;
        total_cpus += cpus;
    }
    return cpu_nodes > 1 ? total_cpus : 0;
}
#endif

}

void applySettingsOverridesForChdb(ContextMutablePtr context)
{
    Settings settings = context->getSettingsCopy();

    /// v26.7's native Arrow IPC writer emits corrupt date32 buffers in the macOS
    /// x86_64 cross-build (pyarrow reads zeros or garbage); default to the mature
    /// libarrow writer until that is root-caused. Users can still opt in.
    setAdjustedDefault(settings, Setting::output_format_arrow_use_native_writer, false);

    /// 26.9 turned materialize_statistics_on_insert on by default so that
    /// cost-based join reordering has estimates on freshly loaded tables. For an
    /// embedded engine that trade is inverted: every insert-produced part gains a
    /// statistics.packed, and loading a part reads that one packed file once per
    /// column - 106-211 opens per part on a 105-column table. A fresh process
    /// therefore pays the whole cost before its first query, and chdb callers that
    /// run one query per process pay it on every query. Measured on ClickBench
    /// hits (13 parts, 105 columns, merges stopped): first-query 43.3 ms -> 7.7 ms,
    /// FileOpen 1665 -> 626, which is 26.7.3's 623. On the full 100M-row table the
    /// per-query floor it removes is ~2.5 ms per statistics-carrying part. Merges
    /// still materialize statistics (materialize_statistics_on_merge), so this
    /// defers them rather than dropping them, leaving the optimizer exactly where
    /// 26.7.3 left it - join reordering is configured identically in both versions.
    /// The cost is that optimize_trivial_count_with_sparsity_filter gives part of
    /// its win back (ClickBench Q1 0.010 s -> 0.049 s, still 2.1x better than
    /// 26.7.3) because only merge-produced parts keep the counters. Revert once
    /// upstream reads the packed statistics file once per part instead of once per
    /// column. Users can opt back in with materialize_statistics_on_insert=1.
    setAdjustedDefault(settings, Setting::materialize_statistics_on_insert, false);

    /// 26.8 changed the default from 1 to one INSERT thread per core. On many-core machines the
    /// table a parallel bulk INSERT SELECT leaves behind is slower to query: ClickBench on c6a.metal
    /// measured hot x0.72 and cold x0.47 with 1 thread, for a 1.5x longer load. Machines with
    /// little memory already drop to 1 thread through max_insert_threads_min_free_memory_per_thread.
    /// Users can still raise it per query.
    setAdjustedDefault(settings, Setting::max_insert_threads, 1);

#if defined(OS_LINUX)
    /// On multi-socket machines a default of "one thread per core" backfires:
    /// per-thread aggregation states multiply the merge work and the merge runs
    /// across sockets (e.g. COUNT(DISTINCT) GROUP BY is ~5x slower with 192
    /// threads than with 96 on a 2-socket EPYC, and clickhouse-local shows the
    /// same). Upstream is shielded on SMT x86 where "physical cores" already
    /// halves the count, but on no-SMT many-core CPUs the default lands on every
    /// core of every socket. Cap the default at one NUMA node's core count;
    /// explicit max_threads settings are untouched.
    if (const size_t logical_cpus = getLogicalCPUsIfMultiNUMA();
        logical_cpus > 0 && !settings[Setting::max_threads].changed)
    {
        const size_t upstream_default = settings[Setting::max_threads];
        /// Half the *logical* CPUs, deliberately not "one node's CPUs": node
        /// granularity depends on BIOS sub-NUMA settings (AMD NPS2/NPS4,
        /// Intel SNC), while the measured optimum tracks logical/2 on both
        /// validated shapes — 2-socket no-SMT 192-core (192 -> 96) and
        /// 2-socket SMT 96-physical/192-logical (upstream default 96 is
        /// already logical/2; unchanged).
        const size_t cap = logical_cpus / 2;
        if (cap > 0 && cap < upstream_default)
            setAdjustedDefault(settings, Setting::max_threads, cap);
    }
#endif

    context->setSettings(settings);
}

}
