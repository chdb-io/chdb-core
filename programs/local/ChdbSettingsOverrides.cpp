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
