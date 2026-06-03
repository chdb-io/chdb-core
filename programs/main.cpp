#include <base/phdr_cache.h>
#include <base/scope_guard.h>
#include <base/defines.h>

#include <Common/EnvironmentChecks.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/Crypto/OpenSSLInitializer.h>


#include "config.h"
#include "config_tools.h"

#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <new>
#include <string>
#include <string_view>
#include <utility> /// pair
#include <vector>

#ifdef SANITIZER
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
extern "C" {
#ifdef ADDRESS_SANITIZER
const char * __asan_default_options()
{
    return "halt_on_error=1 abort_on_error=1";
}
const char * __lsan_default_options()
{
    return "max_allocation_size_mb=32768";
}
const char * __lsan_default_suppressions()
{
    /// OpenSSL intentionally does not free all global state at exit.
    /// These are known false positives from OpenSSL provider and EVP initialization.
    return "leak:ossl_provider_new\n"
           "leak:OSSL_PROVIDER_try_load_ex\n"
           "leak:ossl_rand_ctx_new\n"
           "leak:OSSL_LIB_CTX_new\n"
           "leak:ossl_legacy_provider_init\n"
           /// OpenSSL EVP method objects are cached globally and never freed at exit.
           /// Triggered when S3 client initializes HMAC (AWS SDK -> OpenSSL HMAC_Init_ex).
           "leak:evp_md_new\n"
           "leak:construct_evp_method\n"
           "leak:CRYPTO_THREAD_lock_new\n";
}
#endif

#ifdef MEMORY_SANITIZER
const char * __msan_default_options()
{
    return "abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768";
}
#endif

#ifdef THREAD_SANITIZER
const char * __tsan_default_options()
{
    return "halt_on_error=1 abort_on_error=1 history_size=7 second_deadlock_stack=1 max_allocation_size_mb=32768";
}
#endif

#ifdef UNDEFINED_BEHAVIOR_SANITIZER
const char * __ubsan_default_options()
{
    return "print_stacktrace=1 max_allocation_size_mb=32768";
}
#endif
}
#pragma clang diagnostic pop
#endif

// chdb_spec
#if defined(USE_MUSL) && defined(__aarch64__)
void main_musl_compile_stub(int arg)
{
    jmp_buf buf1;
    sigjmp_buf buf2;

    setjmp(buf1);
    sigsetjmp(buf2, arg);
}
#endif
// chdb_spec

/// Universal executable for various clickhouse applications
int mainEntryClickHouseLocal(int argc, char ** argv);

/// Private-only programs
#if CLICKHOUSE_CLOUD
int mainEntryClickHouseSharedCatalogUtil(int argc, char ** argv);
#if ENABLE_DISTRIBUTED_CACHE
int mainEntryClickHouseDistributedCache(int argc, char ** argv);
#endif
int mainEntryClickHouseSharedMergeTreeGarbageCleaner(int argc, char ** argv);
int mainEntryClickHouseClearZooKeeperLocks(int argc, char ** argv);
int mainEntryClickHousePackedIO(int argc, char ** argv);
int mainEntryClickHouseMangler(int argc, char ** argv);
#endif

namespace
{

using MainFunc = int (*)(int, char**);

/// Forward declaration, since clickhouse_applications is defined after this function.
void printHelp(std::ostream & out);

int mainEntryHelp(int, char **)
{
    printHelp(std::cout);
    return 0;
}

int printHelpOnError(int, char **)
{
    printHelp(std::cerr);
    return -1;
}

/// Add an item here to register new application.
/// This list has a "priority" - e.g. we need to disambiguate clickhouse --format being
/// either clickouse-format or clickhouse-{local, client} --format.
/// Currently we will prefer the latter option.
std::pair<std::string_view, MainFunc> clickhouse_applications[] =
{
    // chdb_spec
    {"local", mainEntryClickHouseLocal}
    // chdb_spec
};

void printHelp(std::ostream & out)
{
    // chdb_spec
    (void)out;
    // chdb_spec
}

/// Add an item here to register a new short name
std::pair<std::string_view, std::string_view> clickhouse_short_names[] =
{
    {"chl", "local"},
    {"chc", "client"},
#if USE_CHDIG
    {"chdig", "chdig"},
#endif
};

}

bool isClickhouseApp(std::string_view app_suffix, std::vector<char *> & argv)
{
    for (const auto & [alias, name] : clickhouse_short_names)
        if (app_suffix == name
            && !argv.empty() && (alias == argv[0] || endsWith(argv[0], "/" + std::string(alias))))
            return true;

    /// Use app if the first arg 'app' is passed (the arg should be quietly removed)
    if (argv.size() >= 2)
    {
        auto first_arg = argv.begin() + 1;

        /// 'clickhouse --client ...' and 'clickhouse client ...' are Ok
        if (*first_arg == app_suffix
            || (std::string_view(*first_arg).starts_with("--") && std::string_view(*first_arg).substr(2) == app_suffix))
        {
            argv.erase(first_arg);
            return true;
        }
    }

    /// Use app if clickhouse binary is run through symbolic link with name clickhouse-app
    std::string app_name = "clickhouse-" + std::string(app_suffix);
    return !argv.empty() && (app_name == argv[0] || endsWith(argv[0], "/" + app_name));
}

// chdb_spec
// /// Don't allow dlopen in the main ClickHouse binary, because it is harmful and insecure.
// /// We don't use it. But it can be used by some libraries for implementation of "plugins".
// /// We absolutely discourage the ancient technique of loading
// /// 3rd-party uncontrolled dangerous libraries into the process address space,
// /// because it is insane.

// #if !defined(USE_MUSL)
// extern "C"
// {
//     void * dlopen(const char *, int)
//     {
//         return nullptr;
//     }

//     void * dlmopen(long, const char *, int) // NOLINT
//     {
//         return nullptr;
//     }

//     int dlclose(void *)
//     {
//         return 0;
//     }

//     const char * dlerror()
//     {
//         return "ClickHouse does not allow dynamic library loading";
//     }
// }
// #endif
// chdb_spec

/// Prevent messages from JeMalloc in the release build.
/// Some of these messages are non-actionable for the users, such as:
/// <jemalloc>: Number of CPUs detected is not deterministic. Per-CPU arena disabled.
#if USE_JEMALLOC && defined(NDEBUG) && !defined(SANITIZER)
extern "C" void (*je_malloc_message)(void *, const char * s);
__attribute__((constructor(0))) void init_je_malloc_message() { je_malloc_message = [](void *, const char *){}; }
#elif USE_JEMALLOC
#include <unordered_set>
/// Ignore messages which can be safely ignored, e.g. EAGAIN on pthread_create
extern "C" void (*je_malloc_message)(void *, const char * s);
__attribute__((constructor(0))) void init_je_malloc_message()
{
    je_malloc_message = [](void *, const char * str)
    {
        using namespace std::literals;
        static const std::unordered_set<std::string_view> ignore_messages{
            "<jemalloc>: background thread creation failed (11)\n"sv};

        std::string_view message_view{str};
        if (ignore_messages.contains(message_view))
            return;

#    if defined(SYS_write)
        syscall(SYS_write, 2 /*stderr*/, message_view.data(), message_view.size());
#    else
        write(STDERR_FILENO, message_view.data(), message_view.size());
#    endif
    };
}
#endif

/// OpenSSL early initialization.
/// See also EnvironmentChecks.cpp for other static initializers.
/// Must be ran after EnvironmentChecks.cpp, as OpenSSL uses SSE4.1 and POPCNT.
__attribute__((constructor(202))) static void init_ssl()
{
    DB::OpenSSLInitializer::initialize();
}

/// This allows to implement assert to forbid initialization of a class in static constructors.
/// Usage:
///
/// extern bool inside_main;
/// class C { C() { assert(inside_main); } };
// bool inside_main = false;

int main(int argc_, char ** argv_)
{
    // inside_main = true;
    // SCOPE_EXIT({ inside_main = false; });

    /// PHDR cache is required for query profiler to work reliably
    /// It also speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    ///  will work only after additional call of this function.
    /// Note: we forbid dlopen in our code.
    updatePHDRCache();

#if !defined(USE_MUSL)
    checkHarmfulEnvironmentVariables(argv_);
#endif

    /// This is used for testing. For example,
    /// clickhouse-local should be able to run a simple query without throw/catch.
    if (getenv("CLICKHOUSE_TERMINATE_ON_ANY_EXCEPTION")) // NOLINT(concurrency-mt-unsafe)
        DB::terminate_on_any_exception = true;

    /// Reset new handler to default (that throws std::bad_alloc)
    /// It is needed because LLVM library clobbers it.
    std::set_new_handler(nullptr);

    std::vector<char *> argv(argv_, argv_ + argc_);

    /// Print a basic help if nothing was matched
    MainFunc main_func = printHelpOnError;

    for (auto & application : clickhouse_applications)
    {
        if (isClickhouseApp(application.first, argv))
        {
            main_func = application.second;
            break;
        }
    }

    /// Top-level --help / -h / -? (as the sole argument) should show the dispatcher
    /// help listing all subcommands and exit with code 0. Without this carve-out,
    /// `--help` would match the `startsWith(argv[i], "-h")` rule below and be routed
    /// into clickhouse-client, which treats anything starting with "-h" as a --host
    /// specification and fails.
    if (main_func == printHelpOnError && argv.size() == 2)
    {
        std::string_view arg(argv[1]);
        if (arg == "--help" || arg == "-h" || arg == "-?")
            main_func = mainEntryHelp;
    }

    /// Interpret binary without argument or with arguments starts with dash
    /// ('-') as clickhouse-local for better usability:
    ///
    ///     clickhouse help # dumps help
    ///     clickhouse -q 'select 1' # use local
    ///     clickhouse # spawn local
    ///     clickhouse local # spawn local
    ///     clickhouse "select ..." # spawn local
    ///     clickhouse /tmp/repro --enable-analyzer
    ///
    std::error_code ec;
    if (main_func == printHelpOnError && !argv.empty()
        && (argv.size() < 2 || argv[1] != std::string_view("--help"))
        && (argv.size() == 1 || argv[1][0] == '-' || std::string_view(argv[1]).contains(' ')
            || std::filesystem::is_regular_file(std::filesystem::path{argv[1]}, ec)))
    {
        main_func = mainEntryClickHouseLocal;
    }

    /// If the argument looks like a file path but doesn't exist, provide a helpful error
    /// instead of the generic "Use one of the following commands" message.
    /// The check above routes existing files to clickhouse-local, but when the file
    /// doesn't exist, we fall through to `printHelp` which is confusing:
    ///     $ clickhouse tests/queries/0_stateless/my_test.sql
    ///     Use one of the following commands: ...
    /// We detect file-like arguments by the presence of `/` (path separator)
    /// or `.` (file extension), which distinguishes them from mistyped subcommand
    /// names like "clickhouse sever" where the generic help is appropriate.
    if (main_func == printHelpOnError && argv.size() >= 2)
    {
        std::string_view arg(argv[1]);
        if (arg.contains('/') || arg.contains('.'))
        {
            std::cerr << "Error: no such file: " << arg << std::endl;
            std::cerr << "If you intended to run a script, please check the path." << std::endl;
            return 1;
        }
    }

    int exit_code = main_func(static_cast<int>(argv.size()), argv.data());

    return exit_code;
}
