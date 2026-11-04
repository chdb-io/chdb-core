/* Host-runtime consumer probe, unwinder half (chdb-io/chdb-rust#53).
 *
 * Walks its own stack with the host unwinder the way Rust's std::backtrace does:
 * _Unwind_Backtrace hands every frame's context to _Unwind_GetIP and friends. All of them
 * must come from the host's libgcc_s. libchdb.a bundles LLVM libunwind, and when the static
 * link bound _Unwind_GetIP to that copy while _Unwind_Backtrace still came from libgcc_s,
 * LLVM's accessor read libgcc's _Unwind_Context as its own cursor and the walk crashed.
 *
 * Linked the way a consumer links: no -rdynamic, no --allow-multiple-definition.
 * check_static_lib_hermetic.sh also checks the binary's symbol table, so a split binding
 * fails the gate even on a platform where it happens not to crash.
 */

#include <stdint.h>
#include <stdio.h>
#include <unwind.h>

#include "chdb.h"

struct walk_state
{
    uintptr_t caller; /* return address into main, which the walk must report */
    int frames;
    int found_caller;
};

static _Unwind_Reason_Code on_frame(struct _Unwind_Context * context, void * arg)
{
    struct walk_state * state = arg;
    int ip_before_insn = 0;
    uintptr_t ip = _Unwind_GetIP(context);
    /* The other two accessors Rust's backtrace and personality code call. */
    uintptr_t ip_info = _Unwind_GetIPInfo(context, &ip_before_insn);
    (void)_Unwind_GetCFA(context);

    if (ip != ip_info)
    {
        fprintf(stderr, "host_unwind_probe: _Unwind_GetIP and _Unwind_GetIPInfo disagree: %p vs %p\n",
                (void *)ip, (void *)ip_info);
        return _URC_FATAL_PHASE1_ERROR;
    }
    if (ip == state->caller)
        state->found_caller = 1;
    ++state->frames;
    return _URC_NO_REASON;
}

static __attribute__((noinline)) int walk_stack(void)
{
    struct walk_state state = {(uintptr_t)__builtin_return_address(0), 0, 0};
    _Unwind_Reason_Code reason = _Unwind_Backtrace(on_frame, &state);
    if (reason != _URC_END_OF_STACK || !state.found_caller)
    {
        fprintf(stderr, "host_unwind_probe: walk ended with reason %d after %d frames, caller %s\n",
                (int)reason, state.frames, state.found_caller ? "found" : "not found");
        return -1;
    }
    return state.frames;
}

int main(void)
{
    /* A crash must show up as a crash, not as chDB's fatal-signal handler stalling the
       watchdog. */
    chdb_set_signal_handlers_enabled(0);

    char * argv[] = {"host_unwind_probe"};
    chdb_connection * conn = chdb_connect(1, argv);
    if (!conn)
    {
        fprintf(stderr, "host_unwind_probe: chdb_connect returned NULL\n");
        return 1;
    }

    /* A failing query runs chDB's own C++ exception handling first, so the walk below
       happens after the bundled runtime has been exercised. */
    chdb_result * result = chdb_query(*conn, "SELECT * FROM host_unwind_probe_missing_table", "CSV");
    int query_failed = result && chdb_result_error(result) != NULL;
    chdb_destroy_query_result(result);
    if (!query_failed)
    {
        fprintf(stderr, "host_unwind_probe: the failing query did not report an error\n");
        chdb_close_conn(conn);
        return 1;
    }

    int frames = walk_stack();
    chdb_close_conn(conn);
    if (frames < 0)
        return 1;

    printf("host_unwind_probe: host unwinder walked %d frames after a chDB exception\n", frames);
    return 0;
}
