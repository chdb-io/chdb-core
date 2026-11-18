/* The public API of the split_static_lib.py fixture (test_split_static_lib.sh): what fx.contract
 * lists. Everything else in the fixture is internal to it, as chDB's internals are. */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

int fx_call_helper(void);      /* helper() of another shard: the library's own, 1001 */
int fx_a_entry(void);          /* a_only() of the same shard: 7 */
int fx_tls_bump(void);         /* ++ a thread_local of another shard that starts at 5 */
const char * fx_beta(void);    /* a pointer into a string of another shard: "beta-gamma" */
const char * fx_code_string(void); /* a pointer into a string with an addend: "hello" */
int fx_state(void);            /* set to 42 by a constructor in another shard */
int fx_get_policy(void);       /* the strong fx_policy() over the weak default: 2 */
int fx_get_policy_same_shard(void); /* the same, called from the weak default's own shard */
int fx_fire(int value);        /* calls the optional hook fx_hook if one is linked: 1, else 0 */
int fx_has_libc_weak(void);    /* whether the weak reference into the C library resolved */
int fx_bump_1(void);           /* ++ an inline function's static counter, from one unit */
int fx_bump_2(void);           /* the same counter, from another unit */
int fx_catch(void);            /* a C++ exception thrown through frames of another shard */

#ifdef __cplusplus
}
#endif
