/* Shard 0: definitions that other shards use (helper, a thread_local, a string pointer, the
 * state a constructor sets) and one that only this shard uses (a_only). */
#include "fx.h"

/* Used by shard 1 only: has to stay global between the shards, under a private name. The
   host defines a helper() of its own that returns 2002. */
__attribute__((noinline)) int helper(void) { return 1001; }

/* Used by this shard only: becomes local. */
__attribute__((noinline)) int a_only(void) { return 7; }

int fx_a_entry(void) { return a_only(); }

/* Read and written by shard 1. */
__thread int fx_tls_counter = 5;

/* A pointer into the middle of a string: a data relocation with a non-zero addend into a
   mergeable string section. */
const char * const fx_beta_pointer = &"alpha-beta-gamma"[6];

/* Set by shard 2's constructor. */
int fx_state_value;
