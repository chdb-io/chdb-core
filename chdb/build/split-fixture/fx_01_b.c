/* Shard 1: uses what shard 0 defines. */
#include "fx.h"

int helper(void);
extern __thread int fx_tls_counter;
extern const char * const fx_beta_pointer;
extern int fx_state_value;

int fx_call_helper(void) { return helper(); }
int fx_tls_bump(void) { return ++fx_tls_counter; }
const char * fx_beta(void) { return fx_beta_pointer; }
int fx_state(void) { return fx_state_value; }

/* A code relocation with a non-zero addend into a mergeable string section. */
__attribute__((noinline)) const char * fx_code_string(void) { return &"xxxx-hello"[5]; }
