/* Shard 3: a weak default, and a caller in the same shard. Shard 4 overrides it. */
#include "fx.h"

__attribute__((weak, noinline)) int fx_policy(void) { return 1; }

int fx_get_policy_same_shard(void) { return fx_policy(); }
